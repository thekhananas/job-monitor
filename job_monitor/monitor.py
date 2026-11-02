"""One scheduled check. History and alerts are independent of resume tooling."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import argparse
import copy
import fcntl
import hashlib
import json
import os
import tempfile
import time
import urllib.parse

from job_monitor.sources import fetch
from job_monitor.screening import screen_with_reasons, validate_profile, REJECTION_LABELS
from job_monitor.notifications import NotificationError, sender_for, telegram
from job_monitor.health import build_health, record_confirmation, render_health


MIN_POLL_INTERVAL = 15


def empty_state():
    return {"version": 1, "boards": {}, "jobs": {}, "pending": [], "notified": []}


def read_state(path):
    if not path.exists():
        return empty_state()
    state = json.loads(path.read_text())
    if not isinstance(state, dict) or state.get("version") != 1 or any(not isinstance(state.get(key), dict) for key in ("boards", "jobs")) or any(not isinstance(state.get(key), list) for key in ("pending", "notified")):
        raise ValueError("Invalid history; restore state.json rather than resetting the baseline")
    return state


def canonical_url(url):
    parts = urllib.parse.urlsplit(url)
    query = [(key, value) for key, value in urllib.parse.parse_qsl(parts.query)
             if not key.casefold().startswith("utm_") and key.casefold() not in {"source", "gh_src", "ref"}]
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc.casefold(), parts.path.rstrip("/"), urllib.parse.urlencode(sorted(query)), ""))


def apply_scan(previous, results, profile, now, *, baseline_policy="suppress", policy_effective_at=None):
    """Process only successful board responses; first success is a baseline."""
    state = copy.deepcopy(previous)
    profile_hash = profile_fingerprint(profile)
    known_urls = {canonical_url(value["job"]["url"]): value["first_seen"] for value in previous["jobs"].values()}
    event_ids = set(previous["notified"]) | {event["event_id"] for event in previous["pending"]}
    for source, jobs, error in results:
        policy = source.get("baseline_policy", baseline_policy)
        if policy not in ("suppress", "notify"):
            raise ValueError("baseline_policy must be suppress or notify")
        board = source["provider"] + ":" + source["board"]
        status = state["boards"].setdefault(board, {"initialized": False})
        if not status["initialized"]:
            status.setdefault("first_attempt_at", now)
        status["last_attempt"] = now
        if error:
            status["last_error"] = error
            continue
        baseline = not status["initialized"]
        previous_check_at = status.get("last_success", "")
        status.update(initialized=True, last_success=now, last_error="")
        if baseline:
            status.update(baseline_at=now, baseline_policy=policy, baseline_job_count=len(jobs),
                          baseline_matching_count=0, baseline_suppressed_count=0)
        seen = set()
        for job in jobs:
            key = board + ":" + job["id"]
            seen.add(key)
            original = state["jobs"].get(key)
            url = canonical_url(job["url"])
            first_seen = original["first_seen"] if original else known_urls.get(url, now)
            match, rejection_reasons = screen_with_reasons(job, profile,
                now=datetime.fromisoformat(now.replace("Z", "+00:00")))
            content_hash = hashlib.sha256(job["description"].encode()).hexdigest()
            safe_job = {name: value for name, value in job.items() if name != "description"}
            newly_found = original is None and url not in known_urls
            newly_matched = original is not None and not original["matched"]
            suppressed = baseline and policy == "suppress" and match and newly_found
            state["jobs"][key] = {"board": board, "company": source["company"], "job": safe_job,
                                  "first_seen": first_seen, "last_seen": now, "content_sha256": content_hash,
                                  "matched": bool(match), "screening": match, "active": True,
                                  "rejection_reasons": rejection_reasons, "screening_profile_sha256": profile_hash,
                                  "baseline_suppressed_at": original.get("baseline_suppressed_at", "") if original else now if suppressed else ""}
            if baseline and match:
                status["baseline_matching_count"] += 1
                status["baseline_suppressed_count"] += bool(suppressed)
            catchup = (policy_effective_at is not None and newly_matched and
                       datetime.fromisoformat(first_seen.replace("Z", "+00:00")).timestamp() < policy_effective_at)
            if (not baseline or policy == "notify") and match and (newly_found or newly_matched) and not catchup:
                kind = ("Present at first successful check" if baseline else
                        "Newly discovered" if newly_found else "New match on an existing posting")
                event_id = hashlib.sha256((url + kind + content_hash).encode()).hexdigest()
                if event_id not in event_ids:
                    state["pending"].append({"event_id": event_id, "key": key, "kind": kind,
                        "company": source["company"], "job": safe_job, "screening": match,
                        "first_seen": first_seen, "queued_at": now,
                        "previous_check_at": previous_check_at})
                    event_ids.add(event_id)
            known_urls.setdefault(url, first_seen)
        for key, value in state["jobs"].items():
            if value["board"] == board and key not in seen:
                value["active"] = False
    # A successful scan that removes a lead cancels its undelivered notification.
    # Failed scans retain history and pending events.
    state["pending"] = [event for event in state["pending"]
                        if state["jobs"][event["key"]]["active"] and state["jobs"][event["key"]]["matched"]]
    for event in state["pending"]:
        current = state["jobs"][event["key"]]
        event.update(job=current["job"], screening=current["screening"])
    return state


def profile_fingerprint(profile):
    return hashlib.sha256(json.dumps(profile, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def rejection_audit(state, config, profile, *, limit=20, title=""):
    """Inspect recorded decisions only; descriptions are not retained for re-screening."""
    boards = {source["provider"] + ":" + source["board"] for source in config["sources"]
              if source.get("enabled", True)}
    active = [value for value in state["jobs"].values() if value["active"] and value["board"] in boards]
    rejected = [value for value in active if not value["matched"]]
    selected = [value for value in rejected if title.casefold() in value["job"]["title"].casefold()]
    selected.sort(key=lambda value: (value["first_seen"], value["company"], value["job"]["title"]), reverse=True)
    fingerprint = profile_fingerprint(profile)
    legacy = sum("rejection_reasons" not in value for value in rejected)
    outdated = sum(value.get("screening_profile_sha256") != fingerprint for value in rejected
                   if "rejection_reasons" in value)
    lines = [f"Audit: {len(active)} last-observed active jobs | Matched: {len(active) - len(rejected)} | Rejected: {len(rejected)}",
             "Recorded decisions, not a fresh board check. Counts cover enabled boards only.",
             f"Missing historical reasons: {legacy} | Decisions using a different profile: {outdated}"]
    for code, label in REJECTION_LABELS.items():
        count = sum(code in value.get("rejection_reasons", []) for value in rejected)
        if count:
            lines.append(f"{count}: {label}")
    lines.append("Reason counts can overlap; one job can fail multiple rules.")
    lines.append(f"Showing {min(limit, len(selected))}/{len(selected)} rejected jobs matching the title search, newest first-seen first.")
    for value in selected[:limit]:
        job = value["job"]
        reasons = value.get("rejection_reasons")
        labels = [REJECTION_LABELS.get(code, code) for code in reasons] if reasons else ["Historical reasons unavailable; wait for a successful check"]
        notes = []
        if reasons and value.get("screening_profile_sha256") != fingerprint:
            notes.append("Recorded under a different profile")
        if state["boards"].get(value["board"], {}).get("last_error"):
            notes.append("Latest board check failed; current availability unverified")
        lines.extend(["", f"{value['company']} — {job['title']} | {job['location']}",
                      "Rejected: " + "; ".join(labels),
                      f"First seen: {value['first_seen']} | Last observed: {value['last_seen']}", job["url"], *notes])
    return "\n".join(lines)


def baseline_summary(state, board):
    status = state["boards"][board]
    if not status["initialized"]:
        return f"Baseline pending: {board}; first attempted: {status.get('first_attempt_at', 'unknown')}"
    if not status.get("baseline_at"):
        return f"Baseline: {board} — established before tracking; original suppression details unavailable"
    return (f"Baseline: {board} — {status['baseline_at']}; policy: {status['baseline_policy']}; "
            f"matching postings: {status['baseline_matching_count']}; "
            f"unique matches initially suppressed: {status['baseline_suppressed_count']}; "
            f"first attempted: {status.get('first_attempt_at', 'unknown')}")


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    # Serialize once, before creating the replacement. Pretty streaming JSON
    # incurs millions of Python writes across a board cycle on the small VM.
    payload = json.dumps(state, ensure_ascii=False, separators=(",", ":")) + "\n"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def ready_delivery(state, now, *, eligible=None):
    """Select one due lead without mutating the durable outbox."""
    if state.get("notification_retry_at", 0) > now:
        return None
    priorities = {"Newly discovered": 0, "New match on an existing posting": 1,
                  "Present at first successful check": 2}
    # Preserve FIFO within each kind; a catch-up batch must not hold new listings.
    for event in sorted(state["pending"], key=lambda lead: priorities.get(lead["kind"], 3)):
        if event.get("next_delivery_at", 0) <= now and (eligible is None or eligible(event)):
            return event
    return None


def delivery_snapshot(state, event):
    """Send the last observed lead even if a later board check failed."""
    snapshot = copy.deepcopy(event)
    board = state["jobs"][event["key"]]["board"]
    snapshot["source_check_failed"] = bool(state["boards"][board].get("last_error"))
    return snapshot


def acknowledge_delivery(state, event, now, error=None):
    """Only the coordinator updates history, including delayed worker results."""
    current = next((lead for lead in state["pending"] if lead["event_id"] == event["event_id"]), None)
    if error is not None:
        state["delivery_failures_total"] = state.get("delivery_failures_total", 0) + 1
        retry_after = error.retry_after if isinstance(error, NotificationError) else 0
        if retry_after > 0:
            state["notification_retry_at"] = now + retry_after
        if current is not None:
            attempts = current.get("delivery_attempts", 0) + 1
            delay = min(900, 15 * 2 ** min(attempts - 1, 6))
            current.update(delivery_attempts=attempts, next_delivery_at=now + max(delay, retry_after),
                           last_delivery_error=type(error).__name__)
        return
    if event["event_id"] not in state["notified"]:
        state["notified"].append(event["event_id"])
        record_confirmation(state, event, now)
        state["last_delivery"] = {"event_id": event["event_id"], "kind": event["kind"],
                                  "confirmed_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                                  "queued_at": event.get("queued_at", "")}
    if current is not None:
        state["pending"].remove(current)


def deliver_pending(state, sender, checkpoint):
    """Save each acknowledgement. Failed delivery stays pending for retry.

    A process crash after remote acceptance but before checkpoint can duplicate
    an alert. Delivery is at-least-once, not an exactly-once promise.
    """
    failures = 0
    while True:
        event = ready_delivery(state, time.time())
        if event is None:
            break
        try:
            sender([delivery_snapshot(state, event)])
        except Exception as exc:
            failures += 1
            acknowledge_delivery(state, event, time.time(), exc)
            checkpoint(state)
            continue
        acknowledge_delivery(state, event, time.time())
        checkpoint(state)
    if failures:
        raise ValueError(f"{failures} alert delivery failure(s); retained for retry")


def report(state, checked_at, *, health=None):
    leads = [value for value in state["jobs"].values() if value["active"] and value["matched"]]
    leads.sort(key=lambda value: (value["first_seen"], value["company"]), reverse=True)
    lines = ["# Job monitor", "", f"Checked: {checked_at}",
             f"Current matching leads: {len(leads)} | Pending alerts: {len(state['pending'])}", "",
             "Newly discovered does not mean newly published. Screening is advisory; verify the official advert.", ""]
    if health is not None:
        lines.extend(["## Monitor health", "", render_health(health), ""])
    for board, status in state["boards"].items():
        lines.append(baseline_summary(state, board))
        if status.get("last_error"):
            lines.append(f"Source needs attention: {board} — {status['last_error']}; last successful check: {status.get('last_success', 'none')}")
    for event in state["pending"]:
        if event.get("delivery_attempts"):
            retry_at = datetime.fromtimestamp(event["next_delivery_at"], timezone.utc).isoformat()
            lines.append(f"Alert retry: {event['company']} — {event['job']['title']}; "
                         f"failed attempts: {event['delivery_attempts']}; next eligible attempt: {retry_at}")
    for lead in leads[:50]:
        job = lead["job"]
        lines.extend(["", f"## {lead['company']} — {job['title']}", job["url"],
                      f"Location: {job['location']}", f"First seen: {lead['first_seen']}",
                      f"{job['date_type']}: {job['published_at'] or 'unknown'}",
                      f"Last seen: {lead['last_seen']}", lead["screening"]["sponsorship"],
                      "; ".join(lead["screening"]["concerns"])])
    suppressed = [lead for lead in leads if lead.get("baseline_suppressed_at")]
    if suppressed:
        lines.extend(["", "## Matching postings initially suppressed at baseline", "",
                      "No initial alert was queued for these postings. Later changes may have produced alerts.",
                      "This list includes all currently matching active postings with recorded suppression.", ""])
        for lead in suppressed:
            job = lead["job"]
            lines.append(f"- {lead['company']} — {job['title']} | {job['location']} | "
                         f"Initially suppressed: {lead['baseline_suppressed_at']} | {job['url']}")
    return "\n".join(lines) + "\n"


def validate_sources(config):
    sources = [source for source in config["sources"] if source.get("enabled", True)]
    identities = [(source["provider"], source["board"]) for source in sources]
    if len(set(identities)) != len(identities) or not sources:
        raise ValueError("Configure unique, enabled employer boards")
    if config.get("baseline_policy", "suppress") not in ("suppress", "notify") or any(
            source.get("baseline_policy", config.get("baseline_policy", "suppress")) not in ("suppress", "notify")
            for source in sources):
        raise ValueError("baseline_policy must be suppress or notify")
    return sources


def validate_config(config, profile):
    sources = validate_sources(config)
    if not isinstance(profile.get("experience_years"), (int, float)) or profile["experience_years"] < 0 or not isinstance(profile.get("title_keywords"), list) or not profile["title_keywords"]:
        raise ValueError("Profile needs experience_years and title_keywords")
    validate_profile(profile)
    return sources


def read_health(config, state_path, health_path=None, *, now=None):
    """Read snapshots and recompute freshness without loading matching settings."""
    validate_sources(config)
    state_path = Path(state_path)
    path = Path(health_path) if health_path is not None else state_path.with_name("health.json")
    try:
        snapshot = json.loads(path.read_text())
    except FileNotFoundError:
        snapshot = {}
    else:
        if not isinstance(snapshot, dict) or snapshot.get("version") != 1:
            raise ValueError("Invalid health snapshot; posting history was not changed")
    return build_health(read_state(state_path), config, time.time() if now is None else now,
                        worker=snapshot.get("worker"))


def run(config, profile, state_path, report_path, *, notify=False, dry_run=False, fetcher=fetch,
        sender=None):
    previous = read_state(state_path)
    sources = validate_config(config, profile)
    def retrieve(source):
        try:
            return source, fetcher(source), ""
        except Exception as exc:
            return source, [], f"{type(exc).__name__}" + (f" HTTP {exc.code}" if hasattr(exc, "code") else "")
    state = previous
    errors = 0
    delivery_failed = False
    now = datetime.now(timezone.utc).isoformat()
    with ThreadPoolExecutor(max_workers=min(4, len(sources))) as executor:
        futures = [executor.submit(retrieve, source) for source in sources]
        for future in as_completed(futures):
            result = future.result()
            errors += bool(result[2])
            now = datetime.now(timezone.utc).isoformat()
            state = apply_scan(state, [result], profile, now,
                               baseline_policy=config.get("baseline_policy", "suppress"))
            board = result[0]["provider"] + ":" + result[0]["board"]
            if not previous["boards"].get(board, {}).get("initialized"):
                print(baseline_summary(state, board))
            if not dry_run:
                save_state(state_path, state)
                if notify:
                    try:
                        deliver_pending(state, sender or telegram, lambda current: save_state(state_path, current))
                    except ValueError as exc:
                        print(f"Alert delivery needs attention: {exc}")
                        delivery_failed = True
    print(f"Checked {len(sources)} boards | Source errors: {errors} | Pending leads: {len(state['pending'])}")
    if dry_run:
        print("Dry run: no history, report or notifications saved")
        return 1 if errors else 0
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report(state, now), encoding="utf-8")
    print(f"Report: {report_path}")
    return 1 if errors or delivery_failed else 0


def main():
    parser = argparse.ArgumentParser(description="Check public employer boards once; retain posting history")
    parser.add_argument("--config", type=Path, default=Path("sources.json"))
    parser.add_argument("--profile", type=Path, default=Path("profile.json"))
    parser.add_argument("--state", type=Path, default=Path("state.json"))
    parser.add_argument("--report", type=Path, default=Path("latest.md"))
    parser.add_argument("--notify", action="store_true", help="Deliver pending alerts")
    parser.add_argument("--channel", choices=("telegram", "discord"), default="telegram")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--watch", action="store_true", help="Keep polling with independent delivery retries")
    parser.add_argument("--interval", type=float, default=60,
                        help=f"Seconds between healthy board checks (minimum {MIN_POLL_INTERVAL})")
    parser.add_argument("--health", action="store_true", help="Read health and recompute freshness; no fetching, saving or sending")
    parser.add_argument("--menu", action="store_true", help="Open the read-only interactive menu; no fetching, saving or sending")
    parser.add_argument("--health-file", type=Path, help="Worker health snapshot (default: health.json beside state)")
    parser.add_argument("--audit", action="store_true", help="Read recorded rejection reasons; no fetching, saving or sending")
    parser.add_argument("--audit-limit", type=int, default=20, help="Maximum rejected jobs to display (default: 20)")
    parser.add_argument("--audit-title", default="", help="Search rejected titles by a case-insensitive substring")
    args = parser.parse_args()
    conflicts = [name for name in ("health", "audit", "watch", "dry_run", "notify") if getattr(args, name)]
    if args.menu and conflicts:
        parser.error("--menu cannot be combined with " + ", ".join("--" + name.replace("_", "-") for name in conflicts))
    try:
        if args.menu:
            from job_monitor.cli_menu import run_menu
            return run_menu(config_path=args.config, profile_path=args.profile, state_path=args.state,
                            health_path=args.health_file)
        config = json.loads(args.config.read_text())
        if args.audit:
            if args.health:
                raise ValueError("Choose --audit or --health")
            if args.audit_limit < 1:
                raise ValueError("--audit-limit must be positive")
            profile = json.loads(args.profile.read_text())
            validate_config(config, profile)
            if not args.state.is_file():
                raise ValueError("No posting history available for audit")
            print(rejection_audit(read_state(args.state), config, profile,
                                  limit=args.audit_limit, title=args.audit_title))
            return 0
        if args.health:
            current = read_health(config, args.state, args.health_file)
            print(render_health(current))
            return 0 if current["status"] == "healthy" else 1
        profile = json.loads(args.profile.read_text())
        if args.dry_run:
            return run(config, profile, args.state, args.report, notify=False, dry_run=True)
        args.state.parent.mkdir(parents=True, exist_ok=True)
        with args.state.with_suffix(".lock").open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print("Another check is running; skipped")
                return 0
            sender = sender_for(args.channel) if args.notify else None
            if args.watch:
                from job_monitor.watch import run_watch
                return run_watch(config, profile, args.state, args.report, interval=args.interval,
                                 sender=sender, health_path=args.health_file)
            return run(config, profile, args.state, args.report, notify=args.notify, sender=sender)
    except (ValueError, OSError, KeyError) as exc:
        operation = "Health unavailable" if args.health else "Audit unavailable" if args.audit else "Check stopped"
        print(f"{operation}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
