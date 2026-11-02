"""Read-only health calculations; no network calls or notification side effects."""
from datetime import datetime, timezone
import math
import statistics


def stamp(now):
    return datetime.fromtimestamp(now, timezone.utc).isoformat()


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def elapsed(start, end):
    start = timestamp(start)
    return round(end - start, 3) if start is not None and end >= start else None


def latency_stats(samples):
    values = sorted(sample["queue_to_confirmation_seconds"] for sample in samples
                    if isinstance(sample.get("queue_to_confirmation_seconds"), (int, float))
                    and math.isfinite(sample["queue_to_confirmation_seconds"])
                    and sample["queue_to_confirmation_seconds"] >= 0)
    if not values:
        return {"sample_count": 0, "mean_seconds": None, "p95_seconds": None, "max_seconds": None}
    return {"sample_count": len(values), "mean_seconds": round(statistics.mean(values), 3),
            "p95_seconds": values[math.ceil(len(values) * 0.95) - 1], "max_seconds": values[-1]}


def record_confirmation(state, event, now):
    metrics = state.setdefault("delivery_metrics", {"confirmed_count": 0, "samples": []})
    metrics["confirmed_count"] += 1
    sample = {"event_id": event["event_id"], "kind": event["kind"],
              "queued_at": event.get("queued_at", ""), "confirmed_at": stamp(now),
              "queue_to_confirmation_seconds": elapsed(event.get("queued_at"), now)}
    metrics["samples"] = (metrics["samples"] + [sample])[-200:]


def build_health(state, config, now, *, worker=None):
    """Recompute ages at read time; an old 'healthy' snapshot cannot stay healthy."""
    worker = dict(worker or {})
    interval = worker.get("interval_seconds", 60)
    if not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval < 60:
        interval = 60
    stale_after = interval * 3
    issues = []
    if worker.get("policy_error"):
        issues.append(worker["policy_error"])
    heartbeat_age = elapsed(worker.get("heartbeat_at"), now)
    worker["heartbeat_age_seconds"] = heartbeat_age
    if worker.get("status") != "running":
        issues.append("Worker heartbeat unavailable or worker stopped")
    elif heartbeat_age is None or heartbeat_age > 60:
        issues.append("Worker heartbeat stale or timestamp invalid")

    rows = []
    for source in config["sources"]:
        if not source.get("enabled", True):
            continue
        board = source["provider"] + ":" + source["board"]
        status = state["boards"].get(board, {})
        age = elapsed(status.get("last_success"), now)
        if status.get("last_error"):
            condition = "failing"
        elif status.get("last_success") and (age is None or age > stale_after):
            condition = "stale"
        elif not status.get("last_success"):
            condition = "waiting"
        else:
            condition = "healthy"
        if condition != "healthy":
            issues.append(f"{board}: {condition}")
        rows.append({"board": board, "company": source["company"], "status": condition,
                     "last_success": status.get("last_success", ""), "success_age_seconds": age,
                     "last_attempt": status.get("last_attempt", ""), "last_error": status.get("last_error", ""),
                     "consecutive_failures": status.get("consecutive_failures", 0),
                     "next_poll_at": status.get("next_poll_at"),
                     "last_fetch_seconds": status.get("last_fetch_seconds")})

    pending = []
    pause = max(0, state.get("notification_retry_at", 0) - now)
    for event in state["pending"]:
        age = elapsed(event.get("queued_at"), now)
        board = state["jobs"][event["key"]]["board"]
        if pause > 0:
            condition = "channel rate limit"
        elif event.get("next_delivery_at", 0) > now:
            condition = "retry backoff"
        else:
            condition = "ready or in flight"
        if age is None:
            issues.append(f"Delivery {event['event_id'][:12]}: queue time unavailable or invalid")
        elif age > 90:
            issues.append(f"Delivery {event['event_id'][:12]}: queued over 90 seconds")
        if event.get("delivery_attempts"):
            issues.append(f"Delivery {event['event_id'][:12]}: failed attempt recorded")
        pending.append({"event_id": event["event_id"], "company": event["company"],
                        "title": event["job"]["title"], "queued_at": event.get("queued_at", ""),
                        "age_seconds": age, "status": condition,
                        "failed_attempts": event.get("delivery_attempts", 0),
                         "source_check_failed": bool(state["boards"][board].get("last_error")),
                        "next_delivery_at": event.get("next_delivery_at"),
                        "last_error": event.get("last_delivery_error", "")})

    metrics = state.get("delivery_metrics", {})
    samples = metrics.get("samples", [])
    new_samples = [sample for sample in samples if sample.get("kind") == "Newly discovered"]
    last = dict(state.get("last_delivery", {}))
    confirmed = timestamp(last.get("confirmed_at"))
    last["queue_to_confirmation_seconds"] = elapsed(last.get("queued_at"), confirmed) if confirmed is not None else None
    return {"version": 1, "checked_at": stamp(now), "status": "attention" if issues else "healthy",
            "worker": worker, "source_stale_after_seconds": stale_after,
            "sources": rows, "issues": issues,
            "delivery": {"pending_count": len(pending), "pending": pending,
                         "failed_attempts_total": state.get("delivery_failures_total", 0),
                         "channel_pause_seconds": round(pause, 3), "last_confirmation": last,
                         "tracked_confirmations": metrics.get("confirmed_count", 0),
                         "recent_latency": latency_stats(samples),
                         "newly_discovered_latency": latency_stats(new_samples)}}


def duration(value):
    return "unknown" if value is None else f"{value:g}s"


def render_health(health):
    sources, delivery, worker = health["sources"], health["delivery"], health["worker"]
    counts = {kind: sum(row["status"] == kind for row in sources)
              for kind in ("healthy", "failing", "stale", "waiting")}
    lines = [f"Health: {health['status']} | Updated: {health['checked_at']}",
             f"Worker: {worker.get('status', 'unverified')} | Heartbeat age: {duration(worker['heartbeat_age_seconds'])}",
             f"Boards: {len(sources)} | Healthy: {counts['healthy']} | Failing: {counts['failing']} | "
             f"Stale: {counts['stale']} | Waiting: {counts['waiting']}",
             f"Delivery: pending {delivery['pending_count']} | Failed attempts tracked: {delivery['failed_attempts_total']} | "
             f"Channel pause: {duration(delivery['channel_pause_seconds'])}"]
    if "policy_version" in worker:
        lines.append(f"Alert settings: version {worker['policy_version']} loaded")
    last = delivery["last_confirmation"]
    lines.append(f"Last confirmation: {last.get('confirmed_at', 'none')} | "
                 f"Queue to confirmation: {duration(last['queue_to_confirmation_seconds'])}")
    recent = delivery["recent_latency"]
    lines.append(f"Recent queue-to-confirmation latency: {recent['sample_count']} timed samples | "
                 f"p95 {duration(recent['p95_seconds'])} | max {duration(recent['max_seconds'])}")
    new = delivery["newly_discovered_latency"]
    lines.append(f"Newly discovered alerts: {new['sample_count']} timed samples | p95 {duration(new['p95_seconds'])}")
    lines.append("Timing ends at API confirmation; original publication and phone delivery latency are unmeasured.")
    for row in sources:
        if row["status"] != "healthy":
            retry = stamp(row["next_poll_at"]) if row["next_poll_at"] is not None else "unknown"
            lines.append(f"Source: {row['board']} | {row['status']} | Success age: {duration(row['success_age_seconds'])} | "
                         f"Last error: {row['last_error'] or 'none'} | Next eligible poll: {retry}")
    for event in delivery["pending"]:
        retry = stamp(event["next_delivery_at"]) if event["next_delivery_at"] is not None else "now"
        lines.append(f"Pending: {event['company']} — {event['title']} | Age: {duration(event['age_seconds'])} | "
                     f"{event['status']} | Failed attempts: {event['failed_attempts']} | Next eligible send: {retry}")
    for issue in health["issues"]:
        lines.append("Attention: " + issue)
    return "\n".join(lines)
