"""Read-only terminal adapter over saved monitoring and review data."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import time

from job_monitor.health import render_health, timestamp
from job_monitor.monitor import read_health, read_state, validate_sources
from job_monitor.review_store import ReviewStore
from job_monitor.screening import first_publication, REJECTION_LABELS, validate_profile, DEFAULT_EXCLUDED_TITLES


class MenuExit(Exception):
    pass


class MenuData:
    """Read side only: no initialisation, scans, delivery, or policy mutations."""
    def __init__(self, config_path, profile_path, state_path, health_path=None):
        self.config_path = Path(config_path).resolve()
        self.profile_path = Path(profile_path).resolve()
        self.state_path = Path(state_path).resolve()
        self.health_path = Path(health_path).resolve() if health_path else self.state_path.with_name("health.json")

    def sources(self):
        config = json.loads(self.config_path.read_text())
        validate_sources(config)
        return config

    def health(self):
        return read_health(self.sources(), self.state_path, self.health_path)

    def review(self):
        path = self.state_path.with_name("review.sqlite3")
        if not path.exists():
            return None, None
        store = ReviewStore(path, read_only=True)
        owner = os.environ.get("JOB_MONITOR_REVIEW_USER_ID")
        if not owner:
            with store.connect() as db:
                users = db.execute("SELECT user FROM users LIMIT 2").fetchall()
            if len(users) != 1:
                raise ValueError("Cannot identify one saved profile. Use Discord /filters for your settings.")
            owner = users[0]["user"]
        store.policy(owner)  # An unreadable saved policy must not silently become JSON settings.
        return store, owner

    def settings(self):
        store, owner = self.review()
        if store is not None:
            version, profile = store.policy(owner)
            worker = store.status(owner)
            loaded = bool(worker and worker["applied"] == version and not worker["error"]
                          and 0 <= time.time() - worker["heartbeat"] <= 30)
            return profile, f"Saved Discord settings — {'loaded by monitor' if loaded else 'activation not confirmed'}", store, owner
        profile = json.loads(self.profile_path.read_text())
        validate_profile(profile)
        return profile, "JSON startup settings — active worker settings not confirmed", None, None

    def postings(self, *, rejected=False, query="", days=None):
        store, owner = self.review()
        if store is not None:
            rows, _ = store.jobs(owner, rejected=rejected, days=days, limit=1_000_000)
            records = [{"job": json.loads(row["payload"]), "company": row["company"],
                        "reasons": reasons, "observed": row["checked"],
                        "error": row["error"]} for row, _, reasons in rows]
            origin = "Saved postings evaluated against saved Discord settings."
            _, settings_note, _, _ = self.settings()
            origin += " " + settings_note + "."
        else:
            if not self.state_path.is_file():
                raise ValueError(f"No saved posting history at {self.state_path}. Open the menu on the machine running the monitor.")
            state = read_state(self.state_path)
            enabled = {source['provider'] + ':' + source['board'] for source in self.sources()['sources'] if source.get('enabled', True)}
            records = [{"job": record["job"], "company": record["company"],
                        "reasons": record.get("rejection_reasons", []),
                        "observed": timestamp(record.get("last_seen", "")),
                        "error": state["boards"].get(record["board"], {}).get("last_error", "")}
                       for record in state["jobs"].values()
                       if record.get("active") and record["board"] in enabled and bool(record["matched"]) != rejected]
            origin = "Historical decisions from the last scan; current filters have not been re-evaluated."
        current = datetime.now(timezone.utc)
        if days is not None:
            records = [record for record in records if self.publication(record["job"], current) is not None
                       and (current - self.publication(record["job"], current)).total_seconds() <= days * 86400]
        query = query.casefold().strip()
        if query:
            records = [record for record in records if query in " ".join(
                (record["job"]["title"], record["company"], record["job"]["location"])).casefold()]
        records.sort(key=lambda record: self.publication(record["job"], current)
                     or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return records, origin

    @staticmethod
    def publication(job, now):
        published = first_publication(job)
        return published if published is not None and published <= now else None


class Menu:
    def __init__(self, data, reader, output):
        self.data, self.reader, self.output = data, reader, output

    def say(self, text=""):
        # Public posting text must not execute terminal control sequences.
        text = str(text)
        self.output.write("".join(char for char in text if char in "\n\t" or char.isprintable()) + "\n")
        self.output.flush()

    def ask(self, prompt):
        self.output.write(prompt)
        self.output.flush()
        answer = self.reader.readline()
        if not answer:
            raise EOFError
        answer = answer.strip()
        if answer == "0":
            raise MenuExit
        return answer

    def pause(self):
        self.ask("Enter: Back | 0: Exit > ")

    def guarded(self, action):
        try:
            action()
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
            self.say(f"\nThis view is unavailable: {error}")
            self.say("Check the saved files on this machine. No monitoring process was stopped and no history was reset.")
            self.pause()

    def header(self):
        self.say("\nJob Monitor")
        self.say(f"Data source: this machine ({socket.gethostname()})")
        self.say(f"Saved data: {self.data.state_path.parent}")
        try:
            health = self.data.health()
            successes = [row["success_age_seconds"] for row in health["sources"] if row["success_age_seconds"] is not None]
            checked = f"{min(successes):g}s ago" if successes else "unavailable"
            self.say(f"Monitoring: {health['status']} | Latest successful board check: {checked}")
            if health['issues']:
                self.say(f"Needs attention: {health['issues'][0]}. Choose 2 for details.")
        except (OSError, ValueError, KeyError, TypeError) as error:
            self.say(f"Monitoring status unavailable: {error}. Choose 2 to inspect.")

    def run(self):
        while True:
            self.header()
            self.say("\n1  Find jobs to apply to\n2  Check monitoring\n3  Improve job matching\n0  Exit")
            choice = self.ask("Choose > ")
            actions = {"1": self.find_jobs, "2": self.monitoring, "3": self.improve}
            if choice in actions:
                self.guarded(actions[choice])
            else:
                self.say("Choose 1, 2, 3, or 0. Nothing changed.")

    def find_jobs(self, *, rejected=False):
        query, days, offset = "", None, 0
        while True:
            records, origin = self.data.postings(rejected=rejected, query=query, days=days)
            self.say("\nExcluded postings" if rejected else "\nJobs to apply to — saved observations")
            self.say(origin)
            self.say(f"Search: {query or 'Any'} | Posting age: {str(days) + ' days' if days else 'Saved rules / last scan'}")
            if days is not None:
                self.say("Temporary posting-age search; saved alert settings are unchanged. Unknown original dates cannot meet this limit.")
            offset = min(offset, max(0, (len(records) - 1) // 5 * 5))
            if not records:
                self.say("No saved postings match this view. This does not prove that no relevant jobs exist.")
            now = datetime.now(timezone.utc)
            for number, record in enumerate(records[offset:offset + 5], offset + 1):
                job = record["job"]
                published = self.data.publication(job, now)
                date = published.astimezone(timezone.utc).strftime("%d %b %Y, %H:%M UTC") if published else "Unknown — original posting date unavailable"
                self.say(f"\n{number}. {job['title']} — {record['company']}\n   {job['location']}\n   First published: {date}\n   {job['url']}")
                if rejected:
                    self.say("   Excluded: " + ("; ".join(REJECTION_LABELS.get(reason, reason) for reason in record["reasons"]) or "Historical reason unavailable"))
                if record['error'] or record['observed'] is None or time.time() - record['observed'] > 180:
                    self.say("   Observation is stale or unavailable; verify the employer posting is still open.")
            self.say(f"\n{len(records)} saved postings. Known original publication dates sort newest first; unknown dates follow.")
            choice = self.ask("N: Next | P: Previous | S: Search | A: Posting age | B: Back | 0: Exit > ").casefold()
            if choice == "b":
                return
            if choice == "n":
                offset = offset + 5 if offset + 5 < len(records) else offset
            elif choice == "p":
                offset = max(0, offset - 5)
            elif choice == "s":
                candidate = self.ask("Title, employer or location (Enter: clear, B: cancel) > ")
                if candidate.casefold() != "b":
                    query, offset = candidate, 0
            elif choice == "a":
                value = self.ask("Original posting age in days, 1–365 (Enter: saved rules, B: cancel) > ")
                if value.casefold() == "b":
                    continue
                if not value:
                    days = None
                elif value.isascii() and value.isdigit() and 1 <= int(value) <= 365:
                    days = int(value)
                else:
                    self.say("Enter a whole number from 1 to 365. Unknown original dates cannot meet an age limit.")
                    continue
                offset = 0
            elif choice not in ("n", "p"):
                self.say("Choose N, P, S, A, B, or 0.")

    def monitoring(self):
        while True:
            self.say("\nCheck monitoring\n1  Status and delivery\n2  Tracked employers\nB  Back\n0  Exit")
            choice = self.ask("Choose > ").casefold()
            if choice == "b":
                return
            if choice == "1":
                self.guarded(self.status)
            elif choice == "2":
                self.guarded(self.employers)
            else:
                self.say("Choose 1, 2, B, or 0.")

    def status(self):
        self.say("\n" + render_health(self.data.health()))
        self.say("This describes saved observations on this machine, not a remote service check.")
        self.say("On the VM, inspect service errors with: journalctl -u job-monitor.service -n 30 --no-pager")
        self.pause()

    def employers(self):
        sources = [source for source in self.data.sources()['sources'] if source.get('enabled', True)]
        try:
            rows = {row['board']: row for row in self.data.health()['sources']}
        except (OSError, ValueError, KeyError, TypeError) as error:
            rows = {}
            self.say(f"Saved health is unavailable: {error}")
        self.say(f"\nTracked employers — {len(sources)} enabled boards")
        for source in sorted(sources, key=lambda source: source['company'].casefold()):
            board = source['provider'] + ':' + source['board']
            row = rows.get(board, {})
            self.say(f"{source['company']} | {board} | {row.get('status', 'unavailable')}")
        self.say("Coverage is limited to these configured boards; it does not include every employer or vacancy.")
        self.pause()

    def improve(self):
        while True:
            _, note, store, owner = self.data.settings()
            self.say("\nImprove job matching\nSettings source: " + note)
            if store is not None:
                suggestions = store.title_suggestions(owner)
                for group in suggestions['groups'][:3]:
                    self.say(f"Consider including {group['title']} — {len(group['jobs'])} unreviewed postings pass your other filters.")
                    self.say("   " + "; ".join(row['company'] for row in group['jobs'][:3]))
                if not suggestions['groups']:
                    self.say("No title-only suggestions. That does not establish that other exclusions are correct.")
                self.say("Suggestions cover engineer/developer titles with no other blocker, using recent successful checks.")
                if suggestions['unknown_dates']:
                    self.say(f"Original publication unavailable: {suggestions['unknown_dates']} postings cannot pass your age limit.")
                if suggestions['stale_boards']:
                    self.say(f"Suggestions omit {suggestions['stale_boards']} boards with stale or failed checks.")
            else:
                self.say("Grouped suggestions require saved Discord review data. Historical exclusions remain browsable.")
            self.say("This menu does not change filters or dismiss reviews. Use Discord /review to preview a suggestion; /filters to change settings.")
            choice = self.ask("1: Browse exclusions | 2: View alert settings | B: Back | 0: Exit > ").casefold()
            if choice == 'b':
                return
            if choice == '1':
                self.guarded(lambda: self.find_jobs(rejected=True))
            elif choice == '2':
                self.guarded(self.settings)
            else:
                self.say("Choose 1, 2, B, or 0.")

    def settings(self):
        profile, note, _, _ = self.data.settings()
        self.say("\nAlert settings\n" + note)
        self.say("Locations: " + ", ".join(profile.get('locations', ['London'])))
        self.say(f"UK remote: {'Allowed' if profile.get('allow_uk_remote', True) else 'Excluded'}")
        self.say(f"Unclear remote eligibility: {'Allowed with warning' if profile.get('allow_ambiguous_remote', False) else 'Excluded'}")
        days = profile.get('max_posting_age_days')
        self.say(f"Original posting age: {str(days) + ' days; unknown dates excluded' if days else 'Any age'}")
        self.say("Allowed title phrases: " + ", ".join(profile['title_keywords']))
        self.say("Excluded title phrases: " + ", ".join(profile.get('excluded_title_keywords', DEFAULT_EXCLUDED_TITLES)))
        self.say("Staff-level titles are excluded except Member of Technical Staff. Explicit negative sponsorship wording is excluded.")
        self.say(f"Experience: {profile.get('experience_years', 'not supplied')} years — used for warnings, not an automatic cutoff.")
        self.say("Use Discord /filters to preview, save, and confirm activation of changes.")
        self.pause()


def run_menu(*, config_path, profile_path, state_path, health_path=None, input_stream=None, output_stream=None):
    reader = input_stream if input_stream is not None else sys.stdin
    output = output_stream if output_stream is not None else sys.stdout
    if not reader.isatty():
        output.write("The menu needs an interactive terminal. Use --health or --audit for scripted inspection; use ssh -t for a remote menu.\n")
        return 2
    menu = Menu(MenuData(config_path, profile_path, state_path, health_path), reader, output)
    try:
        menu.run()
    except (MenuExit, EOFError, KeyboardInterrupt):
        menu.say("\nExited. History and alert settings were not changed.")
    return 0
