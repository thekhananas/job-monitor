"""Offline contract checks; outputs stay in temporary directories."""
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr
from concurrent.futures import Future, ThreadPoolExecutor
import copy
import io
import tempfile
import threading
import urllib.error
import json
import os
import fcntl
import signal
import subprocess
import shutil
import tarfile
import time
import unittest
import importlib.util
import sqlite3
import configparser
import shlex
import itertools
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

from job_monitor.monitor import acknowledge_delivery, apply_scan, canonical_url, deliver_pending, empty_state, read_state, ready_delivery, report, run, save_state, validate_config, rejection_audit
from job_monitor.screening import screen, screen_with_reasons
from job_monitor.sources import normalise
from job_monitor.notifications import discord, discord_text, NotificationError
from job_monitor.watch import Watcher, retry_after_seconds
from job_monitor.health import build_health, render_health, stamp
from deployment.deploy import (Deployer, DeploymentError, FILES, make_bundle,
                              read_manifest, client)
from datetime import datetime, timezone
from job_monitor.review_store import ReviewStore, ReviewConflict
from job_monitor.review_runtime import ReviewRuntime
from job_monitor.review_bot import wait_for_activation
from job_monitor.sources import PublicationEnricher
from job_monitor.prioritization import Evaluator, ordering, validate_candidate
from job_monitor.rank_jobs import database_postings, input_postings, main as rank_main, rank
from job_monitor.learning import LearningStore, LearningService, TARGET, context_id
from job_monitor.review_store import signature


SOURCE = {"provider": "ashby", "board": "example", "company": "Example"}
PROFILE = {"experience_years": 3.5, "allow_uk_remote": True,
           "title_keywords": ["software engineer", "back end engineer", "machine learning engineer"]}
EXPANDED_PROFILE = {**PROFILE, "allow_ambiguous_remote": True,
                    "title_keywords": PROFILE["title_keywords"] + ["software developer", "full stack engineer",
                        "front end engineer", "platform engineer", "devops engineer", "site reliability engineer",
                        "member of technical staff"]}


def job(identity="1", **changes):
    result = {"id": identity, "title": "Software Engineer", "location": "London",
              "description": "Build Python services. Visa sponsorship is available.",
              "url": f"https://example.test/jobs/{identity}", "published_at": "",
              "date_type": "unknown", "updated_at": ""}
    result.update(changes)
    return result


def scan(state, jobs, now="2026-10-12T10:00:00Z", source=SOURCE):
    return apply_scan(state, [(source, jobs, "")], PROFILE, now)


class MonitorContracts(unittest.TestCase):
    def wait_for(self, predicate, tick=None):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if tick:
                tick()
            if predicate():
                return
            time.sleep(0.005)
        self.fail("Worker did not reach expected state")

    def test_health_recomputes_worker_and_source_freshness(self):
        state = scan(empty_state(), [job()], now=stamp(1000))
        worker = {"status": "running", "heartbeat_at": stamp(1000), "interval_seconds": 60}
        fresh = build_health(state, {"sources": [SOURCE]}, 1030, worker=worker)
        self.assertEqual(fresh["status"], "healthy")
        stale = build_health(state, {"sources": [SOURCE]}, 1200, worker=worker)
        self.assertEqual(stale["status"], "attention")
        self.assertEqual(stale["sources"][0]["status"], "stale")
        self.assertTrue(any("Worker heartbeat stale" in issue for issue in stale["issues"]))
        self.assertEqual(state["boards"]["ashby:example"]["last_success"], stamp(1000))
        stopped = build_health(state, {"sources": [SOURCE]}, 1030, worker={**worker, "status": "stopped"})
        self.assertEqual(stopped["status"], "attention")
        self.assertEqual(stopped["sources"][0]["status"], "healthy")
        self.assertNotIn("Publication latency: 0", render_health(stopped))

    def test_health_distinguishes_pending_causes_and_unknown_legacy_queue_times(self):
        state = scan(scan(empty_state(), [job()], now=stamp(990)), [job(), job("2")], now=stamp(1000))
        event = state["pending"][0]
        event.update(delivery_attempts=1, next_delivery_at=1020, last_delivery_error="NotificationError")
        worker = {"status": "running", "heartbeat_at": stamp(1010)}
        state["boards"]["ashby:example"]["last_error"] = "TimeoutError"
        held = build_health(state, {"sources": [SOURCE]}, 1010, worker=worker)
        self.assertEqual(held["sources"][0]["status"], "failing")
        self.assertEqual(held["delivery"]["pending"][0]["status"], "retry backoff")
        self.assertTrue(held["delivery"]["pending"][0]["source_check_failed"])
        state["boards"]["ashby:example"]["last_error"] = ""
        state["notification_retry_at"] = 1030
        paused = build_health(state, {"sources": [SOURCE]}, 1010, worker=worker)
        self.assertEqual(paused["delivery"]["pending"][0]["status"], "channel rate limit")
        self.assertEqual(paused["delivery"]["channel_pause_seconds"], 20)
        state["notification_retry_at"] = 0
        backoff = build_health(state, {"sources": [SOURCE]}, 1010, worker=worker)
        self.assertEqual(backoff["delivery"]["pending"][0]["status"], "retry backoff")
        overdue = build_health(state, {"sources": [SOURCE]}, 1100, worker={**worker, "heartbeat_at": stamp(1100)})
        self.assertEqual(overdue["delivery"]["pending"][0]["age_seconds"], 100)
        self.assertTrue(any("queued over 90 seconds" in issue for issue in overdue["issues"]))
        event.pop("queued_at")
        legacy = build_health(state, {"sources": [SOURCE]}, 1100, worker=worker)
        self.assertIsNone(legacy["delivery"]["pending"][0]["age_seconds"])
        self.assertTrue(any("queue time unavailable" in issue for issue in legacy["issues"]))

    def test_latency_uses_queue_time_separates_catchup_and_counts_acknowledgements_once(self):
        previous = scan(empty_state(), [job(), job("2", location="Berlin")], now=stamp(1000))
        state = scan(previous, [job(), job("2"), job("3")], now=stamp(2000))
        catchup, discovered = state["pending"]
        acknowledge_delivery(state, catchup, 2005)
        acknowledge_delivery(state, catchup, 2010)
        self.assertEqual(state["delivery_metrics"]["confirmed_count"], 1)
        self.assertEqual(state["last_delivery"]["confirmed_at"], stamp(2005))
        self.assertEqual(state["delivery_metrics"]["samples"][0]["queue_to_confirmation_seconds"], 5)
        acknowledge_delivery(state, discovered, 2007.5)
        summary = build_health(state, {"sources": [SOURCE]}, 2008,
                               worker={"status": "running", "heartbeat_at": stamp(2008)})
        self.assertEqual(summary["delivery"]["recent_latency"]["sample_count"], 2)
        self.assertEqual(summary["delivery"]["recent_latency"]["p95_seconds"], 7.5)
        self.assertEqual(summary["delivery"]["newly_discovered_latency"]["sample_count"], 1)
        self.assertEqual(summary["delivery"]["last_confirmation"]["queue_to_confirmation_seconds"], 7.5)
        self.assertIn("original publication and phone delivery latency are unmeasured", render_health(summary))

    def test_latency_window_is_bounded_without_erasing_delivery_or_posting_history(self):
        jobs = [job(str(number)) for number in range(205)]
        state = scan(scan(empty_state(), [], now=stamp(990)), jobs, now=stamp(1000))
        for event in list(state["pending"]):
            acknowledge_delivery(state, event, 1005)
        self.assertEqual(len(state["delivery_metrics"]["samples"]), 200)
        self.assertEqual(state["delivery_metrics"]["confirmed_count"], 205)
        self.assertEqual(len(state["notified"]), 205)
        self.assertEqual(len(state["jobs"]), 205)
        self.assertFalse(state["pending"])

    def test_health_ignores_disabled_sources_and_does_not_invent_clock_skew_measurements(self):
        state = scan(empty_state(), [job()], now=stamp(1000))
        state["boards"]["ashby:disabled"] = {"initialized": True, "last_error": "TimeoutError"}
        config = {"sources": [SOURCE, {**SOURCE, "board": "disabled", "enabled": False}]}
        result = build_health(state, config, 1010,
                              worker={"status": "running", "heartbeat_at": stamp(1010)})
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(len(result["sources"]), 1)
        state = scan(state, [job(), job("2")], now=stamp(1020))
        acknowledge_delivery(state, state["pending"][0], 1015)
        self.assertIsNone(state["delivery_metrics"]["samples"][0]["queue_to_confirmation_seconds"])

    def test_worker_health_keeps_updating_while_network_is_blocked(self):
        release = threading.Event()
        def blocked_fetch(source):
            release.wait(3)
            return [job()]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            state = scan(empty_state(), [job()], now=stamp(1000))
            save_state(path, state)
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                watcher = Watcher({"sources": [SOURCE]}, PROFILE, path, root / "report.md", polls, delivery,
                                  fetcher=blocked_fetch, now=1000)
                try:
                    watcher.tick(1000)
                    watcher.tick(1070)
                    snapshot = json.loads((root / "health.json").read_text())
                    self.assertEqual(snapshot["worker"]["heartbeat_at"], stamp(1070))
                    self.assertEqual(snapshot["status"], "healthy")
                    watcher.tick(1200)
                    snapshot = json.loads((root / "health.json").read_text())
                    self.assertEqual(snapshot["sources"][0]["status"], "stale")
                    self.assertEqual(snapshot["worker"]["heartbeat_at"], stamp(1200))
                    self.assertIn("## Monitor health", (root / "report.md").read_text())
                    self.assertEqual(read_state(path), state)
                finally:
                    release.set()

    def test_health_cli_is_read_only_and_flags_stale_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = time.time()
            state = scan(empty_state(), [job()], now=stamp(now))
            config = {"sources": [SOURCE]}
            save_state(root / "state.json", state)
            save_state(root / "health.json", build_health(state, config, now,
                       worker={"status": "running", "heartbeat_at": stamp(now)}))
            (root / "sources.json").write_text(json.dumps(config))
            (root / "profile.json").write_text(json.dumps(PROFILE))
            before = {path.name: path.read_bytes() for path in root.iterdir()}
            script = str((Path(__file__).resolve().parent.parent / "scripts/monitor.py"))
            result = subprocess.run([__import__("sys").executable, "-B", script, "--health", "--notify", "--watch"],
                                    cwd=root, capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Health: healthy", result.stdout)
            self.assertEqual(before, {path.name: path.read_bytes() for path in root.iterdir()})
            stale = json.loads((root / "health.json").read_text())
            stale["worker"]["heartbeat_at"] = stamp(now - 120)
            save_state(root / "health.json", stale)
            result = subprocess.run([__import__("sys").executable, "-B", script, "--health"],
                                    cwd=root, capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, 1)
            self.assertIn("Worker heartbeat stale", result.stdout)
            (root / "health.json").write_text('[]')
            result = subprocess.run([__import__("sys").executable, "-B", script, "--health"],
                                    cwd=root, capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, 1)
            self.assertIn("Invalid health snapshot", result.stdout)
            self.assertEqual((root / "state.json").read_bytes(), before["state.json"])

    def test_health_does_not_require_or_parse_the_matching_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = time.time()
            config = {"sources": [SOURCE]}
            state = scan(empty_state(), [job()], now=stamp(now))
            save_state(root / "state.json", state)
            save_state(root / "health.json", build_health(state, config, now,
                       worker={"status": "running", "heartbeat_at": stamp(now)}))
            (root / "sources.json").write_text(json.dumps(config))
            script = str((Path(__file__).resolve().parent.parent / "scripts/monitor.py"))
            for profile_content in (None, "not valid JSON", '{"title_keywords":[]}'):
                with self.subTest(profile_content=profile_content):
                    if profile_content is not None:
                        (root / "profile.json").write_text(profile_content)
                    before = {path.name: path.read_bytes() for path in root.iterdir()}
                    result = subprocess.run([__import__("sys").executable, "-B", script, "--health"],
                                            cwd=root, capture_output=True, text=True, timeout=3)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn("Health: healthy", result.stdout)
                    self.assertEqual(before, {path.name: path.read_bytes() for path in root.iterdir()})

    def test_watch_retries_delivery_while_a_board_fetch_is_blocked(self):
        release = threading.Event()
        started = threading.Event()
        def blocked_fetch(source):
            started.set()
            if not release.wait(3):
                raise RuntimeError("fetch timed out in test")
            return [job(), job("2")]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            save_state(path, scan(scan(empty_state(), [job()]), [job(), job("2")]))
            sender = Mock(side_effect=[ValueError("temporary failure"), None])
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                watcher = Watcher({"sources": [SOURCE]}, PROFILE, path, root / "report.md", polls, delivery,
                                  sender=sender, fetcher=blocked_fetch, now=1000)
                try:
                    watcher.tick(1000)
                    self.assertTrue(started.wait(1))
                    self.wait_for(lambda: watcher.state["pending"][0].get("delivery_attempts") == 1,
                                  lambda: watcher.tick(1000))
                    self.assertEqual(read_state(path)["pending"][0]["next_delivery_at"], 1015)
                    watcher.tick(1014)
                    self.assertEqual(sender.call_count, 1)
                    watcher.tick(1015)
                    self.wait_for(lambda: not watcher.state["pending"], lambda: watcher.tick(1015))
                    self.assertFalse(read_state(path)["pending"])
                    self.assertEqual(sender.call_count, 2)
                    self.assertEqual(len(watcher.polls), 1)
                finally:
                    release.set()

    def test_watch_keeps_polling_and_saving_while_sender_is_blocked(self):
        release = threading.Event()
        started = threading.Event()
        def blocked_send(events):
            started.set()
            if not release.wait(3):
                raise RuntimeError("send timed out in test")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            save_state(path, scan(scan(empty_state(), [job()]), [job(), job("2")]))
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                watcher = Watcher({"sources": [SOURCE]}, PROFILE, path, root / "report.md", polls, delivery,
                                  sender=blocked_send, fetcher=lambda source: [job(), job("2"), job("3")], now=1000)
                try:
                    watcher.tick(1000)
                    self.assertTrue(started.wait(1))
                    self.wait_for(lambda: "ashby:example:3" in read_state(path)["jobs"],
                                  lambda: watcher.tick(1000))
                    self.assertEqual(len(watcher.state["pending"]), 2)
                    self.assertFalse(watcher.state["notified"])
                    # Shutdown stops scheduling but persists an accepted in-flight alert.
                    release.set()
                    self.wait_for(lambda: bool(watcher.state["notified"]), lambda: watcher.tick(1001, stopping=True))
                    self.assertEqual([event["job"]["id"] for event in read_state(path)["pending"]], ["3"])
                    self.assertFalse(watcher.busy())
                finally:
                    release.set()

    def test_watch_source_backoff_survives_restart_and_does_not_hold_healthy_board(self):
        other = {**SOURCE, "board": "healthy"}
        def fetcher(source):
            if source["board"] == "example":
                raise urllib.error.HTTPError("https://example.test", 429, "limited", {"Retry-After": "300"}, None)
            return [job("healthy")]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            previous = scan(empty_state(), [job()])
            previous = scan(previous, [], source=other)
            save_state(path, previous)
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                config = {"sources": [SOURCE, other]}
                watcher = Watcher(config, PROFILE, path, root / "report.md", polls, delivery,
                                  fetcher=fetcher, now=1000)
                watcher.tick(1000)
                self.wait_for(lambda: watcher.state["boards"]["ashby:example"].get("consecutive_failures") == 1,
                              lambda: watcher.tick(1000))
                self.assertEqual(watcher.due["ashby:example"], 1300)
                self.assertEqual(watcher.state["jobs"]["ashby:example:1"], previous["jobs"]["ashby:example:1"])
                watcher.tick(1030)
                self.wait_for(lambda: "ashby:healthy:healthy" in watcher.state["jobs"], lambda: watcher.tick(1030))
                self.assertEqual(watcher.due["ashby:healthy"], 1090)
                restart_fetch = Mock(return_value=[])
                restarted = Watcher(config, PROFILE, path, root / "report.md", polls, delivery,
                                    fetcher=restart_fetch, now=1050)
                restarted.tick(1050)
                self.assertFalse(restarted.polls)
                restart_fetch.assert_not_called()
                self.assertEqual(restarted.due["ashby:example"], 1300)
                # A successful retry clears the failure count and returns to a 60-second cadence.
                watcher.fetcher = lambda source: [job()]
                watcher.tick(1300)
                self.wait_for(lambda: watcher.state["boards"]["ashby:example"].get("consecutive_failures") == 0,
                              lambda: watcher.tick(1300))
                self.assertEqual(watcher.due["ashby:example"], 1360)

    def test_watch_honors_persisted_delivery_pause_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            state = scan(scan(empty_state(), [job()]), [job(), job("2")])
            state["notification_retry_at"] = 1120
            state["boards"]["ashby:example"]["next_poll_at"] = 2000
            save_state(path, state)
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                sender = Mock()
                watcher = Watcher({"sources": [SOURCE]}, PROFILE, path, root / "report.md", polls, delivery,
                                  sender=sender, now=1000)
                watcher.tick(1119)
                self.assertIsNone(watcher.delivery)
                sender.assert_not_called()
                watcher.tick(1120)
                self.wait_for(lambda: not watcher.state["pending"], lambda: watcher.tick(1120))
                self.assertEqual(len(read_state(path)["notified"]), 1)

    def test_source_retry_after_supports_dates_and_rejects_invalid_delays(self):
        self.assertEqual(retry_after_seconds({"Retry-After": "Thu, 01 Jan 1970 00:20:00 GMT"}, 1000), 200)
        for value in ("nan", "inf", "-3", "invalid"):
            self.assertEqual(retry_after_seconds({"Retry-After": value}, 1000), 0)

    def test_watch_cli_holds_history_lock_and_stops_cleanly_without_network(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = scan(empty_state(), [job()])
            state["boards"]["ashby:example"]["next_poll_at"] = time.time() + 3600
            save_state(root / "state.json", state)
            (root / "sources.json").write_text(json.dumps({"sources": [SOURCE]}))
            (root / "profile.json").write_text(json.dumps(PROFILE))
            script = str((Path(__file__).resolve().parent.parent / "scripts/monitor.py"))
            process = subprocess.Popen([__import__("sys").executable, "-B", "-u", script, "--watch"],
                                       cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertIn("Watching 1 boards", process.stdout.readline())
                manual = subprocess.run([__import__("sys").executable, "-B", script], cwd=root,
                                        capture_output=True, text=True, timeout=3)
                self.assertEqual(manual.returncode, 0)
                self.assertIn("Another check is running; skipped", manual.stdout)
                process.send_signal(signal.SIGTERM)
                output, errors = process.communicate(timeout=3)
                self.assertEqual(process.returncode, 0, errors)
                self.assertIn("Watcher stopped; history retained", output)
                self.assertEqual(read_state(root / "state.json"), state)
                self.assertEqual(json.loads((root / "health.json").read_text())["worker"]["status"], "stopped")
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate()

    def test_initial_baseline_and_added_board_do_not_alert_old_jobs(self):
        """Prevent first deployment or watchlist expansion flooding the alert channel."""
        baseline = scan(empty_state(), [job()])
        self.assertFalse(baseline["pending"])
        second = scan(baseline, [job(), job("2")])
        self.assertEqual(len(second["pending"]), 1)
        added = scan(second, [job("old")], source={**SOURCE, "board": "another"})
        self.assertEqual(len(added["pending"]), 1)

    def test_delayed_first_success_records_suppression_without_losing_jobs(self):
        first_attempt = "2026-10-12T10:00:00Z"
        success_at = "2026-10-13T10:00:00Z"
        failed = apply_scan(empty_state(), [(SOURCE, [], "HTTPError HTTP 503")], PROFILE, first_attempt)
        self.assertFalse(failed["boards"]["ashby:example"]["initialized"])
        self.assertIn("Baseline pending: ashby:example", report(failed, first_attempt))
        success = scan(failed, [job(), job("2", location="Berlin")], now=success_at)
        status = success["boards"]["ashby:example"]
        self.assertEqual(status["first_attempt_at"], first_attempt)
        self.assertEqual(status["baseline_at"], success_at)
        self.assertEqual(status["baseline_job_count"], 2)
        self.assertEqual(status["baseline_matching_count"], 1)
        self.assertEqual(status["baseline_suppressed_count"], 1)
        self.assertFalse(success["pending"])
        self.assertEqual(success["jobs"]["ashby:example:1"]["baseline_suppressed_at"], success_at)
        self.assertEqual(success["jobs"]["ashby:example:1"]["job"]["date_type"], "unknown")
        self.assertIn("https://example.test/jobs/1", report(success, success_at))
        next_scan = scan(success, [job(), job("3")], now="2026-10-13T11:00:00Z")
        self.assertEqual(next_scan["pending"][0]["job"]["id"], "3")
        self.assertEqual(next_scan["boards"]["ashby:example"]["baseline_suppressed_count"], 1)
        self.assertFalse(next_scan["jobs"]["ashby:example:3"]["baseline_suppressed_at"])

    def test_baseline_report_includes_all_suppressed_matches_and_retains_audit(self):
        jobs = [job(str(number)) for number in range(60)]
        state = scan(empty_state(), jobs)
        text = report(state, "2026-10-12T10:00:00Z")
        suppression = text.split("## Matching postings initially suppressed at baseline", 1)[1]
        for value in jobs:
            self.assertIn(value["url"], suppression)
        changed = scan(state, jobs[:-1], now="2026-10-13T10:00:00Z")
        self.assertEqual(changed["boards"]["ashby:example"]["baseline_suppressed_count"], 60)
        self.assertFalse(changed["jobs"]["ashby:example:59"]["active"])
        self.assertNotIn(jobs[-1]["url"], report(changed, "2026-10-13T10:00:00Z"))

    def test_explicit_baseline_notifications_are_labelled_deduplicated_and_not_retroactive(self):
        failed = apply_scan(empty_state(), [(SOURCE, [], "TimeoutError")], PROFILE, "2026-10-12T10:00:00Z")
        notified_source = {**SOURCE, "baseline_policy": "notify"}
        notified = scan(failed, [job()], source=notified_source, now="2026-10-13T10:00:00Z")
        event = notified["pending"][0]
        self.assertEqual(event["kind"], "Present at first successful check")
        self.assertEqual(event["first_seen"], "2026-10-13T10:00:00Z")
        self.assertEqual(event["job"]["date_type"], "unknown")
        self.assertEqual(notified["boards"]["ashby:example"]["baseline_suppressed_count"], 0)
        duplicate = scan(notified, [job()], source=notified_source)
        self.assertEqual(len(duplicate["pending"]), 1)
        suppressed = scan(empty_state(), [job()])
        switched = scan(suppressed, [job()], source=notified_source)
        self.assertFalse(switched["pending"])
        self.assertEqual(switched["boards"]["ashby:example"]["baseline_policy"], "suppress")
        overridden = apply_scan(empty_state(), [({**SOURCE, "baseline_policy": "suppress"}, [job()], "")],
                                PROFILE, "2026-10-12T10:00:00Z", baseline_policy="notify")
        self.assertFalse(overridden["pending"])
        # A source with no override inherits the configured global policy.
        inherited = apply_scan(empty_state(), [(SOURCE, [job()], "")], PROFILE,
                               "2026-10-12T10:00:00Z", baseline_policy="notify")
        self.assertEqual(len(inherited["pending"]), 1)

    def test_legacy_history_is_not_rebaselined_or_given_invented_baseline_dates(self):
        legacy = scan(empty_state(), [job()])
        for name in ("first_attempt_at", "last_attempt", "baseline_at", "baseline_policy",
                     "baseline_job_count", "baseline_matching_count", "baseline_suppressed_count"):
            legacy["boards"]["ashby:example"].pop(name)
        legacy["jobs"]["ashby:example:1"].pop("baseline_suppressed_at")
        current = scan(legacy, [job()], source={**SOURCE, "baseline_policy": "notify"})
        self.assertFalse(current["pending"])
        self.assertEqual(current["jobs"]["ashby:example:1"]["first_seen"], legacy["jobs"]["ashby:example:1"]["first_seen"])
        self.assertNotIn("baseline_at", current["boards"]["ashby:example"])
        self.assertFalse(current["jobs"]["ashby:example:1"]["baseline_suppressed_at"])
        self.assertIn("original suppression details unavailable", report(current, "2026-10-13T10:00:00Z"))

    def test_invalid_baseline_policy_stops_before_fetching_or_saving(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fetcher = Mock()
            for config in ({"sources": [SOURCE], "baseline_policy": "recent"},
                           {"sources": [{**SOURCE, "baseline_policy": True}]}):
                with self.assertRaises(ValueError):
                    run(config, PROFILE, root / "state.json", root / "report.md", fetcher=fetcher)
            fetcher.assert_not_called()
            self.assertFalse(list(root.iterdir()))

    def test_edits_do_not_realert_but_existing_posting_can_become_a_new_match(self):
        """Distinguish new leads from ordinary advert updates."""
        initial = scan(empty_state(), [job(), job("2", location="Berlin")])
        changed = scan(initial, [job(description="Updated Python services. Visa sponsorship is available."), job("2")])
        self.assertEqual(len(changed["pending"]), 1)
        self.assertEqual(changed["pending"][0]["kind"], "New match on an existing posting")
        again = scan(changed, [job(), job("2")])
        self.assertEqual(len(again["pending"]), 1)
        self.assertEqual(again["jobs"]["ashby:example:2"]["first_seen"], initial["jobs"]["ashby:example:2"]["first_seen"])

    def test_failed_board_preserves_history_and_delivers_observed_leads(self):
        """A later fetch error must not withhold an already discovered job."""
        initial = scan(empty_state(), [job()])
        queued = scan(initial, [job(), job("2")])
        failed = apply_scan(queued, [(SOURCE, [], "HTTPError HTTP 429")], PROFILE, "2026-10-12T11:00:00Z")
        self.assertEqual(failed["jobs"], queued["jobs"])
        self.assertEqual(failed["pending"], queued["pending"])
        unsent = copy.deepcopy(failed)
        sender = Mock()
        deliver_pending(failed, sender, Mock())
        sender.assert_called_once()
        delivered = sender.call_args.args[0][0]
        self.assertTrue(delivered["source_check_failed"])
        self.assertIn("verify the listing is still open", discord_text(delivered))
        self.assertFalse(failed["pending"])
        self.assertEqual(len(failed["notified"]), 1)
        self.assertEqual(failed["boards"]["ashby:example"]["last_error"], "HTTPError HTTP 429")
        # A successful check that observes closure still cancels an unsent lead.
        removed = scan(unsent, [job()])
        self.assertFalse(removed["jobs"]["ashby:example:2"]["active"])
        self.assertFalse(removed["pending"])

    def test_watch_dispatches_a_durable_lead_before_processing_another_completed_board(self):
        other = {**SOURCE, "board": "large"}
        release = threading.Event()
        processing_other = threading.Event()
        sent = threading.Event()
        errors = []
        clock = [1000]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            state = apply_scan(empty_state(), [(SOURCE, [], ""), (other, [], "")], PROFILE, stamp(900))
            save_state(path, state)
            def sender(events):
                # Dispatch is permitted only after the discovered event is saved.
                self.assertEqual(read_state(path)["pending"][0]["event_id"], events[0]["event_id"])
                sent.set()
            def process(previous, results, profile, now, **kwargs):
                if results[0][0]["board"] == "large":
                    processing_other.set()
                    if not release.wait(3):
                        raise RuntimeError("processing timed out in test")
                    clock[0] = 1005
                return apply_scan(previous, results, profile, now, **kwargs)
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                watcher = Watcher({"sources": [SOURCE, other]}, PROFILE, path, root / "report.md", polls, delivery,
                                  sender=sender, now=1000)
                for source in (SOURCE, other):
                    result = Future()
                    result.set_result(([job(source["board"])], "", 0, 0.1))
                    watcher.polls[watcher.identity(source)] = (result, source, 1000)
                def tick():
                    try:
                        watcher.tick()
                    except Exception as exc:
                        errors.append(exc)
                worker = threading.Thread(target=tick)
                with patch("job_monitor.watch.apply_scan", side_effect=process), \
                        patch("job_monitor.watch.time.time", side_effect=lambda: clock[0]):
                    worker.start()
                    try:
                        self.assertTrue(processing_other.wait(1))
                        self.assertTrue(sent.wait(1), "Ready lead waited for another board's processing")
                        watcher.delivery[0].result(timeout=1)
                    finally:
                        release.set()
                        worker.join(2)
                self.assertFalse(worker.is_alive())
                self.assertFalse(errors)
                self.assertEqual(len(read_state(path)["notified"]), 1)
                self.assertEqual(read_state(path)["last_delivery"]["confirmed_at"], stamp(1005))
                self.assertEqual([event["job"]["id"] for event in read_state(path)["pending"]], ["large"])

    def test_short_poll_interval_keeps_staggering_and_honors_source_retry_after(self):
        other = {**SOURCE, "board": "healthy"}
        def fetcher(source):
            if source["board"] == "example":
                raise urllib.error.HTTPError("https://example.test", 429, "limited", {"Retry-After": "300"}, None)
            return []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            state = apply_scan(empty_state(), [(SOURCE, [], ""), (other, [], "")], PROFILE, stamp(900))
            save_state(path, state)
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery, \
                    redirect_stdout(io.StringIO()):
                config = {"sources": [SOURCE, other]}
                for invalid in (0, 14, float("nan"), float("inf")):
                    with self.assertRaises(ValueError):
                        Watcher(config, PROFILE, path, root / "report.md", polls, delivery, interval=invalid)
                watcher = Watcher(config, PROFILE, path, root / "report.md", polls, delivery,
                                  fetcher=fetcher, interval=15, now=1000)
                self.assertEqual(watcher.due["ashby:healthy"], 1007.5)
                watcher.tick(1000)
                self.wait_for(lambda: watcher.state["boards"]["ashby:example"].get("last_error"),
                              lambda: watcher.tick(1000))
                self.assertEqual(watcher.due["ashby:example"], 1300)
                watcher.tick(1007.5)
                self.wait_for(lambda: watcher.state["boards"]["ashby:healthy"]["last_success"] == stamp(1007.5),
                              lambda: watcher.tick(1007.5))
                self.assertEqual(watcher.due["ashby:healthy"], 1022.5)

    def test_failed_atomic_save_preserves_existing_history_and_cleans_temporary_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.json"
            previous = scan(empty_state(), [job()])
            save_state(path, previous)
            original = path.read_bytes()
            current = scan(previous, [job(), job("2")])
            with patch("job_monitor.monitor.os.replace", side_effect=OSError("storage unavailable")):
                with self.assertRaises(OSError):
                    save_state(path, current)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(read_state(path), previous)
            self.assertEqual(list(root.iterdir()), [path])
            with self.assertRaises(TypeError):
                save_state(path, {"bad": object()})
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(list(root.iterdir()), [path])

    def test_delivery_failure_persists_for_retry_and_acknowledgement_stops_repeats(self):
        """Prevent failed notifications being silently lost or acknowledged twice."""
        queued = scan(scan(empty_state(), [job()]), [job(), job("2")])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            save_state(path, queued)
            with self.assertRaises(ValueError):
                deliver_pending(queued, Mock(side_effect=ValueError("delivery failed")), lambda state: save_state(path, state))
            self.assertEqual(len(read_state(path)["pending"]), 1)
            sender = Mock()
            deliver_pending(queued, sender, lambda state: save_state(path, state))
            sender.assert_not_called()
            with patch("job_monitor.monitor.time.time", return_value=queued["pending"][0]["next_delivery_at"] + 1):
                deliver_pending(queued, sender, lambda state: save_state(path, state))
            restored = read_state(path)
            self.assertFalse(restored["pending"])
            self.assertEqual(len(restored["notified"]), 1)
            deliver_pending(restored, sender, Mock())
            sender.assert_called_once()

    def test_bad_alert_does_not_block_later_leads_and_rate_limit_pauses_channel(self):
        queued = scan(scan(empty_state(), [job()]), [job(), job("2"), job("3")])
        sender = Mock(side_effect=[ValueError("bad lead"), None])
        with self.assertRaises(ValueError):
            deliver_pending(queued, sender, Mock())
        self.assertEqual(sender.call_count, 2)
        self.assertEqual([event["job"]["id"] for event in queued["pending"]], ["2"])
        restored = scan(scan(empty_state(), [job()]), [job(), job("2"), job("3")])
        sender = Mock(side_effect=NotificationError("rate limited", retry_after=120))
        with self.assertRaises(ValueError):
            deliver_pending(restored, sender, Mock())
        deliver_pending(restored, sender, Mock())
        sender.assert_called_once()
        self.assertEqual(len(restored["pending"]), 2)

    def test_healthy_board_alert_is_saved_and_sent_before_slow_board_finishes(self):
        slow_source = {**SOURCE, "board": "slow"}
        release = threading.Event()
        def fetcher(source):
            if source["board"] == "slow":
                if not release.wait(2):
                    raise RuntimeError("healthy alert did not arrive first")
                return []
            return [job(), job("2")]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "state.json"
            save_state(history, scan(empty_state(), [job()]))
            observed = []
            def sender(events):
                observed.append((events[0]["job"]["id"], len(read_state(history)["pending"])))
                release.set()
            try:
                with redirect_stdout(io.StringIO()):
                    code = run({"sources": [slow_source, SOURCE]}, PROFILE, history, root / "report.md",
                               notify=True, fetcher=fetcher, sender=sender)
            finally:
                release.set()
            self.assertEqual(code, 0)
            self.assertEqual(observed, [("2", 1)])
            self.assertFalse(read_state(history)["pending"])

    def test_discord_bounds_content_preserves_link_and_requires_confirmation(self):
        event = scan(scan(empty_state(), []), [job(title="Software Engineer " + "😀" * 3000,
            url="https://example.test/jobs/1?return=(apply)&team=R%26D")])["pending"][0]
        response = Mock()
        response.__enter__ = Mock(return_value=io.StringIO('{"id": "message-1"}'))
        response.__exit__ = Mock(return_value=False)
        with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/123/test-secret"}), \
                patch("job_monitor.notifications.urllib.request.urlopen", return_value=response) as request:
            discord([event])
            payload = json.loads(request.call_args.args[0].data)
            self.assertLessEqual(len(payload["content"].encode("utf-16-le")) // 2, 2000)
            # The masked action must retain the whole official destination,
            # including parentheses and existing percent-encoded query values.
            self.assertIn("[View job & apply](<" + event["job"]["url"] + ">)", payload["content"])
            self.assertEqual(payload["allowed_mentions"], {"parse": []})
            self.assertTrue(request.call_args.args[0].full_url.endswith("?wait=true"))
            response.__enter__.return_value = io.StringIO('{}')
            with self.assertRaises(NotificationError):
                discord([event])

    def test_discord_rate_limit_is_retryable_and_errors_do_not_expose_webhook(self):
        event = scan(scan(empty_state(), []), [job()])["pending"][0]
        webhook = "https://discord.com/api/webhooks/123/test-secret"
        error = urllib.error.HTTPError(webhook, 429, "test-secret", {}, io.BytesIO(b'{"retry_after": 42}'))
        with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": webhook}), \
                patch("job_monitor.notifications.urllib.request.urlopen", side_effect=error):
            with self.assertRaises(NotificationError) as caught:
                discord([event])
            self.assertEqual(caught.exception.retry_after, 42)
            self.assertNotIn("test-secret", str(caught.exception))
            self.assertNotIn(webhook, str(caught.exception))

    def test_alert_freshness_keeps_first_publication_separate_from_detection(self):
        state = scan(scan(empty_state(), [], now=stamp(800)), [job(published_at=stamp(880), date_type="first published")], now=stamp(990))
        event = state["pending"][0]
        text = discord_text(event, now=1000)
        self.assertIn("First published <t:880:R>", text)
        self.assertIn("Detected <t:990:R>", text)
        self.assertNotIn("First published <t:990:R>", text)
        self.assertEqual(event["previous_check_at"], stamp(800))

    def test_alert_does_not_turn_republication_creation_or_invalid_dates_into_posted_times(self):
        state = scan(scan(empty_state(), [], now=stamp(950)), [job()], now=stamp(990))
        event = state["pending"][0]
        last_published = {**event, "job": {**event["job"], "published_at": stamp(880), "date_type": "last published"}}
        text = discord_text(last_published, now=1000)
        self.assertIn("Last published <t:880:R>", text)
        self.assertIn("first publication unknown", text)
        self.assertNotIn("First published <t:", text)
        for published, date_type in ((stamp(880), "record created"), (stamp(880), "unknown"),
                                     ("invalid", "first published"), (stamp(2000), "first published"),
                                     ("1970-01-01T00:14:40", "first published"), ("", "unknown")):
            with self.subTest(published=published, date_type=date_type):
                candidate = {**event, "job": {**event["job"], "published_at": published, "date_type": date_type}}
                text = discord_text(candidate, now=1000)
                self.assertIn("Posting time unavailable", text)
                self.assertNotIn("First published <t:", text)
                self.assertIn("detected <t:990:R>", text)

    def test_matching_older_roles_and_monitoring_gaps_do_not_imply_original_freshness(self):
        state = scan(scan(empty_state(), [job("1", location="Berlin")], now=stamp(600)),
                     [job("1", published_at=stamp(100), date_type="first published"), job("2")], now=stamp(990))
        matched, newly_found = state["pending"]
        text = discord_text(matched, now=1000)
        self.assertIn("Existing posting · newly matched", text)
        self.assertIn("First published <t:100:R>", text)
        self.assertIn("Matched <t:990:R>", text)
        self.assertNotIn("New listing detected", text)
        text = discord_text(newly_found, now=1000)
        self.assertIn("Detected after a monitoring gap", text)
        self.assertIn("Posting time unavailable", text)
        self.assertFalse(newly_found["job"]["published_at"])

    def test_new_listings_take_delivery_priority_over_catchup_without_losing_fifo(self):
        previous = scan(empty_state(), [job("old", location="Berlin")], now=stamp(900))
        state = scan(previous, [job("old"), job("1"), job("2")], now=stamp(990))
        self.assertEqual(state["pending"][0]["kind"], "New match on an existing posting")
        original_order = [event["event_id"] for event in state["pending"]]
        first = ready_delivery(state, 1000)
        self.assertEqual(first["job"]["id"], "1")
        self.assertEqual([event["event_id"] for event in state["pending"]], original_order)
        sender = Mock()
        with patch("job_monitor.monitor.time.time", return_value=1000):
            deliver_pending(state, sender, Mock())
        self.assertEqual([call.args[0][0]["job"]["id"] for call in sender.call_args_list], ["1", "2", "old"])
        self.assertEqual(len(state["notified"]), 3)
        self.assertEqual(len(state["jobs"]), 3)

    def test_dry_run_has_no_files_or_notification_side_effects(self):
        """Prevent a preview from establishing history or sending real messages."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("job_monitor.monitor.telegram") as sender, redirect_stdout(io.StringIO()):
                code = run({"sources": [SOURCE]}, PROFILE, root / "history.json", root / "report.md",
                           notify=True, dry_run=True, fetcher=lambda source: [job()])
            self.assertEqual(code, 0)
            self.assertEqual(list(root.iterdir()), [])
            sender.assert_not_called()

    def test_corrupt_state_is_not_silently_reset(self):
        """Prevent invalid persisted history becoming a new baseline."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            path.write_text('{"version": 0}')
            original = path.read_bytes()
            with self.assertRaises(ValueError):
                read_state(path)
            self.assertEqual(path.read_bytes(), original)

    def test_dates_are_labelled_and_unlisted_jobs_are_not_monitored(self):
        """Prevent update timestamps being presented as new publication dates."""
        gh = normalise({**SOURCE, "provider": "greenhouse"}, {"jobs": [{"id": 1, "title": "Software Engineer",
            "absolute_url": "https://example.test/1", "location": {"name": "London"}, "updated_at": "2026-10-12", "content": "Python"}]})
        self.assertEqual((gh[0]["published_at"], gh[0]["date_type"]), ("", "unknown"))
        ashby = normalise(SOURCE, {"jobs": [{"id": "1", "title": "Software Engineer", "jobUrl": "https://example.test/1",
            "location": "London", "publishedAt": "2026-10-12"}, {"isListed": False}]})
        self.assertEqual(len(ashby), 1)
        self.assertEqual(ashby[0]["date_type"], "last published")

    def test_tracking_parameters_deduplicate_but_query_job_ids_remain_distinct(self):
        """Prevent duplicate alerts or merging different query-based vacancy links."""
        first = "https://example.test/careers?gh_jid=1"
        second = "https://example.test/careers?gh_jid=2"
        self.assertEqual(canonical_url(first + "&utm_source=board"), canonical_url(first))
        self.assertNotEqual(canonical_url(first), canonical_url(second))
        initial = scan(empty_state(), [job(url=first)])
        result = scan(initial, [job("new-id", url=first + "&utm_source=board")])
        self.assertFalse(result["pending"])

    def test_screening_rejects_explicit_no_sponsorship_and_accepts_title_variants(self):
        """Prevent known unsuitable alerts and missed hyphenated backend titles."""
        self.assertIsNone(screen(job(description="We cannot sponsor visas."), PROFILE))
        self.assertIsNone(screen(job(location="Berlin"), PROFILE))
        match = screen(job(title="Back-end Engineer", description="4+ years building systems. Sponsorship isn't guaranteed."), PROFILE)
        self.assertIsNotNone(match)
        self.assertIn("conditional", match["sponsorship"])
        self.assertTrue(any("4+ years" in value for value in match["concerns"]))

    def test_audit_captures_all_blockers_and_keeps_borderline_acceptance(self):
        blocked = job(title="Principal Founding Engineer", location="Berlin", description="We cannot sponsor visas.")
        match, reasons = screen_with_reasons(blocked, EXPANDED_PROFILE)
        self.assertIsNone(match)
        self.assertEqual(set(reasons), {"title", "location", "excluded_title", "sponsorship"})
        for posting in (job(title="Senior Software Engineer"), job(location="Europe Remote"),
                        job(description="Sponsorship isn't guaranteed."), job(title="Member of Technical Staff")):
            with self.subTest(posting=posting):
                match, reasons = screen_with_reasons(posting, EXPANDED_PROFILE)
                self.assertIsNotNone(match)
                self.assertEqual(reasons, [])
        self.assertEqual(screen_with_reasons(job(title="Staff Software Engineer"), EXPANDED_PROFILE)[1], ["staff_level"])

    def test_recorded_audit_preserves_history_through_failure_and_marks_policy_changes(self):
        jobs = [job(), job("2", title="Founding Engineer"), job("3", title="Founding Engineer", location="Berlin")]
        state = scan(empty_state(), jobs)
        self.assertFalse(state["pending"])
        self.assertEqual(state["jobs"]["ashby:example:2"]["rejection_reasons"], ["title"])
        self.assertNotIn("description", state["jobs"]["ashby:example:2"]["job"])
        failed = apply_scan(state, [(SOURCE, [], "TimeoutError")], PROFILE, stamp(1000))
        self.assertEqual(failed["jobs"], state["jobs"])
        altered = {**PROFILE, "title_keywords": PROFILE["title_keywords"] + ["founding engineer"]}
        audit = rejection_audit(failed, {"sources": [SOURCE]}, altered, title="Founding", limit=1)
        self.assertIn("Rejected: 2", audit)
        self.assertIn("Decisions using a different profile: 2", audit)
        self.assertIn("Latest board check failed", audit)
        self.assertIn("Showing 1/2", audit)
        updated = apply_scan(failed, [(SOURCE, jobs, "")], altered, stamp(1010))
        self.assertEqual([event["job"]["id"] for event in updated["pending"]], ["2"])
        self.assertEqual(updated["pending"][0]["kind"], "New match on an existing posting")
        self.assertEqual(updated["jobs"]["ashby:example:2"]["first_seen"], state["jobs"]["ashby:example:2"]["first_seen"])
        self.assertEqual(updated["jobs"]["ashby:example:3"]["rejection_reasons"], ["location"])
        legacy = copy.deepcopy(state)
        for value in legacy["jobs"].values():
            value.pop("rejection_reasons")
            value.pop("screening_profile_sha256")
        self.assertIn("Missing historical reasons: 2", rejection_audit(legacy, {"sources": [SOURCE]}, PROFILE))
        self.assertIn("Historical reasons unavailable", rejection_audit(legacy, {"sources": [SOURCE]}, PROFILE))
        other = {**SOURCE, "board": "disabled", "enabled": False}
        legacy["jobs"]["ashby:disabled:x"] = {**legacy["jobs"]["ashby:example:2"], "board": "ashby:disabled"}
        self.assertIn("Audit: 3", rejection_audit(legacy, {"sources": [SOURCE, other]}, PROFILE))

    def test_audit_cli_is_read_only_even_with_notification_and_watch_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_state(root / "state.json", scan(empty_state(), [job(title="Founding Engineer")]))
            (root / "sources.json").write_text(json.dumps({"sources": [SOURCE]}))
            (root / "profile.json").write_text(json.dumps(PROFILE))
            before = {path.name: path.read_bytes() for path in root.iterdir()}
            script = str((Path(__file__).resolve().parent.parent / "scripts/monitor.py"))
            result = subprocess.run([__import__("sys").executable, "-B", script, "--audit", "--notify", "--watch",
                                     "--audit-title", "founding"], cwd=root, capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Title does not match an allowed phrase", result.stdout)
            self.assertIn("https://example.test/jobs/1", result.stdout)
            self.assertEqual(before, {path.name: path.read_bytes() for path in root.iterdir()})

    def test_screening_handles_title_spelling_without_matching_unrelated_roles(self):
        for title in ("Backend Engineer", "Back-end Engineer", "Fullstack Engineer", "Full-stack Engineer",
                      "Frontend Engineer", "Front-end Engineer", "Software Developer", "Platform Engineer",
                      "DevOps Engineer", "Site Reliability Engineer", "Member of Technical Staff"):
            with self.subTest(title=title):
                self.assertIsNotNone(screen(job(title=title), EXPANDED_PROFILE))
        for title in ("Software Engineering Manager", "Software Engineering Recruiter", "AI Engineeringsales",
                      "Principal Software Engineer", "Staff Software Engineer", "Director of Platform Engineering"):
            with self.subTest(title=title):
                self.assertIsNone(screen(job(title=title), EXPANDED_PROFILE))
        match = screen(job(title="Senior Backend Engineer", description="5+ years building distributed systems."), EXPANDED_PROFILE)
        self.assertTrue(any("5+ years" in concern for concern in match["concerns"]))

    def test_ambiguous_remote_roles_warn_and_can_be_disabled(self):
        for location in ("Remote — Europe", "EMEA | Remote", "Worldwide / Remote", "Global remote",
                         "Remote anywhere", "Remote", "Fully remote", "Remote | Remote"):
            with self.subTest(location=location):
                match = screen(job(location=location), EXPANDED_PROFILE)
                self.assertEqual(match["location_eligibility"], "Remote — UK eligibility unconfirmed")
                self.assertTrue(any("Confirm UK residence" in value for value in match["concerns"]))
                self.assertIsNone(screen(job(location=location), {**EXPANDED_PROFILE, "allow_ambiguous_remote": False}))
        for location in ("UK — Remote", "United Kingdom | Remote", "Scotland remote"):
            self.assertIsNotNone(screen(job(location=location), EXPANDED_PROFILE))
        self.assertIsNone(screen(job(location="UK Remote"), {**EXPANDED_PROFILE, "allow_uk_remote": False}))
        self.assertIsNone(screen(job(location="Europe Remote"), {**EXPANDED_PROFILE, "allow_uk_remote": False}))
        self.assertIsNone(screen(job(location="Europe Remote"), {**EXPANDED_PROFILE, "ambiguous_remote_regions": ["Worldwide"]}))

    def test_broader_screening_preserves_clear_location_and_sponsorship_exclusions(self):
        for location in ("Berlin", "Berlin remote", "United States remote", "EU-only Remote",
                         "US-only Worldwide Remote", "Only Canada | Global remote", "Europe (on-site)",
                         "London, Ontario, Canada", "London, United States"):
            with self.subTest(location=location):
                self.assertIsNone(screen(job(location=location), EXPANDED_PROFILE))
        self.assertIsNone(screen(job(location="Worldwide Remote", description="We cannot sponsor visas."), EXPANDED_PROFILE))
        self.assertIsNotNone(screen(job(location="London | New York"), EXPANDED_PROFILE))

    def test_provider_remote_country_and_secondary_locations_reach_screening(self):
        base = {"id": "1", "text": "Software Engineer", "hostedUrl": "https://example.test/1",
                "descriptionPlain": "Build services", "categories": {"location": "Manchester"},
                "country": "GB", "workplaceType": "remote"}
        lever = {**SOURCE, "provider": "lever"}
        remote = normalise(lever, [base])[0]
        self.assertIn("UK remote", screen(remote, EXPANDED_PROFILE)["location_eligibility"])
        secondary = normalise(lever, [{**base, "country": "US", "workplaceType": "on-site",
            "categories": {"location": "New York", "allLocations": ["New York", "London"]}}])[0]
        self.assertEqual(screen(secondary, EXPANDED_PROFILE)["location_eligibility"], "London")
        canada = normalise(lever, [{**base, "country": "CA", "categories": {"location": "London", "allLocations": ["London"]}}])[0]
        self.assertIsNone(screen(canada, EXPANDED_PROFILE))
        ashby = {"id": "1", "title": "Software Engineer", "jobUrl": "https://example.test/1",
                 "location": "Europe", "workplaceType": "Remote", "descriptionPlain": "Build services"}
        remote = normalise(SOURCE, {"jobs": [ashby]})[0]
        self.assertIn("unconfirmed", screen(remote, EXPANDED_PROFILE)["location_eligibility"])
        us = normalise(SOURCE, {"jobs": [{**ashby, "location": "Remote",
            "address": {"postalAddress": {"addressCountry": "USA"}}}]})[0]
        self.assertIsNone(screen(us, EXPANDED_PROFILE))

    def test_profile_expansion_alerts_existing_new_matches_once_without_resetting_history(self):
        jobs = [job(), job("2", title="Software Developer"), job("3", location="Europe Remote")]
        previous = scan(empty_state(), jobs)
        expanded = apply_scan(previous, [(SOURCE, jobs, "")], EXPANDED_PROFILE, "2026-10-13T10:00:00Z")
        self.assertEqual(len(expanded["pending"]), 2)
        self.assertTrue(all(event["kind"] == "New match on an existing posting" for event in expanded["pending"]))
        self.assertEqual(expanded["jobs"]["ashby:example:2"]["first_seen"], previous["jobs"]["ashby:example:2"]["first_seen"])
        again = apply_scan(expanded, [(SOURCE, jobs, "")], EXPANDED_PROFILE, "2026-10-13T11:00:00Z")
        self.assertEqual(again["pending"], expanded["pending"])
        baseline = apply_scan(empty_state(), [(SOURCE, jobs, "")], EXPANDED_PROFILE, "2026-10-13T10:00:00Z")
        self.assertFalse(baseline["pending"])

    def test_bad_screening_configuration_is_rejected_before_a_scan(self):
        for changes in ({"allow_ambiguous_remote": "false"}, {"ambiguous_remote_regions": "Europe"},
                        {"title_keywords": [""]}, {"excluded_title_keywords": [None]}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_config({"sources": [SOURCE]}, {**EXPANDED_PROFILE, **changes})


class MenuContracts(unittest.TestCase):
    class Terminal(io.StringIO):
        def isatty(self):
            return True

    def test_menu_entry_rejects_conflicting_operations_before_reading_or_writing_data(self):
        script = str((Path(__file__).resolve().parent.parent / "scripts/monitor.py"))
        for flag in ("--health", "--audit", "--watch", "--dry-run", "--notify"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result = subprocess.run([__import__("sys").executable, "-B", script, "--menu", flag],
                                        cwd=root, input="0\n", capture_output=True, text=True, timeout=3)
                self.assertEqual(result.returncode, 2)
                self.assertIn("--menu cannot be combined", result.stderr)
                self.assertEqual(list(root.iterdir()), [])

    def test_menu_rejects_piped_input_without_creating_monitoring_files(self):
        script = str((Path(__file__).resolve().parent.parent / "scripts/monitor.py"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = subprocess.run([__import__("sys").executable, "-B", script, "--menu"],
                                    cwd=root, input="0\n", capture_output=True, text=True, timeout=3)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(root.iterdir()), [])

    def test_menu_exit_eof_and_interrupt_remain_available_without_data(self):
        from job_monitor.cli_menu import run_menu
        class InterruptedTerminal(self.Terminal):
            def readline(self, *args):
                raise KeyboardInterrupt
        for terminal in (self.Terminal("0\n"), self.Terminal(""), InterruptedTerminal()):
            with self.subTest(terminal=type(terminal).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = io.StringIO()
                with patch("job_monitor.monitor.run", side_effect=AssertionError("A read-only menu must not scan")), \
                        patch("job_monitor.monitor.fetch", side_effect=AssertionError("A read-only menu must not fetch")), \
                        patch("job_monitor.monitor.sender_for", side_effect=AssertionError("A read-only menu must not send")):
                    result = run_menu(config_path=root / "sources.json", profile_path=root / "profile.json",
                                      state_path=root / "state.json", input_stream=terminal, output_stream=output)
                self.assertEqual(result, 0)
                self.assertEqual(list(root.iterdir()), [])

    def test_saved_policy_drives_menu_browsing_and_navigation_does_not_change_data(self):
        from job_monitor.cli_menu import MenuData, run_menu
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"JOB_MONITOR_REVIEW_USER_ID": ""}):
            root = Path(directory)
            store, jobs, state = ReviewContracts().setup_store(directory)
            now = time.time()
            store.project(SOURCE, jobs, "", now, {key: value["first_seen"] for key, value in state["jobs"].items()})
            token, _, _ = store.propose("123", {"title_keywords": ["founding engineer"]})
            store.confirm("123", token)
            store.worker_status("123", 2, now)
            save_state(root / "state.json", state)
            save_state(root / "health.json", build_health(state, {"sources": [SOURCE]}, now,
                       worker={"status": "running", "heartbeat_at": stamp(now)}))
            (root / "sources.json").write_text(json.dumps({"sources": [SOURCE]}))
            (root / "profile.json").write_text(json.dumps(PROFILE))
            data = MenuData(root / "sources.json", root / "profile.json", root / "state.json")
            before = {name: (root / name).read_bytes() for name in
                      ("state.json", "health.json", "sources.json", "profile.json", "review.sqlite3")}
            records, _ = data.postings()
            self.assertEqual([record["job"]["id"] for record in records], ["2"])
            self.assertEqual(data.settings()[0]["title_keywords"], ["founding engineer"])
            output = io.StringIO()
            navigation = self.Terminal("1\nb\n2\n1\n\n2\n\nb\n3\n1\nb\n2\n\nb\n0\n")
            with patch("job_monitor.monitor.run", side_effect=AssertionError("Menu attempted a scan")), \
                    patch("job_monitor.monitor.fetch", side_effect=AssertionError("Menu attempted a fetch")), \
                    patch("job_monitor.monitor.sender_for", side_effect=AssertionError("Menu attempted delivery")):
                result = run_menu(config_path=data.config_path, profile_path=data.profile_path,
                                  state_path=data.state_path, input_stream=navigation, output_stream=output)
            self.assertEqual(result, 0)
            self.assertIn("https://example.test/jobs/2", output.getvalue())
            self.assertIn("ashby:example", output.getvalue())
            self.assertEqual(before, {name: (root / name).read_bytes() for name in before})
            self.assertEqual(store.policy("123")[0], 2)
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM feedback").fetchone()[0], 0)

    def test_corrupt_history_does_not_block_other_menu_views_or_reset_history(self):
        from job_monitor.cli_menu import run_menu
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sources.json").write_text(json.dumps({"sources": [SOURCE]}))
            (root / "state.json").write_text("{broken posting history")
            before = {path.name: path.read_bytes() for path in root.iterdir()}
            output = io.StringIO()
            result = run_menu(config_path=root / "sources.json", profile_path=root / "profile.json",
                              state_path=root / "state.json", input_stream=self.Terminal("2\n1\n\n2\n\nb\n0\n"),
                              output_stream=output)
            self.assertEqual(result, 0)
            self.assertIn("ashby:example", output.getvalue())
            self.assertEqual(before, {path.name: path.read_bytes() for path in root.iterdir()})

    def test_menu_publication_order_and_age_filter_never_use_discovery_or_creation_time(self):
        from job_monitor.cli_menu import MenuData
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"JOB_MONITOR_REVIEW_USER_ID": ""}):
            root = Path(directory)
            now = time.time()
            jobs = [job("unknown"), job("created", date_type="record created", published_at=stamp(now - 30)),
                    job("future", date_type="first published", published_at=stamp(now + 86400)),
                    job("recent", date_type="first published", published_at=stamp(now - 86400)),
                    job("older", date_type="first published", published_at=stamp(now - 3 * 86400)),
                    job("oldest", date_type="first published", published_at=stamp(now - 8 * 86400))]
            state = scan(empty_state(), jobs, now=stamp(now))
            save_state(root / "state.json", state)
            (root / "sources.json").write_text(json.dumps({"sources": [SOURCE]}))
            (root / "profile.json").write_text(json.dumps(PROFILE))
            store = ReviewStore(root / "review.sqlite3")
            store.initialise("123", PROFILE)
            store.project(SOURCE, jobs, "", now, {key: value["first_seen"] for key, value in state["jobs"].items()})
            data = MenuData(root / "sources.json", root / "profile.json", root / "state.json")
            records, _ = data.postings()
            self.assertEqual([record["job"]["id"] for record in records[:3]], ["recent", "older", "oldest"])
            self.assertEqual({record["job"]["id"] for record in records[3:]}, {"unknown", "created", "future"})
            records, _ = data.postings(days=7)
            self.assertEqual([record["job"]["id"] for record in records], ["recent", "older"])
            # The historical fallback must preserve the same publication semantics.
            with patch.object(data, "review", return_value=(None, None)):
                records, _ = data.postings(days=7)
            self.assertEqual([record["job"]["id"] for record in records], ["recent", "older"])

    def test_menu_owner_selection_does_not_guess_among_users_or_fall_back_on_wrong_owner(self):
        from job_monitor.cli_menu import MenuData
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, _, _ = ReviewContracts().setup_store(directory)
            (root / "profile.json").write_text(json.dumps(PROFILE))
            data = MenuData(root / "sources.json", root / "profile.json", root / "state.json")
            with patch.dict(os.environ, {"JOB_MONITOR_REVIEW_USER_ID": ""}):
                self.assertEqual(data.review()[1], "123")
            alternate = {**PROFILE, "title_keywords": ["founding engineer"]}
            store.initialise("456", alternate)
            with patch.dict(os.environ, {"JOB_MONITOR_REVIEW_USER_ID": ""}), self.assertRaises(ValueError):
                data.review()
            with patch.dict(os.environ, {"JOB_MONITOR_REVIEW_USER_ID": "456"}):
                self.assertEqual(data.settings()[0], alternate)
            with patch.dict(os.environ, {"JOB_MONITOR_REVIEW_USER_ID": "999"}), self.assertRaises(ValueError):
                data.settings()

    def test_read_only_repository_blocks_mutations_and_missing_database_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, _, _ = ReviewContracts().setup_store(directory)
            readonly = ReviewStore(root / "review.sqlite3", read_only=True)
            before = (root / "review.sqlite3").read_bytes()
            self.assertEqual(readonly.policy("123"), (1, PROFILE))
            with self.assertRaises(ValueError):
                readonly.initialise("123", PROFILE)
            with self.assertRaises(ValueError):
                readonly.propose("123", {"locations": ["Manchester"]})
            with readonly.connect() as db, self.assertRaises(sqlite3.OperationalError):
                db.execute("DELETE FROM jobs")
            self.assertEqual((root / "review.sqlite3").read_bytes(), before)
            self.assertEqual(store.policy("123"), (1, PROFILE))
            missing = root / "never-created.sqlite3"
            with self.assertRaises(sqlite3.OperationalError), ReviewStore(missing, read_only=True).connect():
                self.fail("Missing read-only storage should not open")
            self.assertFalse(missing.exists())


class LearningContracts(unittest.TestCase):
    def sample_snapshot(self, user='123'):
        posting = job()
        return {'user': str(user), 'target': TARGET, 'job_key': 'ashby:example:1',
                'job_hash': signature(posting), 'company': 'Example', 'job': posting,
                'candidate': {'status': 'not_configured', 'hash': None, 'revision': None, 'profile': None},
                'screening': {'version': 1, 'hash': signature(PROFILE), 'profile': PROFILE},
                'selection': {'view': 'discord_jobs'}, 'prediction_shown': None}

    def fixture(self, directory, *, with_candidate=False):
        root = Path(directory)
        review, jobs, state = ReviewContracts().setup_store(directory)
        candidate = root / 'private-candidate.json' if with_candidate else None
        if candidate:
            candidate.write_text(json.dumps(PrioritizationContracts().candidate()))
        service = LearningService(review, LearningStore(root / 'learning.sqlite3'), candidate_path=candidate)
        record = review.job('ashby:example:1')
        snapshot = service.capture('123', record, 1, PROFILE, selection={'view': 'discord_jobs'})
        return service, snapshot, jobs

    def test_labeling_is_lazy_private_and_preserves_source_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service, snapshot, _ = self.fixture(directory, with_candidate=True)
            before = {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}
            self.assertIsNone(service.labels.latest('123', snapshot))
            self.assertFalse(service.labels.path.exists())
            head = service.decide('123', snapshot, 'yes', 'discord:1')
            self.assertEqual(head['choice'], 'yes')
            self.assertEqual(service.labels.path.stat().st_mode & 0o777, 0o600)
            for name, contents in before.items():
                self.assertEqual((root / name).read_bytes(), contents)
            with sqlite3.connect(service.labels.path) as db:
                saved = json.loads(db.execute('SELECT payload FROM snapshots').fetchone()[0])
            self.assertEqual(saved['job'], snapshot['job'])
            self.assertEqual(saved['candidate']['hash'], signature(saved['candidate']['profile']))
            self.assertIsNone(saved['prediction_shown'])

    def test_retries_corrections_undo_and_withdrawal_preserve_history(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = LearningStore(Path(directory) / 'learning.sqlite3')
            snap = self.sample_snapshot()
            yes = labels.record('123', snap, 'yes', 'request:1')
            self.assertEqual(labels.record('123', snap, 'yes', 'request:1'), yes)
            self.assertEqual(labels.record('123', snap, 'yes', 'request:noop', expected_id=yes['id']), yes)
            no = labels.record('123', snap, 'no', 'request:2', expected_id=yes['id'], reason='experience')
            # A retried old Yes must not reverse the later correction.
            self.assertEqual(labels.record('123', snap, 'yes', 'request:1')['id'], no['id'])
            undone = labels.undo('123', snap, 'request:3', expected_id=no['id'])
            self.assertEqual(undone['choice'], 'yes')
            self.assertIsNone(undone['reason'])
            self.assertEqual(labels.undo('123', snap, 'request:3', expected_id=no['id'])['id'], undone['id'])
            cleared = labels.record('123', snap, None, 'request:4', expected_id=undone['id'])
            self.assertIsNone(labels.latest('123', snap)['choice'])
            restored = labels.undo('123', snap, 'request:5', expected_id=cleared['id'])
            self.assertEqual(restored['choice'], 'yes')
            with sqlite3.connect(labels.path) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM decisions').fetchone()[0], 5)
                self.assertEqual(db.execute('SELECT COUNT(DISTINCT context) FROM decisions').fetchone()[0], 1)

    def test_ownership_stale_views_and_request_reuse_are_guarded(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = LearningStore(Path(directory) / 'learning.sqlite3')
            snap = self.sample_snapshot()
            with self.assertRaises(ValueError):
                labels.record('456', snap, 'no', 'request:1')
            self.assertFalse(labels.path.exists())
            first = labels.record('123', snap, 'yes', 'request:1')
            with self.assertRaises(ReviewConflict):
                labels.record('123', snap, 'no', 'request:2')
            with self.assertRaises(ReviewConflict):
                labels.record('123', snap, 'no', 'request:1', expected_id=first['id'])
            with self.assertRaises(ValueError):
                labels.latest('456', snap)
            other = self.sample_snapshot('456')
            self.assertIsNone(labels.latest('456', other))
            labels.record('456', other, 'no', 'request:1')
            self.assertEqual(labels.latest('123', snap)['choice'], 'yes')

    def test_skip_and_failed_writes_cannot_become_no(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = LearningStore(Path(directory) / 'learning.sqlite3')
            snap = self.sample_snapshot()
            with self.assertRaises(ValueError):
                labels.record('123', snap, 'skip', 'request:1')
            self.assertFalse(labels.path.exists())
            yes = labels.record('123', snap, 'yes', 'request:2')
            with sqlite3.connect(labels.path, timeout=0.1) as locked:
                locked.execute('BEGIN IMMEDIATE')
                with self.assertRaises(sqlite3.OperationalError):
                    labels.record('123', snap, 'no', 'request:3', expected_id=yes['id'])
                locked.rollback()
            self.assertEqual(labels.latest('123', snap)['choice'], 'yes')

    def test_interrupted_first_write_can_be_retried_without_resetting_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = LearningStore(Path(directory) / 'learning.sqlite3')
            snap = self.sample_snapshot()
            with patch.object(labels, '_append', side_effect=sqlite3.OperationalError('interrupted')), self.assertRaises(sqlite3.OperationalError):
                labels.record('123', snap, 'yes', 'request:1')
            self.assertTrue(labels.path.exists())
            self.assertIsNone(labels.latest('123', snap))
            saved = labels.record('123', snap, 'yes', 'request:1')
            self.assertEqual(saved['choice'], 'yes')
            with sqlite3.connect(labels.path) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM decisions').fetchone()[0], 1)

    def test_competing_corrections_accept_only_one_current_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = LearningStore(Path(directory) / 'learning.sqlite3')
            snap = self.sample_snapshot()
            first = labels.record('123', snap, 'yes', 'request:1')
            cleared = labels.record('123', snap, None, 'request:2', expected_id=first['id'])
            def correct(choice):
                try:
                    labels.record('123', snap, choice, 'request:' + choice, expected_id=cleared['id'])
                    return 'saved'
                except ReviewConflict:
                    return 'conflict'
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(correct, ['yes', 'no']))
            self.assertEqual(sorted(outcomes), ['conflict', 'saved'])

    def test_changed_jobs_policies_and_candidates_refuse_stale_labeling(self):
        for change in ('job', 'policy', 'candidate'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                service, snap, jobs = self.fixture(directory, with_candidate=True)
                if change == 'job':
                    changed = [{**posting, 'description': 'Changed public requirements'} for posting in jobs]
                    service.review.project(SOURCE, changed, '', 1001,
                        {'ashby:example:' + posting['id']: '2026-10-12T10:00:00Z' for posting in changed})
                elif change == 'policy':
                    token, _, _ = service.review.propose('123', {'locations': ['Manchester']})
                    service.review.confirm('123', token)
                else:
                    candidate = json.loads(service.candidate_path.read_text())
                    candidate['profile_revision'] += 1
                    service.candidate_path.write_text(json.dumps(candidate))
                with self.assertRaises(ReviewConflict):
                    service.decide('123', snap, 'yes', 'request:1')
                self.assertFalse(service.labels.path.exists())

    def test_missing_candidate_context_is_honest_and_never_filled_retroactively(self):
        with tempfile.TemporaryDirectory() as directory:
            service, snap, _ = self.fixture(directory)
            first = service.decide('123', snap, 'yes', 'request:1')
            self.assertEqual(snap['candidate']['status'], 'not_configured')
            service.candidate_path = Path(directory) / 'private-candidate.json'
            service.candidate_path.write_text(json.dumps(PrioritizationContracts().candidate()))
            with self.assertRaises(ReviewConflict):
                service.decide('123', snap, 'no', 'request:2', expected_id=first['id'])
            record = service.review.job(snap['job_key'])
            fresh = service.capture('123', record, 1, PROFILE, selection={'view': 'discord_jobs'})
            self.assertNotEqual(context_id('123', fresh), context_id('123', snap))
            self.assertIsNone(service.labels.latest('123', fresh))
            with sqlite3.connect(service.labels.path) as db:
                old = json.loads(db.execute('SELECT payload FROM snapshots').fetchone()[0])
            self.assertEqual(old['candidate']['status'], 'not_configured')

    def test_selection_changes_do_not_multiply_examples_and_review_feedback_is_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            service, snap, _ = self.fixture(directory)
            record = service.review.job(snap['job_key'])
            service.review.feedback('123', record['key'], record['hash'], 1, 'excluded')
            self.assertIsNone(service.labels.latest('123', snap))
            self.assertFalse(service.labels.path.exists())
            first = service.decide('123', snap, 'yes', 'request:1')
            new_view = service.capture('123', {**record, 'checked': 2000}, 1, PROFILE,
                                       selection={'view': 'discord_review', 'reason': 'location'})
            self.assertEqual(service.labels.latest('123', new_view)['id'], first['id'])

    def test_invalid_candidate_or_wrong_database_is_not_silently_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            service, snap, _ = self.fixture(directory, with_candidate=True)
            before = service.review.path.read_bytes()
            with self.assertRaises(ValueError):
                LearningStore(service.review.path).record('123', snap, 'yes', 'request:1')
            self.assertEqual(service.review.path.read_bytes(), before)
            service.candidate_path.write_text('{broken candidate')
            with self.assertRaises(ValueError):
                service.capture('123', service.review.job(snap['job_key']), 1, PROFILE, selection={})
            self.assertFalse(service.labels.path.exists())


class PrioritizationContracts(unittest.TestCase):
    """Synthetic candidate/job contracts; no personal profile or resume fixtures."""
    def candidate(self):
        return {
            "schema_version": 1, "profile_revision": 1,
            "candidate_summary": {"professional_experience_years_approximate": 3.5},
            "evidence_groups": [{"id": value} for value in ("work", "study", "course", "listed")],
            "capabilities": [
                {"id": "python", "name": "Python delivery", "evidence": "Built Python services.", "context": "professional",
                 "support_level": "demonstrated_in_resume", "evidence_group_ids": ["work"], "keywords": ["Python", "API development"]},
                {"id": "ci", "name": "CI", "evidence": "Automated GitLab CI.", "context": "professional",
                 "support_level": "demonstrated_in_resume", "evidence_group_ids": ["work"], "keywords": ["GitLab CI"]},
                {"id": "torch", "name": "Task fine-tuning", "evidence": "Fine-tuned an encoder in a study.", "context": "study",
                 "support_level": "demonstrated_in_resume", "evidence_group_ids": ["study"], "keywords": ["PyTorch", "supervised fine-tuning"]},
                {"id": "rl", "name": "RL course", "evidence": "Completed RL coursework.", "context": "study",
                 "support_level": "coursework_only", "evidence_group_ids": ["course"], "keywords": ["reinforcement learning"]}
            ],
            "declared_skills": {"evidence_group_id": "listed", "languages": ["Python", "C", "C++"], "cloud_and_systems": ["CUDA", "JAX"]},
            "career_interests": ["pre-training", "post-training", "RLHF", "DPO"],
            "matching_preferences": {
                "preferred_title_groups": [
                    {"id": "ai-engineer", "any_of": ["AI Engineer"]},
                    {"id": "ml-engineer", "any_of": ["ML Engineer", "Machine Learning Engineer"]},
                    {"id": "graduate", "any_of": ["Grad", "Graduate", "New Grad"]},
                    {"id": "forward-deployed", "any_of": ["Forward Deployed Engineer"]}
                ],
                "seniority": {"exclude_explicit_title_phrases": ["senior", "sr", "staff", "principal"],
                              "exceptions": ["Member of Technical Staff"], "review_ambiguous": ["Lead"]},
                "cohort": {"candidate_latest_graduation_year": 2026, "excluded_intake_years": [2027]},
                "ai_team_fallback": {"enabled": True, "positive_context": ["AI research", "machine learning", "pre-training", "post-training"]},
                "training_keyword_aliases": {"pre-training": ["PreTraining", "pre training"],
                                             "post-training": ["PostTraining", "post training", "RLHF", "DPO"]}
            },
            "target_organizations": {"organizations": [
                {"id": "example", "name": "Example Lab", "aliases": ["Example AI"], "parent_employers_for_team_review": ["Example Parent"]}
            ]}
        }

    def assess(self, title="AI Engineer", description="Requirements\nPython and PyTorch experience required.", **changes):
        return Evaluator(self.candidate()).assess(job(title=title, description=description, **changes), "Example Lab",
                                                now=datetime(2026, 11, 1, tzinfo=timezone.utc))

    def test_preferred_title_families_are_OR_and_nonstandard_AI_team_roles_are_retained(self):
        for title, family in (("AI Engineer", "ai-engineer"), ("Machine Learning Engineer", "ml-engineer"),
                              ("Graduate Engineer", "graduate"), ("Forward-Deployed Engineer", "forward-deployed")):
            with self.subTest(title=title):
                result = self.assess(title=title, team="AI research")
                self.assertIn(family, result["preferred_title_groups"])
                self.assertEqual(result["scope"], "in_scope")
        result = self.assess(title="Technical Program Associate", team="AI research")
        self.assertFalse(result["preferred_title_groups"])
        self.assertEqual(result["scope"], "in_scope")
        self.assertNotIn("graduate", self.assess(title="Gradient Optimization Engineer")["preferred_title_groups"])

    def test_seniority_only_applies_to_title_or_level_and_staff_exception_is_narrow(self):
        result = self.assess(description="Requirements\nPython experience required.\nYou will work with senior engineers.")
        self.assertEqual(result["scope"], "in_scope")
        for title in ("Senior AI Engineer", "Sr. ML Engineer", "Staff AI Engineer", "Senior Member of Technical Staff", "Principal Member of Technical Staff"):
            self.assertEqual(self.assess(title=title)["scope"], "out_of_scope", title)
        self.assertNotEqual(self.assess(title="Member of Technical Staff", team="AI research")["scope"], "out_of_scope")
        self.assertEqual(self.assess(job_level="Senior")["scope"], "out_of_scope")

    def test_cohort_year_is_not_a_global_year_substring_filter(self):
        for title in ("Graduate ML Engineer 2027", "2027 Graduate Engineer", "ML Engineer — 2027 intake"):
            self.assertEqual(self.assess(title=title)["scope"], "out_of_scope", title)
        self.assertEqual(self.assess(cohort_year=2027)["scope"], "out_of_scope")
        for description in (
                "Requirements\nMust graduate in 2027.\nPython required.",
                "Required qualifications\nGraduation year: 2027.\nPython required."):
            self.assertEqual(self.assess(description=description)["scope"], "out_of_scope", description)
        for description in (
                "Requirements\nGraduating between 2025 and 2027.\nPython required.",
                "Requirements\nPython required.\nCopyright 2027. Application deadline January 2027."):
            self.assertNotEqual(self.assess(description=description)["scope"], "out_of_scope")

    def test_training_interests_coursework_and_listed_tools_never_become_demonstrated_fit(self):
        result = self.assess(title="Member of Technical Staff, PostTraining", description=
            "Requirements\nRLHF and CUDA experience required.\nReinforcement learning experience preferred.")
        self.assertFalse(result["demonstrated_fit"])
        self.assertTrue(result["weaker_evidence"])
        self.assertEqual([item["term"] for item in result["listed_skills"]], ["CUDA"])
        self.assertIn("post-training", [item["interest"] for item in result["career_direction"]])
        self.assertTrue(any(gap["term"] == "RLHF" for gap in result["gaps"]))
        self.assertEqual(result["priority"], "Stretch")
        self.assertFalse(result["professional_group_count"])
        self.assertTrue(self.assess(title="AI Engineer, PreTraining")["career_direction"])

    def test_evidence_is_traceable_and_group_credit_is_deduplicated(self):
        result = self.assess(description="Requirements\nPython, API development and GitLab CI required.")
        self.assertEqual(result["evidence_group_count"], 1)
        self.assertEqual(result["professional_group_count"], 1)
        self.assertNotEqual(result["priority"], "Review first")
        for item in result["demonstrated_fit"]:
            capability = next(cap for cap in self.candidate()["capabilities"] if cap["id"] == item["capability_id"])
            self.assertEqual(item["candidate_evidence"], capability["evidence"])
            self.assertTrue(all(hit["excerpt"] in result["job"]["description"] for hit in item["matches"]))

    def test_negated_skills_and_company_background_do_not_create_fit_or_gaps(self):
        for description in (
                "Requirements\nPython experience is not required. No CUDA experience is required.",
                "Requirements\nPython and CUDA are not required.",
                "About us\nWe use Python, PyTorch and CUDA.\nResponsibilities\nCoordinate weekly meetings."):
            result = self.assess(description=description)
            self.assertFalse(result["demonstrated_fit"], description)
            self.assertFalse(result["gaps"], description)
            self.assertFalse(result["listed_skills"], description)
        result = self.assess(description="Requirements\nPython is required but CUDA is not required.")
        self.assertTrue(result["demonstrated_fit"])
        self.assertFalse(result["gaps"])

    def test_preferred_requirements_and_disjunctions_do_not_create_definite_gaps(self):
        optional = self.assess(description="Requirements\nPython required.\nPreferred qualifications\nCUDA experience.")
        self.assertFalse(optional["gaps"])
        alternative = self.assess(description="Requirements\nPython or CUDA experience required.")
        self.assertTrue(alternative["gaps"])
        self.assertTrue(all(item["uncertain_alternative"] for item in alternative["gaps"]))
        self.assertNotEqual(alternative["priority"], "Stretch")

    def test_experience_duration_and_student_requirements_remain_visible(self):
        result = self.assess(description="Requirements\nPython required.\nMinimum 5 years of software experience.\nMust be currently enrolled.")
        self.assertTrue(any(gap["term"] == "experience duration" for gap in result["gaps"]))
        self.assertTrue(any("enrollment" in note for note in result["uncertainties"]))
        self.assertEqual(result["priority"], "Stretch")

    def test_academic_evidence_does_not_satisfy_explicit_production_experience(self):
        result = self.assess(description="Requirements\nProduction PyTorch experience is required.\nPython required.")
        self.assertTrue(any(item["capability_id"] == "torch" for item in result["demonstrated_fit"]))
        self.assertTrue(any(gap["term"] == "PyTorch" and "not established" in gap["status"] for gap in result["gaps"]))
        self.assertEqual(result["priority"], "Stretch")
        specialist = self.assess(description="Requirements\n2 years of PyTorch experience required.\nPython required.")
        self.assertNotEqual(specialist["priority"], "Review first")

    def test_employer_identity_does_not_come_from_vendor_mentions(self):
        evaluator = Evaluator(self.candidate())
        posting = job(title="AI Engineer", description="Responsibilities\nBuild Python APIs using Example AI services.")
        self.assertIsNone(evaluator.assess(posting, "Other Employer")["target_organization"])
        self.assertEqual(evaluator.assess(posting, "Example AI")["target_organization"]["id"], "example")
        parent_result = evaluator.assess(posting, "Example Parent")
        self.assertIsNone(parent_result["target_organization"])
        self.assertEqual(parent_result["parent_employer_candidate"]["id"], "example")
        self.assertEqual(evaluator.assess({**posting, "team": "Example AI research"}, "Example Parent")["target_organization"]["id"], "example")

    def test_missing_descriptions_and_unknown_dates_do_not_invent_freshness_or_fit(self):
        self.assertEqual(self.assess(description="")["priority"], "Insufficient evidence")
        for kind, date in (("record created", "2026-10-31T00:00:00Z"), ("last published", "2026-10-31T00:00:00Z"),
                           ("first published", "2027-01-01T00:00:00Z"), ("first published", "invalid")):
            self.assertIsNone(self.assess(date_type=kind, published_at=date)["original_publication"])
        earlier = self.assess(date_type="first published", published_at="2026-10-29T10:00:00+02:00")
        later = self.assess(date_type="first published", published_at="2026-10-30T10:00:00Z")
        unknown = self.assess()
        self.assertEqual(sorted([unknown, earlier, later], key=ordering), [later, earlier, unknown])
        self.assertEqual(earlier["original_publication"], "2026-10-29T08:00:00+00:00")

    def test_profile_and_job_hashes_invalidate_results_without_mutating_inputs(self):
        profile = self.candidate()
        posting = job(title="AI Engineer", description="Requirements\nPython required.")
        before = copy.deepcopy((profile, posting))
        first = Evaluator(profile).assess(posting, "Example Lab")
        profile["profile_revision"] = 2
        second = Evaluator(profile).assess(posting, "Example Lab")
        self.assertNotEqual(first["profile_hash"], second["profile_hash"])
        posting["description"] += " PyTorch required."
        third = Evaluator(profile).assess(posting, "Example Lab")
        self.assertNotEqual(second["job_hash"], third["job_hash"])
        self.assertEqual(first["job"], before[1])
        self.assertEqual(before[0]["profile_revision"], 1)

    def test_invalid_profiles_fail_before_scoring_instead_of_silently_downgrading(self):
        broken = []
        for field, value in (("schema_version", True), ("profile_revision", 0), ("matching_preferences", [])):
            profile = self.candidate()
            profile[field] = value
            broken.append(profile)
        profile = self.candidate()
        profile["capabilities"][0]["evidence_group_ids"] = ["missing"]
        broken.append(profile)
        profile = self.candidate()
        profile["target_organizations"]["organizations"].append({"id": "another", "name": "Example AI"})
        broken.append(profile)
        for profile in broken:
            with self.assertRaises(ValueError):
                validate_candidate(profile)

    def test_manual_command_is_offline_read_only_and_keeps_conflicts_auditable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile_path, jobs_path = root / "candidate.json", root / "jobs.json"
            profile_path.write_text(json.dumps(self.candidate()))
            jobs_path.write_text(json.dumps({"jobs": [
                {"company": "Example Lab", "job": job(title="AI Engineer", description="Requirements\nPython required.")},
                {"company": "Example Lab", "job": job("2", title="Senior AI Engineer", description="Requirements\nPython required.")}
            ]}))
            before = {p.name: p.read_bytes() for p in root.iterdir()}
            output = io.StringIO()
            with patch("urllib.request.urlopen", side_effect=AssertionError("Offline ranking attempted network")), redirect_stdout(output):
                self.assertEqual(rank_main(["--candidate", str(profile_path), "--input", str(jobs_path), "--format", "json"]), 0)
            report = json.loads(output.getvalue())
            self.assertEqual(len(report["ranked"]), 1)
            self.assertEqual(len(report["excluded_from_shortlist"]), 1)
            self.assertIsNone(report["ranked"][0]["screening"]["eligible"])
            self.assertEqual(before, {p.name: p.read_bytes() for p in root.iterdir()})
            with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
                self.assertEqual(rank_main(["--candidate", str(profile_path), "--database", str(root / "absent.sqlite3")]), 1)
            self.assertFalse((root / "absent.sqlite3").exists())

    def test_database_snapshot_uses_live_policy_and_screened_out_jobs_cannot_enter_shortlist(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, jobs, state = ReviewContracts().setup_store(directory)
            before = (root / "review.sqlite3").read_bytes()
            records, origin = database_postings(store.path, user="123")
            self.assertEqual([record["job"]["id"] for record in records], ["1"])
            all_records, _ = database_postings(store.path, include_screened_out=True)
            report = rank(all_records, Evaluator(self.candidate()))
            self.assertEqual([item["job"]["id"] for item in report["ranked"]], ["1"])
            self.assertEqual(len(report["excluded_from_shortlist"]), 3)
            self.assertEqual(before, (root / "review.sqlite3").read_bytes())
            self.assertEqual(store.policy("123"), (1, PROFILE))
            self.assertFalse(origin["loaded_by_worker"])
            with self.assertRaises(ValueError):
                database_postings(store.path, user="missing")

    def test_database_work_limit_rejects_large_input_without_changing_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            store, _, _ = ReviewContracts().setup_store(directory)
            before = store.path.read_bytes()
            with patch('job_monitor.rank_jobs.MAX_INPUT_BYTES', 10), self.assertRaises(ValueError):
                database_postings(store.path)
            self.assertEqual(before, store.path.read_bytes())


class ReviewContracts(unittest.TestCase):
    def setup_store(self, directory, *, profile=PROFILE):
        store = ReviewStore(Path(directory) / "review.sqlite3")
        store.initialise("123", profile)
        jobs = [job(), job("2", title="Founding Engineer"), job("3", title="Founding Engineer", location="Berlin"),
                job("4", title="Staff Software Engineer")]
        state = scan(empty_state(), jobs)
        store.project(SOURCE, jobs, "", 1000, {key: value["first_seen"] for key, value in state["jobs"].items()})
        return store, jobs, state

    def test_publication_age_uses_only_first_published_and_handles_boundaries(self):
        now = datetime(2026, 10, 15, tzinfo=timezone.utc)
        profile = {**PROFILE, "max_posting_age_days": 7}
        self.assertIsNotNone(screen(job(published_at="2026-10-08T00:00:00Z", date_type="first published"), profile, now=now))
        self.assertEqual(screen_with_reasons(job(published_at="2026-10-07T23:59:59Z", date_type="first published"), profile, now=now)[1], ["posting_age"])
        for kind, value in (("last published", "2026-10-14T00:00:00Z"), ("record created", "2026-10-14T00:00:00Z"),
                            ("first published", "2026-10-16T00:00:00Z"), ("first published", "2026-10-14"),
                            ("first published", "invalid"), ("unknown", "")):
            self.assertEqual(screen_with_reasons(job(published_at=value, date_type=kind), profile, now=now)[1], ["posting_date_unknown"])
        for value in (True, 0, 1.5, 366, "7"):
            with self.assertRaises(ValueError):
                validate_config({"sources": [SOURCE]}, {**PROFILE, "max_posting_age_days": value})
        self.assertEqual(screen(job(location="Manchester"), {**PROFILE, "locations": ["Manchester"]})["location_eligibility"], "Manchester")
        self.assertIsNone(screen(job(location="London"), {**PROFILE, "locations": ["Manchester"], "allow_uk_remote": False}))

    def test_preview_confirm_undo_are_versioned_and_owner_scoped(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            token, candidate, preview = store.propose("123", {"title_keywords": PROFILE["title_keywords"] + ["founding engineer"]}, now=1000)
            self.assertEqual(preview["added"], ["ashby:example:2"])
            self.assertEqual(store.policy("123"), (1, PROFILE))
            store.initialise("456", PROFILE)
            with self.assertRaises(ReviewConflict):
                store.confirm("456", token, now=1001)
            self.assertEqual(store.confirm("123", token, now=1001), 2)
            self.assertEqual(store.policy("123")[1], candidate)
            self.assertEqual(store.policy("456"), (1, PROFILE))
            with self.assertRaises(ReviewConflict):
                store.confirm("123", token, now=1002)
            undo_token, previous, delta = store.undo("123", 2, now=1002)
            self.assertEqual(delta["removed"], ["ashby:example:2"])
            store.confirm("123", undo_token, now=1003)
            self.assertEqual(store.policy("123")[1]["title_keywords"], PROFILE["title_keywords"])
            self.assertEqual(store.policy("123")[0], 3)
            self.assertEqual(read_state(Path(directory) / "state.json"), empty_state())

    def test_changed_jobs_settings_and_expired_drafts_cannot_be_silently_confirmed(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            patch_rules = {"title_keywords": PROFILE["title_keywords"] + ["founding engineer"]}
            first, _, _ = store.propose("123", patch_rules, now=1000)
            second, _, _ = store.propose("123", {"locations": ["Manchester"]}, now=1000)
            store.confirm("123", first, now=1001)
            with self.assertRaises(ReviewConflict):
                store.confirm("123", second, now=1002)
            token, _, _ = store.propose("123", {"locations": ["Manchester"]}, now=1003)
            changed = [*jobs, job("new", location="Manchester")]
            history = scan(state, changed)
            store.project(SOURCE, changed, "", 1004, {key: value["first_seen"] for key, value in history["jobs"].items()})
            with self.assertRaises(ReviewConflict):
                store.confirm("123", token, now=1005)
            token, _, _ = store.propose("123", {"locations": ["Manchester"]}, now=1006)
            with self.assertRaises(ReviewConflict):
                store.confirm("123", token, now=1607)
            self.assertEqual(store.policy("123")[0], 2)
            with self.assertRaises(ValueError):
                store.propose("123", {"experience_years": 0}, now=1007)
            token, _, _ = store.propose("123", {"excluded_title_keywords": []}, now=1008)
            store.cancel("456", token)
            self.assertTrue(store.draft("123", token))
            store.cancel("123", token)
            with self.assertRaises(ReviewConflict):
                store.confirm("123", token, now=1009)

    def test_review_feedback_is_invalidated_by_changed_postings_and_does_not_change_rules(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            record = store.job("ashby:example:2")
            store.feedback("123", record["key"], record["hash"], 1, "excluded", now=1000)
            rows, _ = store.jobs("123", rejected=True, now=1000)
            self.assertNotIn(record["key"], [row[0]["key"] for row in rows])
            self.assertEqual(store.policy("123"), (1, PROFILE))
            changed = [job("2", title="Founding Engineer", description="A revised advert")]
            store.project(SOURCE, changed, "", 1001, {"ashby:example:2": "2026-10-12T10:00:00Z"})
            rows, _ = store.jobs("123", rejected=True, now=1002)
            self.assertEqual([row[0]["key"] for row in rows], [record["key"]])
            with self.assertRaises(ReviewConflict):
                store.feedback("123", record["key"], record["hash"], 1, "shown", now=1002)
            store.project(SOURCE, [], "TimeoutError", 1003, {})
            self.assertTrue(store.job(record["key"])["error"])
            store.project(SOURCE, [], "", 1004, {})
            with self.assertRaises(ReviewConflict):
                store.job(record["key"])

    def test_temporary_search_does_not_modify_saved_filters(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            rows, total = store.jobs("123", days=7, now=1000)
            self.assertEqual(total, 0)  # Unknown publication is never replaced by discovery time.
            self.assertEqual(store.policy("123"), (1, PROFILE))
            rows, total = store.jobs("123", rejected=True, location="Berlin", now=1000)
            founding = next(row for row in rows if row[0]["key"] == "ashby:example:3")
            self.assertEqual(set(founding[2]), {"title", "location"})
            rows, total = store.jobs("123", rejected=True, reason="staff_level", now=1000)
            self.assertEqual([row[0]["key"] for row in rows], ["ashby:example:4"])

    def test_title_suggestions_only_group_fresh_technical_jobs_blocked_by_title_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            now = datetime(2026, 10, 16, tzinfo=timezone.utc).timestamp()
            profile = {**PROFILE, "max_posting_age_days": 7}
            store, _, _ = self.setup_store(directory, profile=profile)
            def posting(identity, **changes):
                return job(identity, **{"title": "Founding Engineer", "date_type": "first published",
                                       "published_at": "2026-10-15T00:00:00Z", **changes})
            jobs = [posting("a"), posting("b", title="Founding-Engineer"),
                    posting("berlin", location="Berlin"), posting("staff", title="Staff Founding Engineer"),
                    posting("sponsor", description="We cannot provide visa sponsorship."),
                    posting("old", published_at="2026-10-01T00:00:00Z"),
                    posting("unknown", date_type="last published"), posting("nontechnical", title="Account Executive")]
            first_seen = {"ashby:example:" + item["id"]: "2026-10-15T00:00:00Z" for item in jobs}
            store.project(SOURCE, jobs, "", now, first_seen)
            summary = store.title_suggestions("123", now=now)
            self.assertEqual(len(summary["groups"]), 1)
            group = summary["groups"][0]
            self.assertEqual(group["phrase"], "founding engineer")
            self.assertEqual({row["key"] for row in group["jobs"]}, {"ashby:example:a", "ashby:example:b"})
            self.assertEqual(summary["unknown_dates"], 1)
            proposal = store.propose_title("123", group["phrase"], group["evidence"], 1, now=now)
            self.assertEqual(set(proposal[2]["added"]), {"ashby:example:a", "ashby:example:b"})
            self.assertEqual(store.policy("123"), (1, profile))
            # Failed and stale source data stay inspectable but cannot recommend a rule change.
            store.project(SOURCE, jobs, "TimeoutError", now + 2, first_seen)
            self.assertEqual(store.title_suggestions("123", now=now + 2)["groups"], [])
            store.project(SOURCE, jobs, "", now, first_seen)
            self.assertEqual(store.title_suggestions("123", now=now + 181)["groups"], [])
            self.assertEqual(store.confirm("123", proposal[0], now=now + 4), 2)
            self.assertIsNotNone(screen(posting("future"), store.policy("123")[1], now=datetime.fromtimestamp(now, timezone.utc)))
            self.assertIsNone(screen(posting("future", location="Berlin"), store.policy("123")[1]))

    def test_dismissals_survive_unrelated_edits_but_not_changed_blocking_reasons(self):
        with tempfile.TemporaryDirectory() as directory:
            store, _, _ = self.setup_store(directory)
            record = store.job("ashby:example:2")
            store.feedback("123", record["key"], record["hash"], 1, "excluded", now=1000)
            token, _, _ = store.propose("123", {"locations": ["London", "Manchester"]}, now=1001)
            store.confirm("123", token, now=1002)
            rows, _ = store.jobs("123", rejected=True, now=1003)
            self.assertNotIn(record["key"], [row[0]["key"] for row in rows])
            token, _, _ = store.propose("123", {"locations": ["Berlin"]}, now=1004)
            store.confirm("123", token, now=1005)
            rows, _ = store.jobs("123", rejected=True, now=1006)
            returned = next(row for row in rows if row[0]["key"] == record["key"])
            self.assertEqual(set(returned[2]), {"title", "location"})

    def test_title_group_actions_reject_changed_evidence_and_dismiss_only_observed_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            store.initialise("456", PROFILE)
            group = store.title_suggestions("123", now=1000)["groups"][0]
            changed = [*jobs, job("new", title="Founding Engineer")]
            first_seen = {"ashby:example:" + item["id"]: "2026-10-12T10:00:00Z" for item in changed}
            store.project(SOURCE, changed, "", 1001, first_seen)
            for action in (store.propose_title, store.dismiss_title):
                with self.assertRaises(ReviewConflict):
                    action("123", group["phrase"], group["evidence"], 1, now=1002)
            group = store.title_suggestions("123", now=1002)["groups"][0]
            store.dismiss_title("123", group["phrase"], group["evidence"], 1, now=1003)
            self.assertEqual(store.title_suggestions("123", now=1003)["groups"], [])
            self.assertEqual(len(store.title_suggestions("456", now=1003)["groups"][0]["jobs"]), 2)
            self.assertEqual(store.policy("123"), (1, PROFILE))
            newest = job("newest", title="Founding Engineer")
            store.project(SOURCE, [*changed, newest], "", 1004, {**first_seen, "ashby:example:newest": "2026-10-12T10:00:00Z"})
            group = store.title_suggestions("123", now=1004)["groups"][0]
            self.assertEqual([row["key"] for row in group["jobs"]], ["ashby:example:newest"])
            token, _, _ = store.propose("123", {"locations": ["London", "Manchester"]}, now=1004)
            store.confirm("123", token, now=1005)
            with self.assertRaises(ReviewConflict):
                store.propose_title("123", group["phrase"], group["evidence"], 1, now=1006)

    def test_old_postings_resurface_when_age_adds_a_new_blocker(self):
        with tempfile.TemporaryDirectory() as directory:
            profile = {**PROFILE, "max_posting_age_days": 7}
            store, _, _ = self.setup_store(directory, profile=profile)
            now = datetime(2026, 10, 16, tzinfo=timezone.utc).timestamp()
            posting = job("aged", title="Founding Engineer", date_type="first published", published_at="2026-10-10T00:00:00Z")
            store.project(SOURCE, [posting], "", now, {"ashby:example:aged": "2026-10-15T00:00:00Z"})
            record = store.job("ashby:example:aged")
            store.feedback("123", record["key"], record["hash"], 1, "excluded", now=now)
            self.assertEqual(store.jobs("123", rejected=True, now=now)[1], 0)
            rows, _ = store.jobs("123", rejected=True, now=now + 2 * 86400)
            self.assertEqual(set(rows[0][2]), {"title", "posting_age"})

    def test_metadata_enrichment_is_bounded_cached_and_preserves_unknown_on_failure(self):
        source = {"provider": "greenhouse", "board": "example", "company": "Example"}
        posting = job(url="https://example.test/jobs/1")
        payload = {"id": "1", "title": posting["title"], "location": {"name": "London"}, "absolute_url": posting["url"],
                   "content": posting["description"], "first_published": "2026-10-14T00:00:00Z"}
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read = Mock(return_value=json.dumps(payload))
        opener = Mock(return_value=response)
        enricher = PublicationEnricher(opener=opener, clock=lambda: 1000)
        self.assertEqual(enricher.enrich(source, [posting], PROFILE), [posting])
        opener.assert_not_called()
        profile = {**PROFILE, "max_posting_age_days": 7}
        result = enricher.enrich(source, [posting, job("2")], profile)
        self.assertEqual(result[0]["date_type"], "first published")
        self.assertEqual(result[1]["date_type"], "unknown")
        self.assertEqual(posting["date_type"], "unknown")
        self.assertEqual(opener.call_count, 1)
        enricher.enrich(source, [posting], profile)
        self.assertEqual(opener.call_count, 1)
        limited = Mock(side_effect=urllib.error.HTTPError("https://example.test", 429, "rate limited", {"Retry-After": "900"}, None))
        enricher = PublicationEnricher(opener=limited, clock=lambda: 1000)
        self.assertEqual(enricher.enrich(source, [posting], profile)[0]["date_type"], "unknown")
        enricher.enrich(source, [job("2")], profile)
        self.assertEqual(limited.call_count, 1)
        self.assertEqual(enricher.retry["example"], 1900)

    def test_live_policy_reload_does_not_send_catchup_and_preserves_publication_unknown(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory)
            store, jobs, state = self.setup_store(directory)
            save_state(root / "state.json", state)
            runtime = ReviewRuntime(store, "123", PROFILE, fetcher=lambda source: jobs)
            sender = Mock()
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as deliveries:
                watcher = Watcher({"sources": [SOURCE]}, PROFILE, root / "state.json", root / "latest.md", polls, deliveries,
                                  sender=sender, now=1000, review=runtime)
                try:
                    token, _, _ = store.propose("123", {"title_keywords": PROFILE["title_keywords"] + ["founding engineer"]}, now=time.time())
                    store.confirm("123", token)
                    watcher.tick(1002)
                    deadline = time.monotonic() + 3
                    while (watcher.polls or "ashby:example" not in watcher.policy_checked_boards) and time.monotonic() < deadline:
                        watcher.tick(1002)
                        time.sleep(0.005)
                    self.assertTrue(watcher.state["jobs"]["ashby:example:2"]["matched"])
                    self.assertEqual(watcher.profile["title_keywords"][-1], "founding engineer")
                    self.assertFalse(watcher.state["pending"])
                    sender.assert_not_called()
                    self.assertEqual(watcher.state["jobs"]["ashby:example:2"]["first_seen"], state["jobs"]["ashby:example:2"]["first_seen"])
                finally:
                    runtime.close()

    def test_disabled_boards_and_waiting_sources_are_not_hidden_in_a_preview(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            waiting = {**SOURCE, "board": "waiting"}
            store.set_sources([waiting])
            self.assertEqual(store.jobs("123", rejected=True, now=1000)[1], 0)
            with self.assertRaises(ReviewConflict):
                store.job("ashby:example:2")
            _, _, preview = store.propose("123", {"max_posting_age_days": 7}, now=1000)
            self.assertEqual(preview["stale_boards"], 1)
            store.set_sources([SOURCE])
            self.assertEqual(store.jobs("123", rejected=True, now=1000)[1], 3)

    def test_competing_confirmations_commit_only_one_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = self.setup_store(directory)
            tokens = [store.propose("123", {"locations": [city]}, now=1000)[0] for city in ("Manchester", "Bristol")]
            def confirm(token):
                try:
                    return store.confirm("123", token, now=1001)
                except ReviewConflict:
                    return "conflict"
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(confirm, tokens))
            self.assertCountEqual(outcomes, [2, "conflict"])
            self.assertEqual(store.policy("123")[0], 2)

    def test_reloaded_policy_holds_old_pending_until_rechecked_and_new_jobs_still_alert(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory)
            store, jobs, state = self.setup_store(directory)
            state = scan(state, [*jobs, job("pending")], now=stamp(time.time()))
            save_state(root / "state.json", state)
            runtime = ReviewRuntime(store, "123", PROFILE, fetcher=lambda source: jobs)
            sender = Mock()
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as deliveries:
                watcher = Watcher({"sources": [SOURCE]}, PROFILE, root / "state.json", root / "latest.md", polls, deliveries,
                                  sender=sender, now=time.time(), review=runtime)
                try:
                    watcher.dispatch_delivery(time.time())
                    sender.assert_not_called()
                    self.assertEqual(len(watcher.state["pending"]), 1)
                    watcher.policy_checked_boards.add("ashby:example")
                    with patch.object(store, "policy_info", side_effect=sqlite3.OperationalError("locked")):
                        runtime.tick(time.time())
                    watcher.dispatch_delivery(time.time())
                    sender.assert_not_called()
                    runtime.tick(time.time() + 2)
                    watcher.dispatch_delivery(time.time())
                    watcher.delivery[0].result(timeout=1)
                    self.assertEqual(sender.call_count, 1)
                    watcher.delivery = None
                    token, _, _ = store.propose("123", {"title_keywords": PROFILE["title_keywords"] + ["founding engineer"]})
                    store.confirm("123", token)
                    _, expanded, created = store.policy_info("123")
                    observed = stamp(created + 1)
                    fresh = apply_scan(state, [(SOURCE, [*jobs, job("fresh", title="Founding Engineer")], "")], expanded,
                                       observed, policy_effective_at=created)
                    self.assertEqual([lead["job"]["id"] for lead in fresh["pending"]], ["fresh"])
                finally:
                    runtime.close()

    def test_pending_publication_metadata_refresh_preserves_identity_and_age_is_checked_at_send(self):
        previous = scan(scan(empty_state(), []), [job()], now="2026-10-14T00:00:00Z")
        before = previous["pending"][0]
        posting = job(published_at="2026-10-08T00:00:00Z", date_type="first published")
        profile = {**PROFILE, "max_posting_age_days": 7}
        updated = apply_scan(previous, [(SOURCE, [posting], "")], profile, "2026-10-14T00:00:00Z")
        self.assertEqual(updated["pending"][0]["event_id"], before["event_id"])
        self.assertEqual(updated["pending"][0]["queued_at"], before["queued_at"])
        self.assertEqual(updated["pending"][0]["job"]["date_type"], "first published")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_state(root / "state.json", updated)
            with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery:
                sender = Mock()
                watcher = Watcher({"sources": [SOURCE]}, profile, root / "state.json", root / "latest.md", polls, delivery,
                                  sender=sender, now=datetime(2026, 10, 16, tzinfo=timezone.utc).timestamp())
                watcher.dispatch_delivery(datetime(2026, 10, 16, tzinfo=timezone.utc).timestamp())
                sender.assert_not_called()


class ReviewActivationContracts(unittest.IsolatedAsyncioTestCase):
    async def test_activation_requires_fresh_error_free_worker_acknowledgement(self):
        with tempfile.TemporaryDirectory() as directory:
            store, _, _ = ReviewContracts().setup_store(directory)
            self.assertEqual(await wait_for_activation(store, "123", 1, timeout=0), "pending")
            for version, age, error in ((0, 0, ""), (1, 31, ""), (1, 0, "Unavailable")):
                store.worker_status("123", version, time.time() - age, error)
                self.assertEqual(await wait_for_activation(store, "123", 1, timeout=0), "pending")
            store.worker_status("123", 1, time.time())
            self.assertEqual(await wait_for_activation(store, "123", 1, timeout=0), "active")
            token, _, _ = store.propose("123", {"locations": ["Manchester"]})
            store.confirm("123", token)
            self.assertEqual(await wait_for_activation(store, "123", 1, timeout=0), "superseded")
            with patch.object(store, "policy", side_effect=sqlite3.OperationalError("busy")):
                self.assertEqual(await wait_for_activation(store, "123", 2, timeout=0), "pending")

    async def test_activation_message_can_wait_for_worker_without_reapplying_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            store, _, _ = ReviewContracts().setup_store(directory)
            async def worker_loads(_):
                store.worker_status("123", 1, time.time())
            with patch("job_monitor.review_bot.asyncio.sleep", side_effect=worker_loads) as sleep:
                self.assertEqual(await wait_for_activation(store, "123", 1), "active")
                sleep.assert_awaited_once()
            self.assertEqual(store.policy("123"), (1, PROFILE))


@unittest.skipUnless(importlib.util.find_spec("discord"), "Optional Discord SDK unavailable; run tests.test_monitor.DiscordReviewContracts in the review virtual environment")
class DiscordReviewContracts(unittest.IsolatedAsyncioTestCase):
    interaction_ids = itertools.count(1)

    def interaction(self, user=123):
        response = SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock(), is_done=Mock(return_value=True))
        return SimpleNamespace(id=next(self.interaction_ids), user=SimpleNamespace(id=user), guild_id=789, response=response,
                               followup=SimpleNamespace(send=AsyncMock()), edit_original_response=AsyncMock())

    async def test_owner_only_filter_preview_save_and_cancel_use_real_discord_views(self):
        from job_monitor.review_bot import build_client
        import discord
        def interaction(user=123):
            response = SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock(), is_done=Mock(return_value=True))
            return SimpleNamespace(user=SimpleNamespace(id=user), guild_id=789, response=response,
                                   followup=SimpleNamespace(send=AsyncMock()), edit_original_response=AsyncMock())
        with tempfile.TemporaryDirectory() as directory:
            store, _, _ = ReviewContracts().setup_store(directory)
            client = build_client(store, 123, 789, activation_timeout=0)
            try:
                unauthorized = interaction(456)
                await client.tree.get_command("filters").callback(unauthorized)
                unauthorized.response.send_message.assert_awaited_once()
                unauthorized.followup.send.assert_not_awaited()
                authorized = interaction()
                await client.tree.get_command("filters").callback(authorized)
                view = authorized.followup.send.call_args.kwargs["view"]
                select = next(item for item in view.children if isinstance(item, discord.ui.Select))
                clicked = interaction()
                with patch.object(type(select), "values", new_callable=PropertyMock, return_value=["7"]):
                    await select.callback(clicked)
                self.assertEqual(store.policy("123")[0], 1)
                preview = clicked.followup.send.call_args.kwargs["view"]
                saving = interaction()
                await next(item for item in preview.children if item.style == discord.ButtonStyle.primary).callback(saving)
                self.assertEqual(store.policy("123")[1]["max_posting_age_days"], 7)
                self.assertEqual(store.policy("123")[0], 2)
                self.assertEqual(saving.edit_original_response.await_count, 2)
                denied_click = interaction(456)
                self.assertFalse(await preview.interaction_check(denied_click))
            finally:
                await client.close()

    async def test_grouped_review_inspection_and_save_require_explicit_confirmation(self):
        from job_monitor.review_bot import build_client
        import discord
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = ReviewContracts().setup_store(directory)
            store.project(SOURCE, jobs, "", time.time(), {key: value["first_seen"] for key, value in state["jobs"].items()})
            client = build_client(store, 123, 789, activation_timeout=0)
            try:
                opening = self.interaction()
                await client.tree.get_command("review").callback(opening)
                view = opening.followup.send.call_args.kwargs["view"]
                self.assertFalse(await view.interaction_check(self.interaction(456)))
                inspect = self.interaction()
                await next(item for item in view.children if item.label == "View these jobs").callback(inspect)
                inspect.followup.send.assert_not_awaited()
                detail = inspect.edit_original_response.call_args.kwargs["view"]
                self.assertTrue(any(item.url == job("2")["url"] for item in detail.children))
                proposing = self.interaction()
                await next(item for item in view.children if item.style == discord.ButtonStyle.primary).callback(proposing)
                self.assertEqual(store.policy("123"), (1, PROFILE))
                preview = proposing.followup.send.call_args.kwargs["view"]
                saving = self.interaction()
                confirm = store.confirm
                def confirm_and_load(*args):
                    version = confirm(*args)
                    store.worker_status("123", version, time.time())
                    return version
                with patch.object(store, "confirm", side_effect=confirm_and_load):
                    await next(item for item in preview.children if item.style == discord.ButtonStyle.primary).callback(saving)
                self.assertIn("founding engineer", store.policy("123")[1]["title_keywords"])
                self.assertIn("settings are active", saving.edit_original_response.call_args.kwargs["content"])
            finally:
                await client.close()

    async def test_group_dismissal_updates_same_message_without_changing_rules(self):
        from job_monitor.review_bot import build_client
        with tempfile.TemporaryDirectory() as directory:
            store, jobs, state = ReviewContracts().setup_store(directory)
            store.project(SOURCE, jobs, "", time.time(), {key: value["first_seen"] for key, value in state["jobs"].items()})
            client = build_client(store, 123, 789)
            try:
                opening = self.interaction()
                await client.tree.get_command("review").callback(opening)
                view = opening.followup.send.call_args.kwargs["view"]
                clicked = self.interaction()
                await next(item for item in view.children if item.label == "Dismiss these jobs").callback(clicked)
                clicked.edit_original_response.assert_awaited_once()
                clicked.followup.send.assert_not_awaited()
                self.assertEqual(store.title_suggestions("123")["groups"], [])
                self.assertEqual(store.policy("123"), (1, PROFILE))
            finally:
                await client.close()

    async def test_yes_no_skip_correction_reason_and_undo_use_real_owner_only_views(self):
        from job_monitor.review_bot import build_client
        import discord
        with tempfile.TemporaryDirectory() as directory:
            service, snapshot, jobs = LearningContracts().fixture(directory, with_candidate=True)
            client = build_client(service.review, 123, 789, learning=service)
            def control(view, label):
                return next(item for item in view.children if isinstance(item, discord.ui.Button) and item.label == label)
            async def open_jobs():
                opened = self.interaction()
                await client.tree.get_command('jobs').callback(opened)
                return opened.followup.send.call_args.kwargs['view']
            try:
                view = await open_jobs()
                self.assertFalse(service.labels.path.exists())
                denied = self.interaction(456)
                await control(view, 'Yes').callback(denied)
                denied.response.send_message.assert_awaited_once()
                wrong_server = self.interaction()
                wrong_server.guild_id = 999
                await control(view, 'No').callback(wrong_server)
                wrong_server.response.send_message.assert_awaited_once()
                self.assertFalse(service.labels.path.exists())
                await control(view, 'Skip').callback(self.interaction())
                self.assertFalse(service.labels.path.exists())
                view = await open_jobs()
                # A new listing changes the page order, but saving stays on the labeled job.
                extra = job('new', title='Software Engineer')
                changed = [*jobs, extra]
                service.review.project(SOURCE, changed, '', time.time(),
                    {'ashby:example:' + item['id']: '2026-10-12T10:00:00Z' if item['id'] != 'new' else '2026-11-01T00:00:00Z' for item in changed})
                clicked = self.interaction()
                await control(view, 'Yes').callback(clicked)
                self.assertEqual(service.labels.latest('123', snapshot)['choice'], 'yes')
                view = clicked.edit_original_response.call_args.kwargs['view']
                self.assertEqual(control(view, 'View posting').url, job()['url'])
                self.assertEqual(control(view, 'Yes').style, discord.ButtonStyle.primary)
                changed_choice = self.interaction()
                await control(view, 'No').callback(changed_choice)
                self.assertEqual(service.labels.latest('123', snapshot)['choice'], 'no')
                view = changed_choice.edit_original_response.call_args.kwargs['view']
                reverted = self.interaction()
                await control(view, 'Undo decision').callback(reverted)
                self.assertEqual(service.labels.latest('123', snapshot)['choice'], 'yes')
                view = reverted.edit_original_response.call_args.kwargs['view']
                withdrawn = self.interaction()
                await control(view, 'Clear decision').callback(withdrawn)
                self.assertIsNone(service.labels.latest('123', snapshot)['choice'])
                view = withdrawn.edit_original_response.call_args.kwargs['view']
                restored = self.interaction()
                await control(view, 'Undo decision').callback(restored)
                view = restored.edit_original_response.call_args.kwargs['view']
                select = next(item for item in view.children if isinstance(item, discord.ui.Select))
                with patch.object(type(select), 'values', new_callable=PropertyMock, return_value=['interest']):
                    await select.callback(self.interaction())
                self.assertEqual(service.labels.latest('123', snapshot)['reason'], 'interest')
                self.assertEqual(service.review.policy('123'), (1, PROFILE))
                with service.review.connect() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM feedback').fetchone()[0], 0)
            finally:
                await client.close()

    async def test_failed_label_storage_keeps_browsing_and_does_not_infer_no(self):
        from job_monitor.review_bot import build_client
        import discord
        with tempfile.TemporaryDirectory() as directory:
            service, snapshot, _ = LearningContracts().fixture(directory)
            client = build_client(service.review, 123, 789, learning=service)
            try:
                opened = self.interaction()
                await client.tree.get_command('jobs').callback(opened)
                view = opened.followup.send.call_args.kwargs['view']
                yes = next(item for item in view.children if isinstance(item, discord.ui.Button) and item.label == 'Yes')
                clicked = self.interaction()
                with patch.object(service.labels, 'record', side_effect=sqlite3.OperationalError('busy')):
                    await yes.callback(clicked)
                clicked.followup.send.assert_awaited_once()
                self.assertFalse(service.labels.path.exists())
                unavailable = self.interaction()
                with patch.object(service.labels, 'latest', side_effect=sqlite3.OperationalError('busy')):
                    await client.tree.get_command('jobs').callback(unavailable)
                view = unavailable.followup.send.call_args.kwargs['view']
                self.assertTrue(any(isinstance(item, discord.ui.Button) and item.url == job()['url'] for item in view.children))
                self.assertFalse(any(isinstance(item, discord.ui.Button) and item.label == 'No' for item in view.children))
                self.assertEqual(service.review.policy('123'), (1, PROFILE))
            finally:
                await client.close()


class FakeDeploymentVM:
    """Temporary-disk deployment exercise; no SSH, sudo, systemd or notifications."""
    def __init__(self, root, unit, mode="success"):
        self.root, self.unit, self.mode = root, unit, mode
        self.now, self.pid, self.starts = 1000, 7, 0
        self.active = True
        self.sudo_authenticated = mode != "needs_password"
        self.calls = []
        self.heartbeat()

    def heartbeat(self):
        (self.root / "health.json").write_text(json.dumps({"version": 1, "worker": {
            "pid": self.pid, "status": "running", "heartbeat_at": stamp(self.now)}}))

    def sleep(self, seconds):
        self.now += seconds

    def __call__(self, args, **kwargs):
        self.calls.append(tuple(args))
        if args[:2] == ["systemd-analyze", "verify"] and Path(args[-1]).name == "job-monitor-review.service":
            if not os.access(self.root / ".venv-review/bin/python", os.X_OK):
                raise subprocess.CalledProcessError(1, args)
        if "unittest" in args and self.mode == "invalid":
            raise subprocess.CalledProcessError(1, args)
        if args in (["sudo", "-v"], ["sudo", "-n", "-v"]) and self.mode == "validation_password":
            raise subprocess.CalledProcessError(1, args)
        if args[:3] == ["sudo", "systemctl", "show"]:
            self.sudo_authenticated = True
        elif args[:2] == ["sudo", "-n"] and not self.sudo_authenticated:
            raise subprocess.CalledProcessError(1, args)
        output, status = "", 0
        if args[:3] == ["sudo", "-n", "systemctl"]:
            if args[3] == "stop":
                self.active = False
                if self.starts == 0:
                    state = read_state(self.root / "state.json")
                    if "drained-before-stop" not in state["notified"]:
                        state["notified"].append("drained-before-stop")
                    save_state(self.root / "state.json", state)
            if args[3] == "start":
                self.active = True
                self.starts += 1
                self.pid += 1
                state = read_state(self.root / "state.json")
                for source in json.loads((self.root / "sources.json").read_text())["sources"]:
                    board = source["provider"] + ":" + source["board"]
                    state["boards"].setdefault(board, {}).update(last_success=stamp(self.now))
                if state["pending"]:
                    acknowledge_delivery(state, state["pending"][0], self.now)
                save_state(self.root / "state.json", state)
                if self.mode != "stale" or self.starts > 1:
                    self.heartbeat()
        elif args[:3] == ["sudo", "-n", "install"]:
            shutil.copyfile(args[-2], args[-1])
        elif args[:2] == ["systemctl", "is-active"]:
            status = 0 if self.active else 1
        elif args[:2] == ["systemctl", "show"]:
            output = str(self.pid)
        elif "--health" in args:
            status = 1 if self.mode == "unhealthy" and self.starts == 1 else 0
            output = "Health: healthy" if status == 0 else "Health: attention"
        if kwargs.get("check", True) and status:
            raise subprocess.CalledProcessError(status, args)
        return subprocess.CompletedProcess(args, status, output, "")


class DeploymentContracts(unittest.TestCase):
    def fixture(self, directory, mode="success", *, update_config=False):
        base = Path(directory).resolve()
        project, root, stage = base / "project", base / "job-monitor", base / "stage"
        project.mkdir()
        root.mkdir()
        stage.mkdir()
        repository = Path(__file__).resolve().parent.parent
        originals = {}
        for name in FILES:
            destination = project / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(repository / name, destination)
            original = None if name in ("deployment/deploy.py", "deployment/deploy.sh") else b"# Previous release\n" + destination.read_bytes()
            originals[name] = original
            if original is not None:
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_bytes(original)
        (project / "sources.json").write_text(json.dumps({"sources": [SOURCE]}))
        (project / "profile.json").write_text(json.dumps(PROFILE))
        (root / "sources.json").write_text(json.dumps({"sources": [{**SOURCE, "company": "Production"}]}))
        (root / "profile.json").write_text(json.dumps({**PROFILE, "experience_years": 9}))
        state = scan(scan(empty_state(), [job()], now=stamp(900)), [job(), job("2")], now=stamp(990))
        save_state(root / "state.json", state)
        unit = base / "etc" / "job-monitor.service"
        unit.parent.mkdir()
        unit.write_bytes(originals["deployment/job-monitor.service"])
        vm = FakeDeploymentVM(root, unit, mode)
        make_bundle(project, stage, update_config=update_config)
        deployer = Deployer(root, stage, unit=unit, runner=vm,
                            clock=lambda: vm.now, sleep=vm.sleep, timeout=1)
        return project, root, stage, unit, vm, deployer, originals

    def assert_previous_code(self, root, unit, originals):
        for name, previous in originals.items():
            if previous is None:
                self.assertFalse((root / name).exists(), name)
            else:
                self.assertEqual((root / name).read_bytes(), previous, name)
        self.assertEqual(unit.read_bytes(), originals["deployment/job-monitor.service"])

    def test_release_allowlist_excludes_history_and_secrets_and_rejects_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            secret = os.urandom(16).hex()
            for name in ("state.json", ".env", "oracle-job-monitor.key", "candidate-profile.json", "learning.sqlite3", "learning-labels.json"):
                (project / name).write_text(secret)
            clean = Path(directory) / "clean"
            clean.mkdir()
            manifest = make_bundle(project, clean)
            self.assertEqual(set(manifest["files"]), set(FILES))
            self.assertFalse(any((clean / name).exists() for name in ("state.json", ".env", "oracle-job-monitor.key", "profile.json", "candidate-profile.json", "learning.sqlite3", "learning-labels.json")))
            self.assertTrue(all(secret not in (clean / name).read_text() for name in manifest["files"]))
            (project / "scripts/monitor.py").unlink()
            (project / "scripts/monitor.py").symlink_to(project / ".env")
            with self.assertRaises(DeploymentError):
                make_bundle(project, clean)

    def test_changed_bundle_is_rejected_before_stopping_the_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            (stage / "scripts/monitor.py").write_text("tampered")
            with self.assertRaises(DeploymentError):
                deployment.deploy()
            self.assertFalse(vm.calls)
            self.assertTrue(vm.active)
            self.assert_previous_code(root, unit, originals)

    def test_success_preserves_config_history_and_secret_and_repeat_has_no_restart(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            before = {name: (root / name).read_bytes() for name in ("sources.json", "profile.json")}
            secret = Path(directory) / "notifications.env"
            secret.write_text(os.urandom(16).hex())
            secret_before = secret.read_bytes()
            deployment.deploy()
            manifest = read_manifest(stage)
            self.assertEqual(json.loads((root / ".deployment.json").read_text())["release_id"], manifest["release_id"])
            self.assertEqual(read_state(root / "state.json")["jobs"].keys(), read_state(deployment.backup / "state.json")["jobs"].keys())
            self.assertIn("drained-before-stop", read_state(deployment.backup / "state.json")["notified"])
            self.assertEqual(len(read_state(deployment.backup / "state.json")["pending"]), 1)
            self.assertFalse(read_state(root / "state.json")["pending"])
            self.assertEqual(secret.read_bytes(), secret_before)
            for name, value in before.items():
                self.assertEqual((root / name).read_bytes(), value)
            starts = vm.starts
            deployment.deploy()
            self.assertEqual(vm.starts, starts)

    def test_validation_failure_keeps_running_code_and_history_untouched(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory, "invalid")
            history = (root / "state.json").read_bytes()
            with self.assertRaises(subprocess.CalledProcessError):
                deployment.deploy()
            self.assertTrue(vm.active)
            self.assertEqual(vm.starts, 0)
            self.assertEqual((root / "state.json").read_bytes(), history)
            self.assert_previous_code(root, unit, originals)

    def test_existing_review_service_is_restarted_with_update_or_restored_on_failure(self):
        for mode in ("success", "unhealthy"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
                project, root, stage, unit, vm, deployment, originals = self.fixture(directory, mode)
                review_unit = unit.with_name("job-monitor-review.service")
                python = root / ".venv-review/bin/python"
                python.parent.mkdir(parents=True)
                python.write_text("#!/bin/sh\nexit 0\n")
                python.chmod(0o755)
                review_store = ReviewStore(root / "review.sqlite3")
                review_store.initialise("123", PROFILE)
                labels = LearningStore(root / 'learning.sqlite3')
                snapshot = LearningContracts().sample_snapshot()
                initial_label = labels.record('123', snapshot, 'yes', 'initial:1')
                updated_after_backup = [False]
                previous = b"# previous review unit\n" + (stage / "deployment/job-monitor-review.service").read_bytes()
                review_unit.write_bytes(previous)
                active = [True]
                def runner(args, **kwargs):
                    if (mode == 'unhealthy' and args == ['sudo', '-n', 'systemctl', 'start', 'job-monitor.service']
                            and not updated_after_backup[0]):
                        labels.record('123', snapshot, 'no', 'later:2', expected_id=initial_label['id'])
                        updated_after_backup[0] = True
                    if args[:3] == ["sudo", "-n", "systemctl"] and args[-1] == "job-monitor-review.service":
                        vm.calls.append(tuple(args))
                        active[0] = args[3] == "start"
                        return subprocess.CompletedProcess(args, 0, "", "")
                    if args[:2] == ["systemctl", "is-active"] and args[-1] == "job-monitor-review.service":
                        return subprocess.CompletedProcess(args, 0 if active[0] else 1, "", "")
                    return vm(args, **kwargs)
                deployment.runner = runner
                if mode == "unhealthy":
                    with self.assertRaises(DeploymentError):
                        deployment.deploy()
                    self.assertEqual(review_unit.read_bytes(), previous)
                else:
                    deployment.deploy()
                    self.assertEqual(review_unit.read_bytes(), (stage / "deployment/job-monitor-review.service").read_bytes())
                self.assertTrue(active[0])
                self.assertEqual(ReviewStore(deployment.backup / "review.sqlite3").policy("123"), (1, PROFILE))
                self.assertEqual(review_store.policy("123"), (1, PROFILE))
                self.assertEqual(LearningStore(deployment.backup / 'learning.sqlite3').latest('123', snapshot)['choice'], 'yes')
                self.assertEqual(labels.latest('123', snapshot)['choice'], 'no' if mode == 'unhealthy' else 'yes')
                self.assertLess(vm.calls.index(("sudo", "-n", "systemctl", "stop", "job-monitor-review.service")),
                                vm.calls.index(("sudo", "-n", "systemctl", "stop", "job-monitor.service")))
                self.assertLess(vm.calls.index((str(python), "-B", "-m", "unittest", "-v", "tests.test_monitor.DiscordReviewContracts")),
                                vm.calls.index(("sudo", "-n", "systemctl", "stop", "job-monitor-review.service")))

    def test_real_sdk_check_failure_stops_deployment_before_either_service_is_stopped(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            deployment.review_unit.write_bytes((stage / "deployment/job-monitor-review.service").read_bytes())
            python = root / ".venv-review/bin/python"
            python.parent.mkdir(parents=True)
            python.write_text("#!/bin/sh\nexit 0\n")
            python.chmod(0o755)
            def runner(args, **kwargs):
                if "tests.test_monitor.DiscordReviewContracts" in args:
                    self.assertEqual(kwargs["cwd"], stage)
                    raise subprocess.CalledProcessError(1, args)
                return vm(args, **kwargs)
            deployment.runner = runner
            history = (root / "state.json").read_bytes()
            with self.assertRaises(subprocess.CalledProcessError):
                deployment.deploy()
            self.assertIsNone(deployment.backup)
            self.assertTrue(vm.active)
            self.assertFalse(any("stop" in call for call in vm.calls))
            self.assertEqual((root / "state.json").read_bytes(), history)
            self.assert_previous_code(root, unit, originals)

    def test_review_dependencies_are_required_only_when_updating_an_active_review_service(self):
        for review_status in ("absent", "inactive", "active"):
            with self.subTest(review_status=review_status), tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
                project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
                if review_status != "absent":
                    deployment.review_unit.write_bytes((stage / "deployment/job-monitor-review.service").read_bytes())
                def runner(args, **kwargs):
                    if args[:2] == ["systemctl", "is-active"] and args[-1] == "job-monitor-review.service":
                        return subprocess.CompletedProcess(args, 0 if review_status == "active" else 3, "", "")
                    return vm(args, **kwargs)
                deployment.runner = runner
                history = (root / "state.json").read_bytes()
                self.assertFalse((root / ".venv-review/bin/python").exists())
                if review_status == "active":
                    with self.assertRaises(subprocess.CalledProcessError):
                        deployment.deploy()
                    self.assertTrue(vm.active)
                    self.assertEqual(vm.starts, 0)
                    self.assertIsNone(deployment.backup)
                    self.assertEqual((root / "state.json").read_bytes(), history)
                    self.assert_previous_code(root, unit, originals)
                else:
                    deployment.deploy()
                    self.assertTrue(vm.active)
                    self.assertEqual(vm.starts, 1)
                    self.assertFalse(any(call[:2] == ("systemd-analyze", "verify") and
                                         Path(call[-1]).name == "job-monitor-review.service" for call in vm.calls))
                    self.assertNotIn(("sudo", "-n", "systemctl", "start", "job-monitor-review.service"), vm.calls)
                    if review_status == "absent":
                        self.assertFalse(deployment.review_unit.exists())

    def test_failed_or_stale_health_rolls_back_code_and_keeps_new_delivery_progress(self):
        for mode in ("unhealthy", "stale"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
                project, root, stage, unit, vm, deployment, originals = self.fixture(directory, mode, update_config=True)
                config_before = {name: (root / name).read_bytes() for name in ("sources.json", "profile.json")}
                with self.assertRaises(DeploymentError):
                    deployment.deploy()
                self.assertTrue(vm.active)
                self.assert_previous_code(root, unit, originals)
                state = read_state(root / "state.json")
                self.assertFalse(state["pending"])
                self.assertEqual(len(state["notified"]), 2)
                self.assertTrue(read_state(deployment.backup / "state.json")["pending"])
                self.assertFalse((root / ".deployment.json").exists())
                for name, value in config_before.items():
                    self.assertEqual((root / name).read_bytes(), value)

    def test_sudo_authentication_precedes_cutover_and_failure_leaves_monitor_untouched(self):
        for interactive, mode in ((False, "needs_password"), (True, "needs_password"),
                                  (False, "validation_password")):
            with self.subTest(interactive=interactive, mode=mode), tempfile.TemporaryDirectory() as directory, \
                    redirect_stdout(io.StringIO()), patch("deployment.deploy.sys.stdin.isatty", return_value=interactive):
                project, root, stage, unit, vm, deployment, originals = self.fixture(directory, mode)
                history = (root / "state.json").read_bytes()
                if interactive or mode == "validation_password":
                    deployment.deploy()
                    self.assertTrue(vm.active)
                    self.assertEqual(vm.starts, 1)
                    probe = ("sudo",) + (() if interactive else ("-n",)) + (
                        "systemctl", "show", "job-monitor.service", "--property=MainPID", "--value")
                    self.assertLess(vm.calls.index(probe),
                                    vm.calls.index(("sudo", "-n", "systemctl", "stop", "job-monitor.service")))
                    self.assertTrue((root / ".deployment.json").exists())
                else:
                    with self.assertRaisesRegex(DeploymentError, "service permission check failed"):
                        deployment.deploy()
                    self.assertTrue(vm.active)
                    self.assertEqual(vm.starts, 0)
                    self.assertEqual((root / "state.json").read_bytes(), history)
                    self.assert_previous_code(root, unit, originals)
                    self.assertNotIn(("sudo", "-v"), vm.calls)
                    self.assertIsNone(deployment.backup)
                self.assertFalse(any("-v" in call and call[0] == "sudo" for call in vm.calls))

    def test_partial_install_failure_restores_every_changed_file(self):
        from deployment.deploy import atomic_copy
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            failed = []
            def copy_file(source, destination):
                if destination == root / "job_monitor/health.py" and not failed:
                    failed.append(True)
                    raise OSError("disk unavailable")
                return atomic_copy(source, destination)
            with patch("deployment.deploy.atomic_copy", side_effect=copy_file), self.assertRaises(DeploymentError):
                deployment.deploy()
            self.assertTrue(vm.active)
            self.assert_previous_code(root, unit, originals)
            self.assertIn("drained-before-stop", read_state(root / "state.json")["notified"])

    def test_deployment_lock_rejects_overlapping_update(self):
        with tempfile.TemporaryDirectory() as directory:
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            with (root.parent / ".job-monitor-deploy.lock").open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(DeploymentError):
                    deployment.deploy()
            self.assertFalse(vm.calls)
            self.assertTrue(vm.active)

    def test_client_preserves_vm_cadence_and_quotes_transport_without_real_ssh(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            project, root, stage, unit, vm, deployment, originals = self.fixture(directory)
            uploads = []
            unit.write_text(unit.read_text().replace("--interval 60", "--interval 30"))
            def ssh(args, **kwargs):
                if kwargs.get("stdin"):
                    with tarfile.open(fileobj=kwargs["stdin"], mode="r:") as bundle:
                        uploads.append(bundle.extractfile("deployment/job-monitor.service").read())
                    return subprocess.CompletedProcess(args, 0, "", "")
                if "mktemp" in args[-1]:
                    return subprocess.CompletedProcess(args, 0, "/home/ubuntu/job-monitor-release.fake\n", "")
                arguments = shlex.split(args[-1])
                if arguments[:3] == ["python3", "-B", "-c"]:
                    class RemoteParser(configparser.ConfigParser):
                        def read(self, filenames, encoding=None):
                            return super().read(unit, encoding=encoding)
                    output = io.StringIO()
                    with patch("configparser.ConfigParser", RemoteParser), redirect_stdout(output):
                        exec(arguments[-1], {})
                    return subprocess.CompletedProcess(args, 0, output.getvalue(), "")
                return subprocess.CompletedProcess(args, 0, "30\n", "")
            for interactive in (False, True):
                with self.subTest(interactive=interactive), \
                        patch("deployment.deploy.sys.stdin.isatty", return_value=interactive), \
                        patch("deployment.deploy.subprocess.run", side_effect=ssh) as command:
                    client(project, "job-monitor")
                    executions = []
                    for call in command.call_args_list:
                        args = call.args[0]
                        if "--remote" in args[-1]:
                            executions.append(args)
                            self.assertIn("-t" if interactive else "-T", args[:-1])
                        else:
                            self.assertNotIn("-t", args[:-1])
                    self.assertEqual(len(executions), 1)
            self.assertIn(b"--interval 30 ", uploads[0])
            with patch("deployment.deploy.subprocess.run") as command, self.assertRaises(DeploymentError):
                client(project, "host;echo unsafe")
            command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
