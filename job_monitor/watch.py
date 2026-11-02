"""A single coordinator owns history; bounded workers perform only network I/O."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import math
import os
import signal
import threading
import time

from job_monitor.monitor import (MIN_POLL_INTERVAL, acknowledge_delivery, apply_scan, baseline_summary,
                     delivery_snapshot, read_state, ready_delivery, report, save_state, validate_config)
from job_monitor.sources import fetch
from job_monitor.health import build_health, elapsed, render_health, stamp
from job_monitor.screening import posting_age_rejection
from job_monitor.retry import retry_after_seconds


def retrieve(source, fetcher):
    started = time.monotonic()
    try:
        return fetcher(source), "", 0, time.monotonic() - started
    except Exception as exc:
        error = type(exc).__name__ + (f" HTTP {exc.code}" if hasattr(exc, "code") else "")
        return [], error, retry_after_seconds(getattr(exc, "headers", None), time.time()), time.monotonic() - started


def send(sender, event):
    try:
        sender([event])
        return None
    except Exception as exc:
        return exc


class Watcher:
    """Network workers never touch mutable state or write history files."""
    def __init__(self, config, profile, state_path, report_path, poll_pool, delivery_pool,
                 *, interval=60, fetcher=fetch, sender=None, now=None, health_path=None, review=None):
        if not math.isfinite(interval) or interval < MIN_POLL_INTERVAL:
            raise ValueError(f"Polling interval must be at least {MIN_POLL_INTERVAL} seconds")
        self.sources = validate_config(config, profile)
        if not state_path.exists():
            raise ValueError("Establish a baseline with monitor.py before starting --watch")
        self.state = read_state(state_path)
        self.profile, self.state_path, self.report_path = profile, state_path, report_path
        self.poll_pool, self.delivery_pool = poll_pool, delivery_pool
        self.interval, self.fetcher, self.sender = interval, fetcher, sender
        self.review = review
        self.review_version = review.version if review else None
        self.policy_checked_boards = set()
        if review:
            review.store.set_sources(self.sources)
            self.profile = review.profile
            self.fetcher = review.fetch
        self.baseline_policy = config.get("baseline_policy", "suppress")
        now = time.time() if now is None else now
        self.config = config
        self.health_path = health_path or state_path.with_name("health.json")
        self.worker_started_at = stamp(now)
        self.last_health_at = now
        self.last_health_log = None
        self.health = None
        # Spread healthy board checks across the minute and preserve backoff on restart.
        self.due = {self.identity(source): max(now + index * interval / len(self.sources),
                    self.state["boards"].get(self.identity(source), {}).get("next_poll_at", 0))
                    for index, source in enumerate(self.sources)}
        self.polls = {}
        self.delivery = None
        self.last_report_at = 0
        self.report_dirty = True
        self.refresh_health(now)

    @staticmethod
    def identity(source):
        return source["provider"] + ":" + source["board"]

    def checkpoint(self):
        save_state(self.state_path, self.state)
        self.report_dirty = True

    def refresh_health(self, now, status="running"):
        worker = {"status": status, "pid": os.getpid(), "started_at": self.worker_started_at,
                  "heartbeat_at": stamp(now), "interval_seconds": self.interval}
        if self.review:
            worker.update(policy_version=self.review.version, policy_error=self.review.error or self.review.projection_error)
        self.health = build_health(self.state, self.config, now, worker=worker)
        save_state(self.health_path, self.health)
        self.last_health_at = now
        self.report_dirty = True

    def dispatch_delivery(self, now):
        if self.sender is not None and self.delivery is None:
            def eligible(event):
                checked = not self.review or self.state["jobs"][event["key"]]["board"] in self.policy_checked_boards
                policy_available = not self.review or not self.review.error
                return policy_available and checked and not posting_age_rejection(event["job"], self.profile, datetime.fromtimestamp(now, timezone.utc))
            event = ready_delivery(self.state, now, eligible=eligible)
            if event is not None:
                snapshot = delivery_snapshot(self.state, event)
                self.delivery = self.delivery_pool.submit(send, self.sender, snapshot), snapshot

    def tick(self, now=None, *, stopping=False):
        # Explicit times keep offline scheduling checks deterministic. At runtime,
        # use a fresh clock after processing so acknowledgements aren't backdated.
        clock = time.time if now is None else lambda: now
        now = clock()
        if self.review:
            self.review.tick(now)
            if self.review.version != self.review_version:
                self.policy_checked_boards.clear()
                self.review_version = self.review.version
                print(f"Alert settings loaded: version {self.review_version}", flush=True)
            self.profile = self.review.profile
        for board, (future, source, started_at) in list(self.polls.items()):
            if not future.done():
                continue
            jobs, error, server_delay, duration = future.result()
            checked_at = stamp(now)
            first_success = not self.state["boards"].get(board, {}).get("initialized") and not error
            self.state = apply_scan(self.state, [(source, jobs, error)], self.profile, checked_at,
                                    baseline_policy=self.baseline_policy,
                                    policy_effective_at=self.review.effective_at if self.review else None)
            if self.review:
                self.review.observe(source, jobs, error, clock(), self.state)
                if not error:
                    self.policy_checked_boards.add(board)
            status = self.state["boards"][board]
            failures = status.get("consecutive_failures", 0) + 1 if error else 0
            delay = max(server_delay, min(900, self.interval * 2 ** min(failures, 6))) if error else self.interval
            due = now + delay if error else max(now, started_at + self.interval)
            status.update(last_attempt=checked_at, consecutive_failures=failures,
                          next_poll_at=due, last_fetch_seconds=round(duration, 3))
            self.due[board] = due
            del self.polls[board]
            self.checkpoint()
            if not stopping:
                # The lead is durable now; don't make it wait behind other
                # completed boards' screening and history writes.
                self.dispatch_delivery(clock())
            if first_success:
                print(baseline_summary(self.state, board), flush=True)
            print(f"Board {board} | Jobs: {len(jobs)} | Error: {error or 'none'} | "
                  f"Pending leads: {len(self.state['pending'])}", flush=True)

        if self.delivery is not None:
            future, event = self.delivery
            if future.done():
                now = clock()
                error = future.result()
                acknowledge_delivery(self.state, event, now, error)
                self.delivery = None
                self.checkpoint()
                print(f"Delivery {event['event_id'][:12]} | "
                      f"{'confirmed' if error is None else 'failed; retained if still active'}", flush=True)
                if error is None:
                    delay = self.state["last_delivery"].get("queued_at")
                    print(f"Delivery timing {event['event_id'][:12]} | Queue-to-confirmation: "
                          f"{elapsed(delay, now)} seconds | Kind: {event['kind']}", flush=True)

        if not stopping:
            now = clock()
            self.dispatch_delivery(now)
            # Never queue unbounded fetches or fetch the same board concurrently.
            for source in sorted(self.sources, key=lambda value: self.due[self.identity(value)]):
                board = self.identity(source)
                if len(self.polls) >= 4:
                    break
                if board not in self.polls and self.due[board] <= now:
                    self.polls[board] = self.poll_pool.submit(retrieve, source, self.fetcher), source, now

        if stopping or now - self.last_health_at >= 15 or self.report_dirty and now - self.last_report_at >= 5:
            self.refresh_health(now, "stopping" if stopping else "running")
            signature = (self.health["status"], tuple(self.health["issues"]))
            if signature != self.last_health_log:
                print(render_health(self.health).splitlines()[0], flush=True)
                self.last_health_log = signature
        if self.report_dirty and (stopping or now - self.last_report_at >= 5):
            self.report_path.parent.mkdir(parents=True, exist_ok=True)
            self.report_path.write_text(report(self.state, stamp(now), health=self.health),
                                        encoding="utf-8")
            self.last_report_at, self.report_dirty = now, False

    def busy(self):
        return bool(self.polls) or self.delivery is not None


def run_watch(config, profile, state_path, report_path, *, interval=60, sender=None, health_path=None):
    from job_monitor.review_runtime import ReviewRuntime
    review = ReviewRuntime.from_environment(profile, state_path)
    stop = threading.Event()
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    for sig in handlers:
        signal.signal(sig, lambda *_: stop.set())
    try:
        with ThreadPoolExecutor(max_workers=4) as polls, ThreadPoolExecutor(max_workers=1) as delivery:
            watcher = Watcher(config, profile, state_path, report_path, polls, delivery,
                              interval=interval, sender=sender, health_path=health_path, review=review)
            print(f"Watching {len(watcher.sources)} boards | Interval: {interval:g}s | "
                  f"Notifications: {'enabled' if sender else 'disabled'}", flush=True)
            while not stop.is_set() or watcher.busy():
                watcher.tick(stopping=stop.is_set())
                # Drain in-flight acknowledgements on shutdown without a busy loop.
                if stop.is_set():
                    time.sleep(0.1)
                else:
                    stop.wait(0.25)
            watcher.tick(stopping=True)
            watcher.refresh_health(time.time(), "stopped")
            print("Watcher stopped; history retained", flush=True)
            return 0
    finally:
        if review:
            review.close()
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
