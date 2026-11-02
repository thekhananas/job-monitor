"""Optional bridge: dynamic policy reads and a bounded asynchronous review projection."""
from concurrent.futures import ThreadPoolExecutor
import os
import sqlite3

from job_monitor.review_store import ReviewStore
from job_monitor.sources import PublicationEnricher, fetch


class ReviewRuntime:
    def __init__(self, store, user, profile, *, fetcher=fetch):
        self.store, self.user, self.fetcher = store, str(user), fetcher
        store.initialise(user, profile)
        self.version, self.profile, self.effective_at = store.policy_info(user)
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="review-projection")
        self.future = None
        self.inflight = None
        self.pending = {}
        self.last_check = float("-inf")
        self.error = ""
        self.projection_error = ""
        self.publications = PublicationEnricher()

    @classmethod
    def from_environment(cls, profile, state_path):
        user = os.environ.get("JOB_MONITOR_REVIEW_USER_ID")
        return cls(ReviewStore(state_path.with_name("review.sqlite3")), user, profile) if user else None

    def fetch(self, source):
        return self.publications.enrich(source, self.fetcher(source), self.profile)

    def observe(self, source, jobs, error, now, state):
        board = source["provider"] + ":" + source["board"]
        # Coalesce a delayed projection to the latest observation, bounded by board count.
        self.pending[board] = (source, jobs, error, now,
            {board + ":" + job["id"]: state["jobs"][board + ":" + job["id"]]["first_seen"] for job in jobs})

    def tick(self, now):
        if self.future is not None and self.future.done():
            try:
                self.future.result()
                self.projection_error = ""
            except (OSError, ValueError, sqlite3.Error):
                self.projection_error = "Review projection unavailable"
                board, args = self.inflight
                self.pending.setdefault(board, args)
            self.future = None
        if self.future is None and self.pending:
            board = next(iter(self.pending))
            self.inflight = board, self.pending.pop(board)
            self.future = self.executor.submit(self.store.project, *self.inflight[1])
        if now - self.last_check >= 2:
            self.last_check = now
            try:
                self.version, self.profile, self.effective_at = self.store.policy_info(self.user)
                self.store.worker_status(self.user, self.version, now, self.projection_error)
                self.error = ""
            except (OSError, ValueError, sqlite3.Error):
                self.error = "Saved settings unavailable; retaining last loaded rules"

    def close(self):
        # Projection is a rebuildable view; posting history/outbox already live in state.json.
        self.pending.clear()
        self.executor.shutdown(wait=True)
