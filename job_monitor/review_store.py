"""Transactional settings and review records; independent of Discord and job history."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time

from job_monitor.screening import normal_phrase, screen_with_reasons, validate_profile, DEFAULT_EXCLUDED_TITLES
from job_monitor.serialization import encode, fingerprint as signature


class ReviewConflict(ValueError):
    pass


class ReviewStore:
    """SQLite repository. Each operation owns its connection; mutations are atomic."""
    def __init__(self, path, *, read_only=False):
        self.path = Path(path)
        self.read_only = read_only

    @contextmanager
    def connect(self, *, write=False):
        if self.read_only and write:
            raise ValueError("This view is read-only; use Discord to change your alert settings")
        db = (sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)
              if self.read_only else sqlite3.connect(self.path, timeout=0.1))
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def initialise(self, user, profile):
        if self.read_only:
            raise ValueError("Read-only views cannot initialise review storage")
        validate_profile(profile)
        if not str(user).isdigit():
            raise ValueError("Review owner must be a Discord user ID")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            os.close(descriptor)
        self.path.chmod(0o600)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript('''
                CREATE TABLE IF NOT EXISTS policies (
                    user TEXT NOT NULL, version INTEGER NOT NULL, profile TEXT NOT NULL,
                    previous INTEGER, created REAL NOT NULL, PRIMARY KEY(user,version));
                CREATE TABLE IF NOT EXISTS users (user TEXT PRIMARY KEY, version INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS boards (board TEXT PRIMARY KEY, company TEXT NOT NULL,
                    checked REAL NOT NULL, error TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE IF NOT EXISTS jobs (key TEXT PRIMARY KEY, board TEXT NOT NULL,
                    company TEXT NOT NULL, payload TEXT NOT NULL, hash TEXT NOT NULL,
                    first_seen TEXT NOT NULL, active INTEGER NOT NULL);
                CREATE INDEX IF NOT EXISTS jobs_board ON jobs(board,active);
                CREATE TABLE IF NOT EXISTS drafts (token TEXT PRIMARY KEY, user TEXT NOT NULL,
                    base INTEGER NOT NULL, profile TEXT NOT NULL, preview TEXT NOT NULL,
                    created REAL NOT NULL, consumed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS feedback (user TEXT NOT NULL, key TEXT NOT NULL,
                    job_hash TEXT NOT NULL, policy_version INTEGER NOT NULL, action TEXT NOT NULL,
                    created REAL NOT NULL, PRIMARY KEY(user,key,job_hash,policy_version));
                CREATE TABLE IF NOT EXISTS worker (user TEXT PRIMARY KEY, applied INTEGER NOT NULL,
                    heartbeat REAL NOT NULL, error TEXT NOT NULL);
            ''')
        os.chmod(self.path, 0o600)
        with self.connect(write=True) as db:
            if not db.execute("SELECT 1 FROM users WHERE user=?", (str(user),)).fetchone():
                db.execute("INSERT INTO policies VALUES(?,?,?,?,?)", (str(user), 1, encode(profile), None, time.time()))
                db.execute("INSERT INTO users VALUES(?,1)", (str(user),))

    @staticmethod
    def _policy(db, user):
        row = db.execute("SELECT p.* FROM policies p JOIN users u ON u.user=p.user AND u.version=p.version WHERE u.user=?",
                         (str(user),)).fetchone()
        if row is None:
            raise ValueError("User has no saved review settings")
        return row["version"], json.loads(row["profile"])

    def policy(self, user):
        with self.connect() as db:
            return self._policy(db, user)

    def policy_info(self, user):
        with self.connect() as db:
            version, profile = self._policy(db, user)
            created = db.execute("SELECT created FROM policies WHERE user=? AND version=?", (str(user), version)).fetchone()[0]
            return version, profile, created

    def project(self, source, jobs, error, observed, first_seen):
        """Projection only. Never writes state.json or the notification outbox."""
        board = source["provider"] + ":" + source["board"]
        with self.connect(write=True) as db:
            db.execute("INSERT INTO boards(board,company,checked,error) VALUES(?,?,?,?) ON CONFLICT(board) DO UPDATE SET checked=excluded.checked,error=excluded.error,company=excluded.company",
                       (board, source["company"], observed, error))
            if error:
                return
            old = {row["key"]: row["hash"] for row in db.execute("SELECT key,hash FROM jobs WHERE board=?", (board,))}
            seen = set()
            for job in jobs:
                key = board + ":" + job["id"]
                seen.add(key)
                digest = signature(job)
                if old.get(key) != digest:
                    db.execute("INSERT INTO jobs VALUES(?,?,?,?,?,?,1) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,hash=excluded.hash,company=excluded.company,active=1",
                        (key, board, source["company"], encode(job), digest, first_seen[key]))
                else:
                    db.execute("UPDATE jobs SET active=1 WHERE key=? AND active=0", (key,))
            db.executemany("UPDATE jobs SET active=0 WHERE key=? AND active=1", ((key,) for key in old.keys() - seen))

    def set_sources(self, sources):
        with self.connect(write=True) as db:
            db.execute("UPDATE boards SET enabled=0")
            for source in sources:
                board = source["provider"] + ":" + source["board"]
                db.execute("INSERT INTO boards VALUES(?,?,0,'Waiting for first observation',1) ON CONFLICT(board) DO UPDATE SET enabled=1,company=excluded.company",
                           (board, source["company"]))

    def worker_status(self, user, version, now, error=""):
        with self.connect(write=True) as db:
            db.execute("INSERT INTO worker VALUES(?,?,?,?) ON CONFLICT(user) DO UPDATE SET applied=excluded.applied,heartbeat=excluded.heartbeat,error=excluded.error",
                       (str(user), version, now, error))

    def status(self, user):
        with self.connect() as db:
            row = db.execute("SELECT * FROM worker WHERE user=?", (str(user),)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _jobs(db):
        return [dict(row) for row in db.execute("SELECT j.*,b.checked,b.error FROM jobs j JOIN boards b ON b.board=j.board WHERE j.active=1 AND b.enabled=1 ORDER BY j.first_seen DESC,j.key")]

    @staticmethod
    def _evaluate(rows, profile, now):
        current = datetime.fromtimestamp(now, timezone.utc)
        return [(row, *screen_with_reasons(json.loads(row["payload"]), profile, now=current)) for row in rows]

    @classmethod
    def _unreviewed(cls, db, user, evaluated):
        # Reuse a decision across unrelated policy edits, but not changed job content
        # or a changed set of blocking reasons. Historical policies remain immutable.
        current = {row["key"]: (row, reasons) for row, _, reasons in evaluated}
        dismissed = set()
        for decision in db.execute("SELECT f.*,p.profile FROM feedback f JOIN policies p "
                                   "ON p.user=f.user AND p.version=f.policy_version "
                                   "WHERE f.user=? AND f.action IN ('excluded','shown')", (str(user),)):
            item = current.get(decision["key"])
            if item is None or item[0]["hash"] != decision["job_hash"]:
                continue
            previous = cls._evaluate([item[0]], json.loads(decision["profile"]), decision["created"])[0][2]
            if previous == item[1]:
                dismissed.add(decision["key"])
        return [item for item in evaluated if item[0]["key"] not in dismissed]

    def jobs(self, user, *, rejected=False, location=None, days=None, reason=None, title=None,
             offset=0, limit=5, now=None):
        now = time.time() if now is None else now
        with self.connect() as db:
            version, profile = self._policy(db, user)
            if location is not None and not rejected:
                profile = {**profile, "locations": [location], "allow_uk_remote": False}
            if days is not None:
                profile = {**profile, "max_posting_age_days": days}
            validate_profile(profile)
            rows = [(row, match, reasons) for row, match, reasons in self._evaluate(self._jobs(db), profile, now)
                    if (not bool(match)) == rejected]
            if rejected:
                rows = self._unreviewed(db, user, rows)
                if location:
                    rows = [item for item in rows if location.casefold() in json.loads(item[0]["payload"])["location"].casefold()]
                if reason:
                    rows = [item for item in rows if reason in item[2]]
                if title is not None:
                    rows = [item for item in rows if normal_phrase(json.loads(item[0]["payload"])["title"]) == title]
                # Surface near misses before clearly excluded jobs without hiding the latter.
                hard = {"location", "excluded_title", "staff_level", "sponsorship", "posting_age"}
                rows.sort(key=lambda item: sum(code in hard for code in item[2]))
            return rows[offset:offset + limit], len(rows)

    @classmethod
    def _title_suggestions(cls, db, user, now):
        version, profile = cls._policy(db, user)
        rejected = [item for item in cls._evaluate(cls._jobs(db), profile, now) if not item[1]]
        unreviewed = cls._unreviewed(db, user, rejected)
        groups = {}
        for row, _, reasons in unreviewed:
            payload = json.loads(row["payload"])
            phrase = normal_phrase(payload["title"])
            # Explicitly bounded heuristic, not a suitability or confidence score.
            if (reasons != ["title"] or row["error"] or now - row["checked"] > 180
                    or not re.search(r"\b(?:engineer|developer)\b", phrase) or len(phrase) > 200):
                continue
            group = groups.setdefault(phrase, {"phrase": phrase, "title": payload["title"], "jobs": []})
            group["jobs"].append(row)
        for group in groups.values():
            group["evidence"] = signature(sorted((row["key"], row["hash"]) for row in group["jobs"]))
        coverage = list(db.execute("SELECT checked,error FROM boards WHERE enabled=1"))
        return {"version": version, "groups": sorted(groups.values(), key=lambda group: (-len(group["jobs"]), group["phrase"])),
                "unreviewed": len(unreviewed),
                "unknown_dates": sum("posting_date_unknown" in reasons for _, _, reasons in rejected),
                "stale_boards": sum(bool(row["error"]) or now - row["checked"] > 180 for row in coverage)}

    def title_suggestions(self, user, *, now=None):
        with self.connect() as db:
            return self._title_suggestions(db, user, time.time() if now is None else now)

    @classmethod
    def _title_group(cls, db, user, phrase, evidence, version, now):
        suggestions = cls._title_suggestions(db, user, now)
        group = next((group for group in suggestions["groups"] if group["phrase"] == phrase), None)
        if suggestions["version"] != version or group is None or group["evidence"] != evidence:
            raise ReviewConflict("These jobs or your settings changed. Reopen /review for an updated suggestion.")
        return group

    def propose_title(self, user, phrase, evidence, version, *, now=None):
        now = time.time() if now is None else now
        with self.connect(write=True) as db:
            self._title_group(db, user, phrase, evidence, version, now)
            _, profile = self._policy(db, user)
            return self._propose(db, user, {"title_keywords": [*profile["title_keywords"], phrase]}, now)

    def dismiss_title(self, user, phrase, evidence, version, *, now=None):
        now = time.time() if now is None else now
        with self.connect(write=True) as db:
            group = self._title_group(db, user, phrase, evidence, version, now)
            db.executemany("INSERT OR IGNORE INTO feedback VALUES(?,?,?,?,?,?)",
                ((str(user), row["key"], row["hash"], version, "excluded", now) for row in group["jobs"]))

    def job(self, key):
        with self.connect() as db:
            row = db.execute("SELECT j.*,b.checked,b.error FROM jobs j JOIN boards b ON b.board=j.board WHERE key=? AND b.enabled=1", (key,)).fetchone()
            if not row or not row["active"]:
                raise ReviewConflict("This posting is no longer in the last observed active list. Refresh /review.")
            return dict(row)

    @classmethod
    def _preview(cls, db, user, candidate, now):
        version, current = cls._policy(db, user)
        rows = cls._jobs(db)
        before = {row["key"] for row, match, _ in cls._evaluate(rows, current, now) if match}
        after = cls._evaluate(rows, candidate, now)
        eligible = {row["key"] for row, match, _ in after if match}
        coverage = list(db.execute("SELECT checked,error FROM boards WHERE enabled=1"))
        stale = sum(bool(row["error"]) or now - row["checked"] > 180 for row in coverage)
        return {"eligible": len(eligible), "added": sorted(eligible - before), "removed": sorted(before - eligible),
                "unknown_dates": sum("posting_date_unknown" in reasons for _, _, reasons in after),
                "stale_boards": stale,
                "evidence": signature([(row["key"], row["hash"]) for row in rows]), "base": version}

    def propose(self, user, patch, *, now=None):
        now = time.time() if now is None else now
        with self.connect(write=True) as db:
            return self._propose(db, user, patch, now)

    @classmethod
    def _propose(cls, db, user, patch, now):
        allowed = {"locations", "allow_uk_remote", "allow_ambiguous_remote", "max_posting_age_days", "title_keywords", "excluded_title_keywords"}
        if not patch or set(patch) - allowed:
            raise ValueError("Unsupported filter change")
        version, current = cls._policy(db, user)
        candidate = {**current, **patch}
        validate_profile(candidate)
        if candidate == current:
            raise ValueError("These settings are already saved")
        preview = cls._preview(db, user, candidate, now)
        token = secrets.token_hex(12)
        db.execute("DELETE FROM drafts WHERE created<?", (now - 86400,))
        db.execute("INSERT INTO drafts(token,user,base,profile,preview,created) VALUES(?,?,?,?,?,?)",
                   (token, str(user), version, encode(candidate), encode(preview), now))
        return token, candidate, preview

    def draft(self, user, token):
        with self.connect() as db:
            row = db.execute("SELECT * FROM drafts WHERE token=? AND user=? AND consumed=0", (token, str(user))).fetchone()
            if row is None:
                raise ReviewConflict("This preview is unavailable. Open /filters again.")
            return dict(row)

    def cancel(self, user, token):
        with self.connect(write=True) as db:
            db.execute("UPDATE drafts SET consumed=1 WHERE token=? AND user=?", (token, str(user)))

    def confirm(self, user, token, *, now=None):
        now = time.time() if now is None else now
        with self.connect(write=True) as db:
            draft = db.execute("SELECT * FROM drafts WHERE token=? AND user=? AND consumed=0", (token, str(user))).fetchone()
            version, current = self._policy(db, user)
            if not draft or draft["base"] != version or now - draft["created"] > 600:
                raise ReviewConflict("This preview expired or your settings changed. Preview again.")
            candidate = json.loads(draft["profile"])
            preview = self._preview(db, user, candidate, now)
            previous = json.loads(draft["preview"])
            if any(preview[key] != previous[key] for key in ("evidence", "added", "removed", "unknown_dates")):
                raise ReviewConflict("The affected jobs changed. Preview again before saving.")
            new_version = version + 1
            db.execute("INSERT INTO policies VALUES(?,?,?,?,?)", (str(user), new_version, encode(candidate), version, now))
            db.execute("UPDATE users SET version=? WHERE user=?", (new_version, str(user)))
            db.execute("UPDATE drafts SET consumed=1 WHERE token=?", (token,))
            return new_version

    def undo(self, user, expected_version, *, now=None):
        with self.connect() as db:
            version, _ = self._policy(db, user)
            if version != expected_version:
                raise ReviewConflict("Settings changed since this message. Open /filters again.")
            parent = db.execute("SELECT previous FROM policies WHERE user=? AND version=?", (str(user), version)).fetchone()[0]
            if parent is None:
                raise ValueError("No previous settings to restore")
            profile = json.loads(db.execute("SELECT profile FROM policies WHERE user=? AND version=?", (str(user), parent)).fetchone()[0])
            _, current = self._policy(db, user)
        return self.propose(user, {key: profile.get(key, {"locations": ["London"], "max_posting_age_days": None,
                    "allow_uk_remote": True, "allow_ambiguous_remote": False,
                    "excluded_title_keywords": DEFAULT_EXCLUDED_TITLES}.get(key))
                    for key in ("locations", "max_posting_age_days", "allow_uk_remote", "allow_ambiguous_remote", "title_keywords", "excluded_title_keywords")}, now=now)

    def feedback(self, user, key, digest, version, action, *, now=None):
        if action not in ("excluded", "shown"):
            raise ValueError("Unknown review action")
        with self.connect(write=True) as db:
            active, _ = self._policy(db, user)
            row = db.execute("SELECT j.hash,j.active FROM jobs j JOIN boards b ON b.board=j.board WHERE key=? AND b.enabled=1", (key,)).fetchone()
            if active != version or row is None or not row["active"] or row["hash"] != digest:
                raise ReviewConflict("The posting or settings changed. Refresh /review.")
            db.execute("INSERT OR IGNORE INTO feedback VALUES(?,?,?,?,?,?)", (str(user), key, digest, version, action,
                                                                         time.time() if now is None else now))
