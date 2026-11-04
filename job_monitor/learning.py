"""Private explicit preference labels. No training, policy changes or notifications."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from job_monitor.prioritization import validate_candidate
from job_monitor.review_store import ReviewConflict, ReviewStore, encode, signature


TARGET = 'consider_applying_v2'
QUESTION = 'Would you consider applying?'
REASONS = {
    'fit': 'Good fit', 'interest': 'Career interest', 'seniority': 'Seniority',
    'cohort': 'Graduate cohort', 'location': 'Location', 'experience': 'Experience gap',
    'not_interested': 'Not interested'
}


def context_id(user, snapshot):
    """Observation time/selection origin do not turn one decision into many examples."""
    return signature({'user': str(user), 'job_key': snapshot['job_key'],
                      'job_hash': snapshot['job_hash'], 'company': snapshot['company'],
                      'candidate': snapshot['candidate']['hash'],
                      'policy': snapshot['screening']['hash'], 'target': TARGET})


def validate_snapshot(user, snapshot):
    if not re.fullmatch(r'[1-9][0-9]*', str(user)) or not isinstance(snapshot, dict):
        raise ValueError('Invalid preference owner or snapshot')
    if snapshot.get('user') != str(user) or snapshot.get('target') != TARGET:
        raise ValueError('This preference context belongs to another owner or target')
    if not isinstance(snapshot.get('job_key'), str) or not snapshot['job_key']:
        raise ValueError('Preference context needs a job key')
    if not isinstance(snapshot.get('company'), str) or not isinstance(snapshot.get('job'), dict):
        raise ValueError('Preference context needs a public job snapshot')
    if signature(snapshot['job']) != snapshot.get('job_hash'):
        raise ValueError('Job snapshot changed; reopen the job')
    screening = snapshot.get('screening', {})
    candidate = snapshot.get('candidate', {})
    if (not isinstance(screening, dict) or not isinstance(screening.get('profile'), dict)
            or signature(screening['profile']) != screening.get('hash')):
        raise ValueError('Invalid screening context')
    if not isinstance(candidate, dict):
        raise ValueError('Invalid candidate context')
    if candidate.get('status') == 'available':
        if (not isinstance(candidate.get('profile'), dict)
                or signature(candidate['profile']) != candidate.get('hash')
                or candidate['profile'].get('profile_revision') != candidate.get('revision')):
            raise ValueError('Candidate snapshot changed; reopen the job')
    elif (candidate.get('status') != 'not_configured' or candidate.get('hash') is not None
          or candidate.get('revision') is not None or candidate.get('profile') is not None):
        raise ValueError('Candidate context must be explicit or marked unavailable')
    if len(encode(snapshot).encode('utf-8')) > 1_000_000:
        raise ValueError('Preference snapshot exceeds the 1 MB storage limit')
    return context_id(user, snapshot)


class LearningStore:
    """Append-only revisions; the current head is one effective label per context."""
    def __init__(self, path):
        self.path = Path(path)

    @staticmethod
    def _validate_database(db, *, allow_empty=False):
        version = db.execute('PRAGMA user_version').fetchone()[0]
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if allow_empty and version == 0 and not tables:
            return False
        if version != 1 or tables != {'snapshots', 'decisions'}:
            raise ValueError('Unsupported learning database; preserve it and inspect the configured path')
        return True

    @contextmanager
    def _write(self, *, create=True):
        if not self.path.exists():
            if not create:
                raise ReviewConflict('No decision to undo')
            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(descriptor)
            except FileExistsError:
                pass
        db = sqlite3.connect(self.path, timeout=0.2)
        db.row_factory = sqlite3.Row
        try:
            self._validate_database(db, allow_empty=True)
            self.path.chmod(0o600)
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS snapshots (hash TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user TEXT NOT NULL, context TEXT NOT NULL,
                snapshot_hash TEXT NOT NULL REFERENCES snapshots(hash),
                choice TEXT CHECK(choice IN ('yes','no') OR choice IS NULL),
                reason TEXT, kind TEXT NOT NULL CHECK(kind IN ('decision','undo')),
                request TEXT NOT NULL, created REAL NOT NULL, UNIQUE(user,request))''')
            db.execute('CREATE INDEX IF NOT EXISTS decisions_context ON decisions(user,context,id DESC)')
            db.execute('PRAGMA user_version=1')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _head(db, user, context):
        row = db.execute('SELECT * FROM decisions WHERE user=? AND context=? ORDER BY id DESC LIMIT 1',
                         (str(user), context)).fetchone()
        return dict(row) if row else None

    def latest(self, user, snapshot):
        context = validate_snapshot(user, snapshot)
        if not self.path.exists():
            return None
        db = sqlite3.connect(self.path.resolve().as_uri() + '?mode=ro', uri=True, timeout=0.2)
        db.row_factory = sqlite3.Row
        try:
            if not self._validate_database(db, allow_empty=True):
                # An interrupted first transaction may leave a private empty
                # database. Keep the card usable so an explicit retry can finish.
                return None
            return self._head(db, user, context)
        finally:
            db.close()

    @staticmethod
    def _request(request, expected_id):
        if not isinstance(request, str) or not re.fullmatch(r'[A-Za-z0-9:_-]{1,100}', request):
            raise ValueError('Invalid preference request ID')
        if expected_id is not None and (type(expected_id) is not int or expected_id < 1):
            raise ValueError('Invalid decision revision')

    @staticmethod
    def _append(db, user, context, snapshot, choice, reason, kind, request):
        digest = signature(snapshot)
        db.execute('INSERT OR IGNORE INTO snapshots VALUES(?,?)', (digest, encode(snapshot)))
        db.execute('INSERT INTO decisions(user,context,snapshot_hash,choice,reason,kind,request,created) VALUES(?,?,?,?,?,?,?,?)',
                   (str(user), context, digest, choice, reason, kind, request, time.time()))
        return LearningStore._head(db, user, context)

    def record(self, user, snapshot, choice, request, *, expected_id=None, reason=None):
        context = validate_snapshot(user, snapshot)
        self._request(request, expected_id)
        if choice not in ('yes', 'no', None) or reason is not None and reason not in REASONS:
            raise ValueError('Choose Yes, No or an explicit withdrawal; Skip is not a label')
        if choice is None and reason is not None:
            raise ValueError('An unlabeled role cannot have a preference reason')
        with self._write() as db:
            receipt = db.execute('SELECT * FROM decisions WHERE user=? AND request=?', (str(user), request)).fetchone()
            if receipt:
                if (receipt['context'], receipt['choice'], receipt['reason'], receipt['kind']) != (context, choice, reason, 'decision'):
                    raise ReviewConflict('This request already recorded a different decision; reopen the job')
                return self._head(db, user, context)
            head = self._head(db, user, context)
            if (head['id'] if head else None) != expected_id:
                raise ReviewConflict('Your decision changed in another view. Reopen the job before correcting it.')
            if head and (head['choice'], head['reason']) == (choice, reason):
                return head
            if not head and choice is None:
                return None
            return self._append(db, user, context, snapshot, choice, reason, 'decision', request)

    def undo(self, user, snapshot, request, *, expected_id):
        context = validate_snapshot(user, snapshot)
        self._request(request, expected_id)
        if expected_id is None:
            raise ReviewConflict('No decision to undo')
        with self._write(create=False) as db:
            receipt = db.execute('SELECT * FROM decisions WHERE user=? AND request=?', (str(user), request)).fetchone()
            if receipt:
                if receipt['context'] != context or receipt['kind'] != 'undo':
                    raise ReviewConflict('This request already recorded a different action')
                return self._head(db, user, context)
            head = self._head(db, user, context)
            if not head or head['id'] != expected_id or head['kind'] != 'decision':
                raise ReviewConflict('The decision changed or was already undone. Reopen the job.')
            previous = db.execute('SELECT choice,reason FROM decisions WHERE user=? AND context=? AND id<? ORDER BY id DESC LIMIT 1',
                                  (str(user), context, head['id'])).fetchone()
            return self._append(db, user, context, snapshot,
                                previous['choice'] if previous else None,
                                previous['reason'] if previous else None, 'undo', request)


class LearningService:
    """Bind explicit decisions to the observed job, candidate and screening context."""
    def __init__(self, review, labels, *, candidate_path=None):
        self.review, self.labels = review, labels
        self.candidate_path = Path(candidate_path) if candidate_path is not None else None

    def candidate(self):
        if self.candidate_path is None or not self.candidate_path.exists():
            return {'status': 'not_configured', 'hash': None, 'revision': None, 'profile': None}
        try:
            with self.candidate_path.open('rb') as stream:
                contents = stream.read(256_001)
            if len(contents) > 256_000:
                raise ValueError('Candidate file too large')
            profile = json.loads(contents)
            validate_candidate(profile)
        except (OSError, ValueError, TypeError, KeyError) as error:
            raise ValueError('Candidate profile cannot be read for labeling; fix it or reopen after configuration') from error
        return {'status': 'available', 'hash': signature(profile),
                'revision': profile['profile_revision'], 'profile': profile}

    def capture(self, user, record, version, policy, *, selection, require_candidate=False):
        snapshot = {'user': str(user), 'target': TARGET, 'question': QUESTION,
                    'job_key': record['key'], 'job_hash': record['hash'],
                    'company': record['company'], 'board': record['board'],
                    'job': json.loads(record['payload']), 'candidate': self.candidate(),
                    'screening': {'version': version, 'hash': signature(policy), 'profile': policy},
                    'selection': selection, 'observation': {'checked': record['checked'], 'source_error': record['error']},
                    'prediction_shown': None}
        if require_candidate and snapshot['candidate']['status'] != 'available':
            raise ValueError('Rating is unavailable until your candidate profile is configured.')
        validate_snapshot(user, snapshot)
        return snapshot

    def _check_current(self, user, snapshot):
        validate_snapshot(user, snapshot)
        with ReviewStore(self.review.path, read_only=True).connect() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT j.* FROM jobs j JOIN boards b ON b.board=j.board WHERE j.key=? AND j.active=1 AND b.enabled=1',
                             (snapshot['job_key'],)).fetchone()
            policy = db.execute('SELECT p.profile FROM policies p JOIN users u ON p.user=u.user AND p.version=u.version WHERE u.user=?',
                                (str(user),)).fetchone()
        if (row is None or row['hash'] != snapshot['job_hash'] or row['company'] != snapshot['company']
                or policy is None or signature(json.loads(policy['profile'])) != snapshot['screening']['hash']
                or self.candidate()['hash'] != snapshot['candidate']['hash']):
            raise ReviewConflict('The posting or your preferences changed. Reopen Rate this job, /jobs or /review before labeling.')

    def decide(self, user, snapshot, choice, request, *, expected_id=None, reason=None):
        self._check_current(user, snapshot)
        return self.labels.record(user, snapshot, choice, request, expected_id=expected_id, reason=reason)

    def undo(self, user, snapshot, request, *, expected_id):
        self._check_current(user, snapshot)
        return self.labels.undo(user, snapshot, request, expected_id=expected_id)
