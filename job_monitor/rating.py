"""Resolve a selected alert to a saved job without network access or label writes."""
import json
import re
import urllib.parse

from job_monitor.monitor import canonical_url
from job_monitor.notifications import discord_text
from job_monitor.learning import QUESTION
from job_monitor.review_store import ReviewConflict, ReviewStore
from job_monitor.screening import screen


def posting_url(value):
    if not isinstance(value, str) or len(value) > 2000:
        raise ValueError('Unsupported job link')
    parts = urllib.parse.urlsplit(value)
    if parts.scheme != 'https' or not parts.hostname or parts.username or parts.password:
        raise ValueError('Unsupported job link')
    value = re.sub(r'[\s<>\\]', lambda item: urllib.parse.quote(item.group(), safe=''), value)
    # Normalize unreserved escapes only; encoded slashes must keep their meaning.
    def escape(item):
        character = chr(int(item.group()[1:], 16))
        return character if re.fullmatch(r'[A-Za-z0-9_.~-]', character) else item.group().upper()
    return canonical_url(re.sub(r'%[0-9a-fA-F]{2}', escape, value))


def resolve_alert(store, content):
    if not isinstance(content, str) or len(content) > 4000:
        raise ValueError('Choose a job alert with a View job & apply link.')
    links = re.findall(r'^\[View job & apply\]\(<(https://[^<>\r\n]+)>\)$', content, re.MULTILINE)
    if len(links) != 1:
        raise ValueError('Choose one job alert with a View job & apply link.')
    selected = posting_url(links[0])
    matches = []
    with ReviewStore(store.path, read_only=True).connect() as db:
        if db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0] > 20000:
            raise ValueError('Saved job review is too large to resolve safely. Use /jobs instead.')
        for row in db.execute('SELECT j.*,b.checked,b.error,b.enabled FROM jobs j JOIN boards b ON b.board=j.board'):
            job = json.loads(row['payload'])
            if posting_url(job['url']) == selected:
                matches.append(dict(row))
    if not matches:
        raise ReviewConflict('Job details are not available yet. Try again shortly, or use the original application link.')
    if len(matches) != 1:
        raise ReviewConflict('This link matches more than one saved posting. Open /jobs to choose the role.')
    record = matches[0]
    if not record['active'] or not record['enabled']:
        raise ReviewConflict('This posting is no longer in the active saved list. Check the original application link.')
    return record


def rating_text(record, profile, *, now, decision=None, unavailable=None):
    job = json.loads(record['payload'])
    eligibility = screen(job, profile) or {'concerns': [], 'sponsorship': 'unknown'}
    text = discord_text({'company': record['company'], 'job': job,
                         'kind': 'Newly discovered', 'screening': eligibility,
                         'source_check_failed': bool(record['error'])}, now=now)
    suffix = ''
    if not record['error'] and now - record['checked'] > 180:
        suffix += '\nSaved observation is stale; verify the listing is still open.'
    if unavailable:
        suffix += '\n\n' + unavailable
    else:
        suffix += '\n\n' + QUESTION
        if decision and decision['choice']:
            suffix += '\nSaved: **' + decision['choice'].capitalize() + '**'
    # The existing renderer reserves the full official URL. Refuse oversized
    # additions rather than truncating that destination or a saved-state notice.
    if len((text + suffix).encode('utf-16-le')) // 2 > 1900:
        raise ValueError('Job details are too long for a rating card. Use the original application link.')
    return text + suffix
