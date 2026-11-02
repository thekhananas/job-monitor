"""Offline ranking command: reads local inputs, prints results, never fetches or saves."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys

from job_monitor.prioritization import Evaluator, SCORER_VERSION, ordering
from job_monitor.review_store import ReviewStore
from job_monitor.screening import screen_with_reasons


MAX_POSTINGS = 5000
MAX_INPUT_BYTES = 25_000_000


def read_json(path):
    with Path(path).open('rb') as stream:
        contents = stream.read(MAX_INPUT_BYTES + 1)
    if len(contents) > MAX_INPUT_BYTES:
        raise ValueError("Local input exceeds the 25 MB work limit; use a smaller calibration set")
    return json.loads(contents)


def input_postings(path):
    payload = read_json(path)
    if not isinstance(payload, dict) or not isinstance(payload.get('jobs'), list):
        raise ValueError("Job input must be an object containing a jobs list")
    if len(payload['jobs']) > MAX_POSTINGS:
        raise ValueError("At most 5000 postings can be assessed in one offline run")
    result = []
    keys = set()
    for item in payload['jobs']:
        if not isinstance(item, dict) or not isinstance(item.get('job'), dict):
            raise ValueError("Each input posting needs company and job fields")
        job = item['job']
        identity = (item.get('company'), job.get('url'))
        if not all(isinstance(value, str) for value in identity) or identity in keys:
            raise ValueError("Posting employer and URL must be text and uniquely identify each input")
        keys.add(identity)
        result.append({'company': item['company'], 'job': job, 'screening': {
            'eligible': None, 'reasons': [], 'note': 'Live location/eligibility filters not evaluated for manual input'}})
    return result, {'kind': 'local_json', 'screening': 'not_checked',
                    'note': 'Public descriptions and employer labels supplied locally; employer identity is not independently verified'}


def database_postings(path, *, user=None, include_screened_out=False, now=None):
    """Take a bounded read snapshot and release it before doing any scoring."""
    now = now or datetime.now(timezone.utc)
    store = ReviewStore(path, read_only=True)
    with store.connect() as db:
        db.execute('BEGIN')
        if not user:
            users = db.execute('SELECT user FROM users LIMIT 2').fetchall()
            if len(users) != 1:
                raise ValueError("Choose --user when saved profiles do not identify one owner")
            user = users[0]['user']
        row = db.execute('SELECT p.version,p.profile FROM policies p JOIN users u '
                         'ON p.user=u.user AND p.version=u.version WHERE u.user=?', (str(user),)).fetchone()
        if row is None:
            raise ValueError("No saved filter policy for the selected user")
        version, policy = row['version'], json.loads(row['profile'])
        size = db.execute('SELECT COUNT(*) AS count,COALESCE(SUM(length(CAST(j.payload AS BLOB))),0) AS bytes '
                          'FROM jobs j JOIN boards b ON b.board=j.board '
                          'WHERE j.active=1 AND b.enabled=1').fetchone()
        count = size['count']
        if count > MAX_POSTINGS:
            raise ValueError("Active saved postings exceed the 5000-job work limit; use a smaller calibration export")
        if size['bytes'] > MAX_INPUT_BYTES:
            raise ValueError("Active saved descriptions exceed the 25 MB work limit; use a smaller calibration export")
        rows = db.execute('SELECT j.*,b.checked,b.error FROM jobs j JOIN boards b ON b.board=j.board '
                          'WHERE j.active=1 AND b.enabled=1').fetchall()
        worker = db.execute('SELECT applied,heartbeat,error FROM worker WHERE user=?', (str(user),)).fetchone()
    result = []
    for row in rows:
        job = json.loads(row['payload'])
        match, reasons = screen_with_reasons(job, policy, now=now)
        if not match and not include_screened_out:
            continue
        result.append({'company': row['company'], 'job': job,
                       'screening': {'eligible': bool(match), 'reasons': reasons, 'policy_version': version},
                       'observation': {'checked_at': row['checked'], 'source_error': row['error'],
                                       'stale': bool(row['error']) or now.timestamp() - row['checked'] > 180}})
    loaded = bool(worker and worker['applied'] == version and not worker['error']
                  and 0 <= now.timestamp() - worker['heartbeat'] <= 30)
    return result, {'kind': 'review_database', 'policy_version': version, 'loaded_by_worker': loaded,
                    'includes_screened_out': include_screened_out,
                    'note': 'Saved public observations, not a fresh vacancy check. Ranking does not change filter eligibility.'}


def rank(postings, evaluator, *, now=None):
    results = []
    for record in postings:
        result = evaluator.assess(record['job'], record['company'], now=now)
        result['screening'] = record['screening']
        if 'observation' in record:
            result['observation'] = record['observation']
        results.append(result)
    candidates = sorted((item for item in results if item['scope'] != 'out_of_scope'
                         and item['screening']['eligible'] is not False), key=ordering)
    excluded = [item for item in results if item['scope'] == 'out_of_scope'
                or item['screening']['eligible'] is False]
    return {'ranked': candidates, 'excluded_from_shortlist': excluded,
            'assessed_count': len(results), 'quality_status': 'experimental_not_calibrated'}


def clean(value):
    return ''.join(char for char in str(value) if char in '\n\t' or char.isprintable())


def render(report, limit):
    lines = ['Experimental job priorities — not calibrated against your decisions', report['source']['note'],
             f"Assessed: {report['assessed_count']} | Shortlist: {len(report['ranked'])} | Outside shortlist: {len(report['excluded_from_shortlist'])}",
             'Interests/listed skills are not demonstrated experience. Scores are not qualification probabilities.']
    if report['source']['kind'] == 'review_database':
        lines.append(f"Saved filter version: {report['source']['policy_version']} | Worker activation: " +
                     ('confirmed loaded' if report['source']['loaded_by_worker'] else 'unconfirmed'))
    for number, item in enumerate(report['ranked'][:limit], 1):
        job = item['job']
        lines.extend(['', f"{number}. {job['title']} — {item['company']} | {item['priority']}",
                      job['location'], f"First published: {item['original_publication'] or 'Unknown'}", job['url']])
        if item['scope'] == 'review':
            lines.append('Scope needs review; title/team context is not established.')
        if item['screening']['eligible'] is None:
            lines.append(item['screening']['note'])
        if item.get('observation', {}).get('stale'):
            lines.append('Saved source observation is stale or failed; verify the official listing.')
        for strength in item['demonstrated_fit'][:3]:
            hit = strength['matches'][0]
            lines.append(f"Evidence [{strength['capability_id']}; {strength['context']}]: {strength['candidate_evidence']}")
            lines.append(f"  Advert: {hit['excerpt']}")
        for gap in item['gaps'][:3]:
            lines.append(f"Check {gap['term']}: {gap['status']} | {gap['excerpt']}")
        if item['career_direction']:
            lines.append('Interests: ' + ', '.join(value['interest'] for value in item['career_direction']))
        if item['listed_skills']:
            lines.append('Listed-only skills: ' + ', '.join(value['term'] for value in item['listed_skills']))
        if item['weaker_evidence']:
            lines.append('Coursework/listed evidence: ' + ', '.join(value['name'] for value in item['weaker_evidence']))
        lines.extend('Uncertain: ' + note for note in item['uncertainties'])
    if report['excluded_from_shortlist']:
        lines.append('\nOutside shortlist — preferences or existing screening; no alert rules changed:')
        for item in report['excluded_from_shortlist'][:limit]:
            reasons = [value['code'] + ': ' + value['excerpt'] for value in item['scope_conflicts']]
            reasons.extend(item['screening'].get('reasons', []))
            lines.append(f"{item['job']['title']} — {item['company']} | " + '; '.join(reasons))
    lines.append('\nUse --format json for complete evidence and all assessed jobs; output is private candidate-derived data.')
    return clean('\n'.join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Rank saved public jobs offline; no fetching, saving, alerts or policy changes')
    parser.add_argument('--candidate', type=Path, default=Path('candidate-profile.json'))
    source = parser.add_mutually_exclusive_group()
    source.add_argument('--input', type=Path, help='Local JSON: {"jobs": [{"company": "...", "job": {...}}]}')
    source.add_argument('--database', type=Path, help='Review DB, read-only; default review.sqlite3')
    parser.add_argument('--user', help='Saved filter owner; defaults to configured owner or sole saved user')
    parser.add_argument('--include-screened-out', action='store_true', help='Audit excluded DB jobs separately; never make them alert-eligible')
    parser.add_argument('--check-profile', action='store_true', help='Validate candidate profile without reading job data')
    parser.add_argument('--limit', type=int, default=10, help='Text display limit; JSON includes all assessed jobs')
    parser.add_argument('--format', choices=('text', 'json'), default='text')
    args = parser.parse_args(argv)
    if args.limit < 1 or args.limit > MAX_POSTINGS:
        parser.error('--limit must be between 1 and 5000')
    if args.input and args.include_screened_out:
        parser.error('--include-screened-out applies only to --database')
    try:
        evaluator = Evaluator(read_json(args.candidate))
        if args.check_profile:
            print(f"Candidate profile revision {evaluator.profile['profile_revision']} is structurally valid; job fit is not evaluated.")
            return 0
        if args.input:
            postings, origin = input_postings(args.input)
        else:
            postings, origin = database_postings(args.database or Path('review.sqlite3'),
                user=args.user or os.environ.get('JOB_MONITOR_REVIEW_USER_ID'),
                include_screened_out=args.include_screened_out)
        report = rank(postings, evaluator)
        report.update(source=origin, scorer_version=SCORER_VERSION, profile_hash=evaluator.profile_hash,
                      profile_revision=evaluator.profile['profile_revision'])
        print(json.dumps(report, ensure_ascii=True, indent=2) if args.format == 'json' else render(report, args.limit))
        return 0
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        message = 'Saved review database unavailable; provide --input with public descriptions or an existing --database' if isinstance(error, sqlite3.Error) else str(error)
        print('Ranking unavailable: ' + clean(message), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
