"""Generate and validate sources.json from a simple CSV table."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import argparse
import csv
import io
import json
import re

from job_monitor.monitor import validate_sources
from job_monitor.sources import fetch


SUPPORTED_PROVIDERS = ("greenhouse", "lever", "ashby")
BOARD_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
TRUE_VALUES = {"", "1", "true", "yes", "y"}
FALSE_VALUES = {"0", "false", "no", "n"}


def parse_bool(value, *, row_num):
    normalized = (value or "").strip().casefold()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"Row {row_num}: invalid enabled value {value!r}; use true or false")


def parse_sources_csv(csv_text, *, default_baseline_policy="suppress"):
    """Parse CSV text into a validated sources.json config dict."""
    if default_baseline_policy not in ("suppress", "notify"):
        raise ValueError("baseline_policy must be suppress or notify")
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("CSV file is empty or missing a header row")
    headers = {name.strip() for name in reader.fieldnames if name is not None}
    missing = {"provider", "board", "company"} - headers
    if missing:
        raise ValueError("CSV missing required column(s): " + ", ".join(sorted(missing)))

    sources = []
    for row_num, raw_row in enumerate(reader, start=2):
        row = {(key.strip() if key is not None else ""): (value.strip() if value is not None else "")
               for key, value in raw_row.items()}
        if not any(row.values()):
            continue
        provider = row.get("provider", "").casefold()
        board = row.get("board", "")
        company = row.get("company", "")
        if provider not in SUPPORTED_PROVIDERS:
            raise ValueError(f"Row {row_num}: unsupported provider {provider!r}; "
                             f"choose from {', '.join(SUPPORTED_PROVIDERS)}")
        if not BOARD_PATTERN.fullmatch(board):
            raise ValueError(f"Row {row_num}: invalid board slug {board!r}")
        if not company:
            raise ValueError(f"Row {row_num}: company is required")
        enabled = parse_bool(row.get("enabled", ""), row_num=row_num)
        policy = row.get("baseline_policy", "").casefold()
        if policy and policy not in ("suppress", "notify"):
            raise ValueError(f"Row {row_num}: baseline_policy must be suppress or notify")

        entry = {"provider": provider, "board": board, "company": company}
        if not enabled:
            entry["enabled"] = False
        if policy:
            entry["baseline_policy"] = policy
        sources.append(entry)

    config = {"version": 1, "baseline_policy": default_baseline_policy, "sources": sources}
    validate_sources(config)
    return config


def render_sources_json(config):
    """Render deterministic, diff-friendly JSON for review."""
    return json.dumps(config, indent=2, ensure_ascii=False) + "\n"


def render_sources_csv(config):
    """Export an existing sources.json config dict to CSV text."""
    validate_sources(config)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["provider", "board", "company", "enabled", "baseline_policy"])
    for source in config["sources"]:
        enabled = "true" if source.get("enabled", True) else "false"
        writer.writerow([
            source["provider"],
            source["board"],
            source["company"],
            enabled,
            source.get("baseline_policy", ""),
        ])
    return buffer.getvalue()


def check_live_sources(config, *, fetcher=fetch):
    """Probe each enabled board and return a list of (board_id, error) failures."""
    sources = [source for source in config["sources"] if source.get("enabled", True)]
    failures = []

    def probe(source):
        board_id = f"{source['provider']}:{source['board']}"
        try:
            fetcher(source)
            return board_id, ""
        except Exception as exc:
            detail = type(exc).__name__ + (f" HTTP {exc.code}" if hasattr(exc, "code") else "")
            return board_id, detail

    with ThreadPoolExecutor(max_workers=min(4, len(sources))) as executor:
        futures = [executor.submit(probe, source) for source in sources]
        for future in as_completed(futures):
            board_id, error = future.result()
            if error:
                failures.append((board_id, error))
    return sorted(failures)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate and validate sources.json from a CSV file")
    parser.add_argument("--csv", type=Path, default=Path("sources.csv"),
                        help="Input CSV path (default: sources.csv)")
    parser.add_argument("--out", type=Path, default=Path("sources.json"),
                        help="Output JSON path (default: sources.json)")
    parser.add_argument("--baseline-policy", choices=("suppress", "notify"), default="suppress",
                        help="Default baseline_policy for generated config (default: suppress)")
    parser.add_argument("--check", action="store_true",
                        help="Verify --out matches --csv without writing; exit 1 if out of sync")
    parser.add_argument("--check-live", action="store_true",
                        help="Fetch each enabled board to catch 404s or invalid slugs before writing")
    parser.add_argument("--export-csv", action="store_true",
                        help="Export an existing --out JSON file into --csv to bootstrap CSV management")
    args = parser.parse_args(argv)

    if args.export_csv:
        try:
            config = json.loads(args.out.read_text(encoding="utf-8"))
            csv_text = render_sources_csv(config)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            print(f"Sources JSON invalid: {exc}")
            return 1
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        args.csv.write_text(csv_text, encoding="utf-8")
        print(f"Exported {args.out} to {args.csv} ({len(config['sources'])} sources)")
        return 0

    try:
        config = parse_sources_csv(args.csv.read_text(encoding="utf-8"),
                                   default_baseline_policy=args.baseline_policy)
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        print(f"Sources CSV invalid: {exc}")
        return 1

    if args.check_live:
        failures = check_live_sources(config)
        if failures:
            for board_id, error in failures:
                print(f"Live board check failed: {board_id} ({error})")
            return 1
        print(f"Live board check passed for {len(validate_sources(config))} enabled boards")

    rendered = render_sources_json(config)
    if args.check:
        try:
            current = json.loads(args.out.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(f"Sources JSON check failed: {exc}")
            return 1
        if current != config:
            print(f"{args.out} is out of sync with {args.csv}; run scripts/sources_from_csv.py to regenerate")
            return 1
        print(f"{args.out} is up to date with {args.csv} ({len(config['sources'])} sources)")
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(rendered, encoding="utf-8")
    enabled_count = sum(1 for source in config["sources"] if source.get("enabled", True))
    print(f"Wrote {args.out} ({len(config['sources'])} total, {enabled_count} enabled)")
    return 0
