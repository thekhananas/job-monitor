"""Run the experimental offline evaluator from a checkout."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from job_monitor.rank_jobs import main

if __name__ == "__main__":
    raise SystemExit(main())
