"""Run the optional Discord review bot from a checkout."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from job_monitor.review_bot import main

if __name__ == "__main__":
    raise SystemExit(main())
