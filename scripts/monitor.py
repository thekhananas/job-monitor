"""Run the monitor from a checkout without installing the package."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from job_monitor.monitor import main

if __name__ == "__main__":
    raise SystemExit(main())
