"""One-command updates for the existing VM; no credentials or runtime state shipped."""
from contextlib import closing
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time


FILES = (
    'job_monitor/__init__.py',
    'job_monitor/cli_menu.py',
    'job_monitor/health.py',
    'job_monitor/learning.py',
    'job_monitor/monitor.py',
    'job_monitor/notifications.py',
    'job_monitor/prioritization.py',
    'job_monitor/rank_jobs.py',
    'job_monitor/review_bot.py',
    'job_monitor/review_runtime.py',
    'job_monitor/review_store.py',
    'job_monitor/retry.py',
    'job_monitor/screening.py',
    'job_monitor/serialization.py',
    'job_monitor/sources.py',
    'job_monitor/watch.py',
    'scripts/monitor.py',
    'scripts/review_bot.py',
    'scripts/rank_jobs.py',
    'tests/__init__.py',
    'tests/test_monitor.py',
    'README.md',
    'pyproject.toml',
    'examples/prioritization-jobs.example.json',
    'deployment/job-monitor-review.service',
    'deployment/job-monitor.service',
    'deployment/deploy.py',
    'deployment/deploy.sh',
)
CONFIG = ("sources.json", "profile.json")
SERVICE = "job-monitor.service"
MANIFEST = "release.json"


class DeploymentError(RuntimeError):
    pass


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def release_id(files, update_config):
    data = json.dumps({"files": files, "update_config": update_config}, sort_keys=True).encode()
    return hashlib.sha256(data).hexdigest()


def make_bundle(project, stage, *, interval=None, update_config=False):
    """Explicit allowlist: never walk the workspace or include state/key/env files."""
    project = project.resolve()
    names = FILES + (CONFIG if update_config else ())
    hashes = {}
    for name in names:
        source = project / name
        if source.is_symlink() or not source.is_file() or source.resolve().parent != (project / name).parent.resolve():
            raise DeploymentError(f"Expected a regular project file: {name}")
        # Reject symlinked parent directories as well as symlinked files.
        if any(parent.is_symlink() for parent in source.parents if parent != project.parent):
            raise DeploymentError(f"Symlinked release path: {name}")
        destination = stage / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        if name == "deployment/job-monitor.service" and interval is not None:
            value, count = re.subn(r"--interval\s+\d+(?:\.\d+)?", f"--interval {interval:g}", destination.read_text())
            if count != 1:
                raise DeploymentError("Expected one polling interval in the service template")
            destination.write_text(value)
        hashes[name] = digest(destination)
    manifest = {"version": 1, "files": hashes, "update_config": update_config,
                "release_id": release_id(hashes, update_config)}
    (stage / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def read_manifest(stage):
    manifest = json.loads((stage / MANIFEST).read_text())
    if (manifest.get("version") != 1 or type(manifest.get("update_config")) is not bool
            or not isinstance(manifest.get("files"), dict)):
        raise DeploymentError("Invalid release manifest")
    expected = set(FILES + (CONFIG if manifest["update_config"] else ()))
    if set(manifest["files"]) != expected:
        raise DeploymentError("Release must contain exactly the allowed project files")
    for name, checksum in manifest["files"].items():
        path = stage / name
        if path.is_symlink() or not path.is_file() or path.resolve() != stage.resolve() / name:
            raise DeploymentError(f"Invalid release file: {name}")
        if digest(path) != checksum:
            raise DeploymentError(f"Release checksum mismatch: {name}")
    if release_id(manifest["files"], manifest["update_config"]) != manifest.get("release_id"):
        raise DeploymentError("Release identity mismatch")
    return manifest


def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
            temporary = Path(output.name)
            output.write(source.read_bytes())
        temporary.chmod(0o755 if destination.suffix == ".sh" else 0o644)
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def epoch(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo else None
    except (ValueError, TypeError, AttributeError):
        return None


class Deployer:
    """Owns a locked staging/backup/cutover/verification transaction on the VM."""
    def __init__(self, root, stage, *, unit=Path("/etc/systemd/system/job-monitor.service"),
                 runner=subprocess.run, clock=time.time, sleep=time.sleep, timeout=180):
        self.root, self.stage, self.unit = root, stage, unit
        self.runner, self.clock, self.sleep, self.timeout = runner, clock, sleep, timeout
        self.backup = None
        self.review_unit = unit.with_name("job-monitor-review.service")
        self.review_active = False

    def command(self, args, *, cwd=None, capture=False, check=True):
        return self.runner(args, cwd=cwd, check=check, text=True, capture_output=capture)

    def control(self, action):
        return self.command(["sudo", "-n", "systemctl", action, SERVICE])

    def current_health(self, *, since=None):
        result = self.command([sys.executable, "-B", "scripts/monitor.py", "--health"],
                              cwd=self.root, capture=True, check=False)
        if result.returncode != 0:
            return False, "Monitor reports attention required"
        try:
            pid = int(self.command(["systemctl", "show", SERVICE, "--property=MainPID", "--value"],
                                   capture=True).stdout.strip())
            snapshot = json.loads((self.root / "health.json").read_text())
            worker = snapshot["worker"]
            heartbeat = epoch(worker.get("heartbeat_at"))
            age = self.clock() - heartbeat if heartbeat is not None else None
            if (pid <= 0 or worker.get("pid") != pid or worker.get("status") != "running"
                    or age is None or not 0 <= age <= 30):
                return False, "Waiting for a fresh heartbeat from the service process"
            if since is not None:
                config = json.loads((self.root / "sources.json").read_text())
                state = json.loads((self.root / "state.json").read_text())
                boards = [s["provider"] + ":" + s["board"] for s in config["sources"] if s.get("enabled", True)]
                fresh = sum((epoch(state["boards"].get(board, {}).get("last_success")) or 0) >= since
                            for board in boards)
                if fresh != len(boards):
                    return False, f"Fresh successful board checks: {fresh}/{len(boards)}"
            return True, result.stdout.strip()
        except (OSError, ValueError, KeyError, TypeError):
            return False, "Waiting for readable current-worker health and history"

    def wait_healthy(self, since):
        deadline = self.clock() + self.timeout
        last_note = None
        while True:
            healthy, note = self.current_health(since=since)
            if healthy:
                print(note, flush=True)
                return
            if note != last_note:
                print(note, flush=True)
                last_note = note
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise DeploymentError(f"Health verification timed out: {note}")
            self.sleep(min(2, remaining))

    def validate(self):
        for name in CONFIG:
            if name not in self.manifest["files"]:
                shutil.copyfile(self.root / name, self.stage / name)
        self.command([sys.executable, "-B", "-m", "unittest", "-v", "tests.test_monitor"], cwd=self.stage)
        code = ("import json,sys; from pathlib import Path; from job_monitor.monitor import validate_config,read_state; "
                "validate_config(json.loads(Path('sources.json').read_text()), "
                "json.loads(Path('profile.json').read_text())); read_state(Path(sys.argv[1]))")
        self.command([sys.executable, "-B", "-c", code, str(self.root / "state.json")], cwd=self.stage)
        self.command(["systemd-analyze", "verify", str(self.stage / "deployment/job-monitor.service")])
        if self.review_unit.exists():
            self.review_active = self.command(["systemctl", "is-active", "--quiet", "job-monitor-review.service"], check=False).returncode == 0
            if self.review_active:
                # Only an already active review service is installed/restarted by
                # this update. First activation validates it after creating its venv.
                self.command(["systemd-analyze", "verify", str(self.stage / "deployment/job-monitor-review.service")])
                self.command([str(self.root / ".venv-review/bin/python"), "-B", "-c",
                              "import discord; assert discord.__version__ == '2.7.1', 'Install the review extra before updating'"])
                # Exercise staged controls with the installed SDK before stopping
                # either service. This is offline: no Discord login or messages.
                self.command([str(self.root / ".venv-review/bin/python"), "-B", "-m", "unittest", "-v",
                              "tests.test_monitor.DiscordReviewContracts"], cwd=self.stage)
        interactive = sys.stdin.isatty()
        try:
            # Check a real, read-only service command. sudo -v can require a
            # password even when commands are allowed through NOPASSWD rules.
            self.command(["sudo"] + ([] if interactive else ["-n"]) +
                         ["systemctl", "show", SERVICE, "--property=MainPID", "--value"], capture=True)
        except subprocess.CalledProcessError as error:
            message = ("Sudo authentication failed; the running monitor was not changed" if interactive
                       else "The service permission check failed; inspect sudo access from an interactive terminal. "
                            "The running monitor was not changed")
            raise DeploymentError(message) from error
        self.command(["systemctl", "is-active", "--quiet", SERVICE])

    def unchanged(self):
        marker = self.root / ".deployment.json"
        if not marker.exists():
            return False
        data = json.loads(marker.read_text())
        return (data.get("release_id") == self.manifest["release_id"]
                and all((self.root / name).is_file() and digest(self.root / name) == checksum
                        for name, checksum in self.manifest["files"].items())
                and digest(self.unit) == self.manifest["files"]["deployment/job-monitor.service"]
                and (not self.review_unit.exists() or
                     digest(self.review_unit) == self.manifest["files"]["deployment/job-monitor-review.service"]))

    def rollback(self):
        if self.review_active:
            self.command(["sudo", "-n", "systemctl", "stop", "job-monitor-review.service"])
        self.control("stop")
        for name in self.manifest["files"]:
            previous = self.backup / "code" / name
            if previous.exists():
                atomic_copy(previous, self.root / name)
            else:
                (self.root / name).unlink(missing_ok=True)
        self.command(["sudo", "-n", "install", "-m", "644", str(self.backup / "service.original"), str(self.unit)])
        if self.review_active:
            self.command(["sudo", "-n", "install", "-m", "644", str(self.backup / "review-service.original"), str(self.review_unit)])
        self.command(["sudo", "-n", "systemctl", "daemon-reload"])
        self.control("start")
        self.command(["systemctl", "is-active", "--quiet", SERVICE])
        if self.review_active:
            self.command(["sudo", "-n", "systemctl", "start", "job-monitor-review.service"])
        print("Previous code/service restored and started; latest posting history retained.", flush=True)

    def deploy(self):
        self.manifest = read_manifest(self.stage)
        if not (self.root / "state.json").is_file() or not self.unit.is_file():
            raise DeploymentError("This command updates an existing installation; history and service must exist")
        lock_path = self.root.parent / ".job-monitor-deploy.lock"
        with lock_path.open("a") as lock:
            os.chmod(lock_path, 0o600)
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise DeploymentError("Another deployment is in progress") from None
            if self.unchanged():
                healthy, note = self.current_health()
                if not healthy:
                    raise DeploymentError(f"Release already installed; health needs attention: {note}")
                print(f"Release {self.manifest['release_id'][:12]} already installed and healthy; no restart.")
                return
            print("Validating staged release while monitoring continues...", flush=True)
            self.validate()
            self.backup = Path(tempfile.mkdtemp(prefix="job-monitor-backup-", dir=self.root.parent))
            for name in self.manifest["files"]:
                if (self.root / name).exists():
                    atomic_copy(self.root / name, self.backup / "code" / name)
            shutil.copyfile(self.unit, self.backup / "service.original")
            if self.review_active:
                shutil.copyfile(self.review_unit, self.backup / "review-service.original")
            print(f"Backup: {self.backup}", flush=True)
            try:
                if self.review_active:
                    self.command(["sudo", "-n", "systemctl", "stop", "job-monitor-review.service"])
                self.control("stop")
                # Capture drained acknowledgements only after the worker stops.
                shutil.copyfile(self.root / "state.json", self.backup / "state.json")
                for database_name in ('review.sqlite3', 'learning.sqlite3'):
                    database = self.root / database_name
                    if not database.is_file():
                        continue
                    # Include WAL contents; raw copying the main file would not.
                    backup_deadline = time.monotonic() + 5
                    def backup_progress(status, remaining, total):
                        if time.monotonic() > backup_deadline:
                            raise DeploymentError(f"{database_name} backup timed out")
                    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as source_db:
                        with closing(sqlite3.connect(self.backup / database_name)) as backup_db:
                            source_db.backup(backup_db, pages=128, progress=backup_progress)
                            # The archive has no live writers. Make it readable
                            # without WAL coordination files on recovery hosts.
                            backup_db.execute('PRAGMA journal_mode=DELETE')
                    (self.backup / database_name).chmod(0o600)
                for name in self.manifest["files"]:
                    atomic_copy(self.stage / name, self.root / name)
                self.command(["sudo", "-n", "install", "-m", "644",
                              str(self.stage / "deployment/job-monitor.service"), str(self.unit)])
                self.command(["sudo", "-n", "systemctl", "daemon-reload"])
                if self.review_active:
                    self.command(["sudo", "-n", "install", "-m", "644",
                                  str(self.stage / "deployment/job-monitor-review.service"), str(self.review_unit)])
                    self.command(["sudo", "-n", "systemctl", "daemon-reload"])
                started = self.clock()
                self.control("start")
                print("Service started; verifying fresh worker and board checks...", flush=True)
                self.wait_healthy(started)
                if self.review_active:
                    self.command(["sudo", "-n", "systemctl", "start", "job-monitor-review.service"])
                    self.command(["systemctl", "is-active", "--quiet", "job-monitor-review.service"])
                data = {**self.manifest, "deployed_at": datetime.now(timezone.utc).isoformat(),
                        "backup": str(self.backup)}
                marker = self.stage / "deployment-result.json"
                marker.write_text(json.dumps(data, indent=2) + "\n")
                atomic_copy(marker, self.root / ".deployment.json")
                print(f"Deployment confirmed: {self.manifest['release_id'][:12]}", flush=True)
            except BaseException as error:
                print("Update failed; restoring previous code/service...", flush=True)
                try:
                    self.rollback()
                except BaseException as rollback_error:
                    raise DeploymentError(f"Update and rollback failed; inspect service and backup {self.backup}") from rollback_error
                raise DeploymentError(f"Update failed; previous service restarted. Backup: {self.backup}") from error


def client(project, target, *, interval=None, update_config=False):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._@-]*", target):
        raise DeploymentError("Use an SSH alias or user@host without shell metacharacters")
    ssh = ["ssh", "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15",
           "-o", "ServerAliveCountMax=3", target]
    if interval is None:
        code = ("import configparser,shlex; p=configparser.ConfigParser(interpolation=None, strict=False); "
                "p.read('/etc/systemd/system/job-monitor.service'); "
                "a=shlex.split(p['Service']['ExecStart']); print(a[a.index('--interval')+1])")
        result = subprocess.run(ssh + [shlex.join(["python3", "-B", "-c", code])],
                                check=True, text=True, capture_output=True)
        interval = float(result.stdout.strip())
        if not math.isfinite(interval) or interval < 15:
            raise DeploymentError("Running service has an unsupported polling interval")
    with tempfile.TemporaryDirectory(prefix="job-monitor-release-") as directory:
        stage = Path(directory)
        manifest = make_bundle(project, stage, interval=interval, update_config=update_config)
        print(f"Release: {manifest['release_id'][:12]} | Target: {target}", flush=True)
        result = subprocess.run(ssh + ['umask 077\nmktemp -d "$HOME/job-monitor-release.XXXXXX"'],
                                check=True, text=True, capture_output=True)
        remote = result.stdout.strip()
        remote_path = PurePosixPath(remote)
        if (not remote_path.is_absolute() or "\n" in remote or ".." in remote_path.parts
                or not remote_path.name.startswith("job-monitor-release.")):
            raise DeploymentError("SSH did not return a valid staging directory")
        archive = stage / "bundle.tar"
        with tarfile.open(archive, "w") as bundle:
            for name in (*manifest["files"], MANIFEST):
                bundle.add(stage / name, arcname=name, recursive=False)
        print("Uploading the explicit release bundle...", flush=True)
        with archive.open("rb") as stream:
            subprocess.run(ssh + [shlex.join(["tar", "-xf", "-", "-C", remote])], stdin=stream, check=True)
        command = shlex.join(["python3", "-B", f"{remote}/deployment/deploy.py", "--remote", remote])
        # Only execution needs a terminal for sudo. Never allocate one for binary upload.
        execution_ssh = ssh[:-1] + (["-t"] if sys.stdin.isatty() else ["-T"]) + [target]
        subprocess.run(execution_ssh + [command], check=True)
        subprocess.run(ssh + [shlex.join(["rm", "-rf", "--", remote])], check=False)


def main():
    parser = argparse.ArgumentParser(description="Update the existing monitor with backup and health verification")
    parser.add_argument("target", nargs="?", default="job-monitor", help="SSH alias (default: job-monitor)")
    parser.add_argument("--interval", type=int, choices=(15, 30, 60), help="Explicit polling interval override")
    parser.add_argument("--update-config", action="store_true", help="Also deploy local sources.json and profile.json")
    parser.add_argument("--remote", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.remote:
            def interrupted(*_):
                raise DeploymentError("Deployment interrupted")
            for sig in (signal.SIGTERM, signal.SIGHUP):
                signal.signal(sig, interrupted)
            Deployer(Path.home() / "job-monitor", args.remote).deploy()
        else:
            client(Path(__file__).resolve().parent.parent, args.target,
                   interval=args.interval, update_config=args.update_config)
        return 0
    except (DeploymentError, OSError, ValueError, subprocess.CalledProcessError) as error:
        # Do not include arbitrary command output or configuration contents.
        print(f"Deployment stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
