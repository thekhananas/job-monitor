# Deployment and operations

The reference setup is an always-on Ubuntu VM with Python 3.12 or newer.
The monitor process and optional review bot run as separate systemd services.
The supplied unit paths assume user `ubuntu` and `/home/ubuntu/job-monitor`;
adapt them for another installation.

## First installation

Copy the public project files to the VM. From the VM project directory, copy the
configuration examples to `sources.json` and `profile.json`, customize them, and
run `python3 -B scripts/monitor.py --dry-run`. Then run once without `--dry-run`
to establish history before starting the service.

Create `/home/ubuntu/.config/job-monitor/notifications.env` with mode `600` and
the entry `DISCORD_WEBHOOK_URL=your-webhook-url`. Keep the directory private.
The service reads this file; do not place credentials in the unit template.

After adapting the paths, install and start the worker:

```sh
sudo install -m 644 deployment/job-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now job-monitor.service
```

These commands enable continuous monitoring and real Discord notifications.
`sudo` supplies the privileges needed to manage a system service; file staging
alone does not need those privileges. The service continues after SSH exits.
The timer template is retained for older installations; the watch service does
not require it. Disable an old timer before enabling the watch service.

## Status

On the VM:

```sh
systemctl status job-monitor.service --no-pager
python3 -B scripts/monitor.py --health
journalctl -u job-monitor.service -n 40 --no-pager
```

From your local terminal, reuse your SSH alias:

```sh
ssh job-monitor 'cd ~/job-monitor && python3 -B scripts/monitor.py --health'
```

Inspect failing/stale boards and pending delivery retries. Queue-to-confirmation
timing ends at the notification API response; it does not measure original
publication-to-discovery or phone arrival. A healthy snapshot does not replace
an independent uptime alert.

## Update an existing installation

From the local checkout:

```sh
bash deployment/deploy.sh job-monitor
```

This command uploads an explicit file allowlist, validates staged code, backs up
code and consistent state, restarts the service, and checks fresh worker/board
health. It requires SSH access and service-management privileges on the VM.
Use `--update-config` only when intentionally replacing the VM's JSON startup
configuration. Saved review policies remain separate. The existing polling
interval is retained unless explicitly overridden.

The package layout changes the service entry point to `scripts/monitor.py`.
Use the deployment tool to install matching code and units together. Rollback
restores prior code/units while retaining newer live history and label data.
Keep older root-level modules on an existing VM until this migration is verified;
they may be required by its previous service during rollback.

The updater targets an existing installation with history and a service. It is
not the first-install procedure. Back up runtime state independently and check
that backups can be read before relying on recovery.

## Optional review service

Create `.venv-review` in the VM project directory and install `.[review]` using
its Python interpreter. On Ubuntu, creating a venv may require the matching
`python3-venv` package. Create a private `review.env` with the bot token, owner
and guild variables described in [configuration](configuration.md).

Put `JOB_MONITOR_REVIEW_USER_ID` in the private `review-worker.env` file so the
worker maintains the asynchronous public-job projection. Restart the worker to
load that environment setting. Validate bot setup with:

```sh
.venv-review/bin/python -B scripts/review_bot.py --check
```

This checks construction without connecting or sending messages, but it can
initialize the local review database. Install and enable
`deployment/job-monitor-review.service` separately when ready. Updates restart
an already active review service; they do not activate it for the first time.
