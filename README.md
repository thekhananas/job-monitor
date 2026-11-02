# Job Monitor

Get Discord or Telegram alerts when new jobs appear on public employer boards.
Each alert shows the role, company, location and a link to apply.

The monitor checks Greenhouse, Lever and Ashby boards, filters roles using your
preferences, and remembers postings to avoid repeat alerts. It runs on a small,
always-on VM and needs no paid API.

## Get started

Use **Python 3.12+ on Linux or macOS**. The core monitor needs no extra packages.
Run commands from the project directory.

Copy the example settings, then edit them for your employers and preferences:

```sh
cp config/sources.example.json sources.json
cp config/profile.example.json profile.json
```

Check the boards without saving anything or sending alerts:

```sh
python3 -B scripts/monitor.py --dry-run
```

Then run once to record the existing jobs:

```sh
python3 -B scripts/monitor.py
```

With the example settings, existing jobs are recorded without alerts. Later
matches can trigger alerts. Keep `state.json` so the monitor remembers its history.

## Receive Discord alerts

Create a webhook for your Discord channel. Run `read` below, paste its URL and
press Enter. Your input stays hidden.

```sh
read -rs DISCORD_WEBHOOK_URL
export DISCORD_WEBHOOK_URL
python3 -B scripts/monitor.py --watch --interval 60 --notify --channel discord
```

This checks each board about once a minute and sends matching job alerts.
Checks, source delays and retries affect how quickly an alert arrives.
Unknown posting dates are shown as unavailable; discovery time is not treated
as the time a job was posted.

For monitoring after you close the terminal, follow the
[VM setup guide](docs/operations.md). Keep webhook URLs and tokens private.
Telegram and the optional Discord browsing and filter controls are covered in
[configuration](docs/configuration.md).

## Check status

```sh
python3 -B scripts/monitor.py --health
```

This reads the worker status, recent board checks and pending alerts. Use
`--menu` to browse saved jobs or `--audit` to inspect rejected postings.

The current location rules focus on London and UK remote roles. Coverage depends
on the boards you configure, and filters can miss useful jobs. Prioritization
and preference learning are experimental.