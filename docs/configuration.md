# Configuration

Copy `config/sources.example.json` and `config/profile.example.json` to
`sources.json` and `profile.json` in the working directory. The examples contain
public board identifiers and illustrative preferences. Credentials belong in
environment variables or private files outside the checkout.

## Sources

Each source needs `provider`, `board` and `company`. Supported providers are
`greenhouse`, `lever` and `ashby`. Set `enabled` to `false` to disable a source.
Verify the board identifier against the employer's official careers site.
A configured board does not guarantee coverage of every employer team.

`baseline_policy` controls the first successful observation of a board:
`suppress` records current jobs without alerting, while `notify` queues matching
existing jobs. Both preserve history on later source failures.

## Screening

The example profile contains title keywords, excluded title phrases, years of
experience and remote eligibility preferences. The current implementation
assumes London/UK geography; changing remote-region hints does not turn it into
a general international location filter. Review recorded exclusions before
assuming the rules capture your intended roles.

The optional Discord integration provides owner-only `/jobs`, `/review` and
`/filters` commands. Filter changes are previewed and saved as versioned policy;
the worker reloads saved settings. The JSON profile seeds setup and is not a
replacement for an already saved policy. Feedback on an excluded posting and a
Yes/No preference label are distinct records.

## Environment variables

| Variable | Purpose |
| --- | --- |
| `DISCORD_WEBHOOK_URL` | Incoming webhook for job alerts. |
| `TELEGRAM_BOT_TOKEN` | Telegram delivery credential, if using Telegram. |
| `TELEGRAM_CHAT_ID` | Telegram destination, if using Telegram. |
| `JOB_MONITOR_REVIEW_BOT_TOKEN` | Optional Discord review application token. |
| `JOB_MONITOR_REVIEW_USER_ID` | Allowed review user; also enables worker projection when configured. |
| `JOB_MONITOR_REVIEW_GUILD_ID` | Allowed Discord server. |
| `JOB_MONITOR_REVIEW_ALERT_CHANNEL_ID` | Channel containing trusted monitor alerts, for message rating. |
| `JOB_MONITOR_REVIEW_ALERT_WEBHOOK_ID` | ID of the alert webhook, for message rating. This is an ID, not its token or URL. |

Install the optional `review` dependency, configure these IDs/token privately,
and invite your Discord application with the `bot` and `applications.commands`
scopes. Run `python3 -B scripts/review_bot.py` from the working directory.
Starting the bot connects to Discord and registers commands.

## Optional preference feedback

The local feedback implementation adds **Rate this job** to a job alert's message
menu under Apps. Configure both alert IDs above in the review bot's private
environment file. They restrict the action to the intended channel and webhook;
the bot does not need the webhook URL or delivery token. Existing slash commands
remain available. With no alert IDs, browsing still works and message rating
reports that setup is incomplete.

Supply a validated, sanitized candidate profile at
`~/.config/job-monitor/candidate.json`, or pass `--candidate` to the review bot.
Use a private summary, never a raw resume. A missing or unreadable profile
disables saving preferences while leaving the official application link usable.

The private card asks **Would you consider applying?** Choose Yes or No. Saving
offers Undo; More contains correction, withdrawal and an optional reason.
Ignoring a role records nothing. `/jobs` and individual `/review` cards provide
the same rating action without adding the full controls to every browsing card.

The new target is `consider_applying_v2`. Older `worth_applying_v1` decisions
remain stored under their original context and are not silently reinterpreted.
Feedback does not change filters, send alerts or train a model. The local message
action still needs real-SDK and phone verification before claiming live operation.

## Runtime files

`state.json` holds posting history and pending deliveries. `health.json` is a
worker snapshot, and `latest.md` is a generated report. Optional review and
learning data live in `review.sqlite3` and `learning.sqlite3`. Keep these private
and preserve them across updates; SQLite databases may have WAL/SHM files.

`--config`, `--profile` and `--state` override monitor paths. Use `--help` on each
entry point for the complete CLI. Paths default to the working directory, not
the installed package directory.
