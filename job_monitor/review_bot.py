"""Discord adapter for personal job review. Gateway connection; no public web server."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import time

from job_monitor.review_store import ReviewConflict, ReviewStore
from job_monitor.screening import normal_phrase, REJECTION_LABELS, DEFAULT_EXCLUDED_TITLES, first_publication
from job_monitor.notifications import shortened
from job_monitor.learning import LearningService, LearningStore, QUESTION, REASONS


def settings_text(profile):
    cities = ", ".join(profile.get("locations", ["London"]))
    remote = " + UK remote" if profile.get("allow_uk_remote", True) else ""
    age = profile.get("max_posting_age_days")
    return (f"Location: {cities}{remote}\nPosted within: {str(age) + ' days' if age else 'Any age'}\n"
            "Posting age uses first publication only. Unknown dates stay in review when an age limit is set.")


async def wait_for_activation(store, owner, version, *, timeout=30, interval=2):
    """Bounded acknowledgement of a loaded policy, not a claim about board freshness."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            current, _ = await asyncio.to_thread(store.policy, owner)
            if current != version:
                return "superseded"
            status = await asyncio.to_thread(store.status, owner)
            if (status and status["applied"] == version and not status["error"]
                    and 0 <= time.time() - status["heartbeat"] <= 30):
                return "active"
        except (OSError, ValueError, sqlite3.Error):
            pass  # A transient read failure must not undo a successfully saved rule.
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "pending"
        await asyncio.sleep(min(interval, remaining))


def build_client(store, owner, guild_id, *, activation_timeout=30, learning=None):
    # Optional dependency: the existing poller and all domain checks stay standard-library only.
    import discord
    from discord import app_commands
    owner, guild_id = int(owner), int(guild_id)
    learning = learning or LearningService(store, LearningStore(store.path.with_name('learning.sqlite3')))

    def safe(value, limit=300):
        return discord.utils.escape_mentions(discord.utils.escape_markdown(str(value)))[:limit]

    def link(value):
        if not str(value).startswith("https://") or any(char in value for char in "\r\n<> "):
            return None
        return value if len(value) <= 512 else None

    async def failure(interaction, error):
        message = str(error) if isinstance(error, ValueError) else "Review storage is busy or unavailable. Try again; your alert worker is separate."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    class PersonalView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=600)

        async def interaction_check(self, interaction):
            if interaction.user.id == owner and interaction.guild_id == guild_id:
                return True
            await interaction.response.send_message("These controls belong to the configured job-monitor owner.", ephemeral=True)
            return False

        async def on_error(self, interaction, error, item):
            await failure(interaction, error)

    def button(view, label, action, *, style=discord.ButtonStyle.secondary, row=None):
        item = discord.ui.Button(label=label, style=style, row=row)
        item.callback = action
        view.add_item(item)

    async def display(interaction, text, view, *, replace=False):
        if replace:
            await interaction.edit_original_response(content=shortened(text, 1900), view=view)
        else:
            await interaction.followup.send(shortened(text, 1900), view=view, ephemeral=True)

    async def publish_preview(interaction, proposal):
        token, profile, preview = proposal
        view = PreviewView(token)
        _, current = await asyncio.to_thread(store.policy, owner)
        title_changes = []
        for key, label in (("title_keywords", "Allowed titles"), ("excluded_title_keywords", "Excluded titles")):
            default = DEFAULT_EXCLUDED_TITLES if key == "excluded_title_keywords" else []
            before, after = set(current.get(key, default)), set(profile.get(key, default))
            if after - before:
                title_changes.append(label + " added: " + safe(", ".join(sorted(after - before)), 250))
            if before - after:
                title_changes.append(label + " removed: " + safe(", ".join(sorted(before - after)), 250))
        text = ("**Review your filter change**\n" + "\n".join(title_changes) + "\n\n" +
                f"Current postings newly eligible: **{len(preview['added'])}**\n"
                f"Current postings no longer eligible: **{len(preview['removed'])}**\n\n" +
                settings_text(profile) + "\n\n"
                "Other eligibility checks still apply. Existing postings will not trigger a catch-up alert; "
                "open /jobs to apply to them.\n"
                "Based on last-observed postings. Preview expires in 10 minutes.")
        if preview['unknown_dates']:
            text += f"\nOriginal posting date unavailable for {preview['unknown_dates']} postings; they cannot pass this age limit."
        if preview['stale_boards']:
            text += f"\n{preview['stale_boards']} boards have stale or failed checks; this preview may be incomplete."
        await interaction.followup.send(shortened(text, 1900), view=view, ephemeral=True)

    class PreviewView(PersonalView):
        def __init__(self, token):
            super().__init__()
            self.token = token
            async def save(interaction):
                await interaction.response.defer(ephemeral=True)
                version = await asyncio.to_thread(store.confirm, owner, token)
                undo_view = PersonalView()
                async def undo(clicked):
                    await clicked.response.defer(ephemeral=True)
                    proposal = await asyncio.to_thread(store.undo, owner, version)
                    await publish_preview(clicked, proposal)
                button(undo_view, "Undo change…", undo)
                await interaction.edit_original_response(content="Saved — waiting for the monitor to load your settings…", view=undo_view)
                outcome = await wait_for_activation(store, owner, version, timeout=activation_timeout)
                message = {"active": "Your new settings are active. Jobs will be checked on their next poll.",
                           "superseded": "Saved, then replaced by a newer settings change. Open /filters to see the current rules.",
                           "pending": "Saved, but activation is not confirmed yet. Open /filters to check again; if this persists, check monitor health."}[outcome]
                await interaction.edit_original_response(content=message + " Existing postings remain in /jobs; no catch-up alerts were sent. Undo is available for 10 minutes.", view=undo_view)
            async def cancel(interaction):
                # Cancel consumes the preview as well as removing its controls.
                await interaction.response.defer(ephemeral=True)
                await asyncio.to_thread(store.cancel, owner, token)
                await interaction.edit_original_response(content="Cancelled. Your alert settings are unchanged.", view=None)
            async def inspect(interaction):
                await interaction.response.defer(ephemeral=True)
                draft = await asyncio.to_thread(store.draft, owner, token)
                preview = json.loads(draft["preview"])
                lines = ["**Affected postings** · first 10"]
                for heading, keys in (("Would qualify", preview["added"]), ("Would stop qualifying", preview["removed"])):
                    lines.append(heading)
                    for key in keys[:5]:
                        record = await asyncio.to_thread(store.job, key)
                        job = json.loads(record["payload"])
                        url = link(job["url"])
                        lines.append(f"{safe(record['company'],60)} · {safe(job['title'],100)}" + (f"\n<{url}>" if url else ""))
                # Send whole entries so truncation never cuts an application URL.
                chunk = ""
                for line in lines:
                    if len(chunk) + len(line) + 1 > 1800:
                        await interaction.followup.send(chunk, ephemeral=True)
                        chunk = ""
                    chunk += line + "\n"
                if chunk:
                    await interaction.followup.send(chunk, ephemeral=True)
            button(self, "View affected jobs", inspect)
            button(self, "Save filter change", save, style=discord.ButtonStyle.primary)
            button(self, "Cancel", cancel)

    class LocationsModal(discord.ui.Modal, title="Alert locations"):
        cities = discord.ui.TextInput(label="Cities (comma separated)", max_length=200)

        def __init__(self, profile):
            super().__init__()
            self.cities.default = ", ".join(profile.get("locations", ["London"]))

        async def on_submit(self, interaction):
            if interaction.user.id != owner or interaction.guild_id != guild_id:
                await interaction.response.send_message("These settings belong to the configured owner.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            try:
                cities = list(dict.fromkeys(value.strip() for value in self.cities.value.split(",") if value.strip()))
                if not cities or len(cities) > 10:
                    raise ValueError("Enter between one and ten city names")
                proposal = await asyncio.to_thread(store.propose, owner, {"locations": cities})
                await publish_preview(interaction, proposal)
            except (ValueError, OSError, sqlite3.Error) as error:
                await failure(interaction, error)

    class TitlesModal(discord.ui.Modal, title="Alert title phrases"):
        allowed = discord.ui.TextInput(label="Allowed phrases (one per line)", style=discord.TextStyle.paragraph, max_length=2000)
        excluded = discord.ui.TextInput(label="Excluded phrases (one per line)", style=discord.TextStyle.paragraph, max_length=1000, required=False)

        def __init__(self, profile):
            super().__init__()
            self.allowed.default = "\n".join(profile["title_keywords"])
            self.excluded.default = "\n".join(profile.get("excluded_title_keywords", DEFAULT_EXCLUDED_TITLES))

        async def on_submit(self, interaction):
            if interaction.user.id != owner or interaction.guild_id != guild_id:
                await interaction.response.send_message("These settings belong to the configured owner.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            try:
                proposal = await asyncio.to_thread(store.propose, owner, {
                    "title_keywords": list(dict.fromkeys(value.strip() for value in self.allowed.value.splitlines() if value.strip())),
                    "excluded_title_keywords": list(dict.fromkeys(value.strip() for value in self.excluded.value.splitlines() if value.strip()))})
                await publish_preview(interaction, proposal)
            except (ValueError, OSError, sqlite3.Error) as error:
                await failure(interaction, error)

    class FiltersView(PersonalView):
        def __init__(self, profile):
            super().__init__()
            async def locations(interaction):
                await interaction.response.send_modal(LocationsModal(profile))
            async def titles(interaction):
                await interaction.response.send_modal(TitlesModal(profile))
            async def remote(interaction):
                await interaction.response.defer(ephemeral=True)
                proposal = await asyncio.to_thread(store.propose, owner, {"allow_uk_remote": not profile.get("allow_uk_remote", True)})
                await publish_preview(interaction, proposal)
            async def ambiguous(interaction):
                await interaction.response.defer(ephemeral=True)
                proposal = await asyncio.to_thread(store.propose, owner, {"allow_ambiguous_remote": not profile.get("allow_ambiguous_remote", False)})
                await publish_preview(interaction, proposal)
            select = discord.ui.Select(placeholder="Change maximum posting age", options=[
                discord.SelectOption(label=label, value=value) for label, value in
                (("Any age", "any"), ("Last 24 hours", "1"), ("Last 3 days", "3"), ("Last 7 days", "7"), ("Last 14 days", "14"))])
            async def age(interaction):
                await interaction.response.defer(ephemeral=True)
                value = select.values[0]
                proposal = await asyncio.to_thread(store.propose, owner, {"max_posting_age_days": None if value == "any" else int(value)})
                await publish_preview(interaction, proposal)
            select.callback = age
            self.add_item(select)
            button(self, "Edit cities", locations, row=1)
            button(self, "Disable UK remote" if profile.get("allow_uk_remote", True) else "Allow UK remote", remote, row=1)
            button(self, "Exclude unclear remote" if profile.get("allow_ambiguous_remote", False) else "Allow unclear remote", ambiguous, row=2)
            button(self, "Edit title phrases", titles, row=2)

    async def show_suggestions(interaction, *, offset=0, replace=False):
        summary = await asyncio.to_thread(store.title_suggestions, owner)
        groups = summary["groups"]
        view = PersonalView()
        if groups:
            offset = min(offset, len(groups) - 1)
            group = groups[offset]
            text = (f"**Consider including “{safe(group['title'],200)}”**\n"
                    f"**{len(group['jobs'])}** unreviewed postings match your other filters but fail the allowed-title rule.\n\n")
            for row in group["jobs"][:3]:
                payload = json.loads(row["payload"])
                text += f"• {safe(row['company'],80)} · {safe(payload['location'],120)}\n"
            if len(group["jobs"]) > 3:
                text += f"…and {len(group['jobs']) - 3} more.\n"
            text += (f"\nProposed phrase: **{safe(group['phrase'],200)}**\n"
                     "Applies to future titles containing this exact phrase, with all other filters still applied.\n"
                     f"\nSuggestion {offset + 1} of {len(groups)}. Based on recent successful board checks.")
            async def propose(interaction):
                await interaction.response.defer(ephemeral=True)
                proposal = await asyncio.to_thread(store.propose_title, owner, group["phrase"], group["evidence"], summary["version"])
                await publish_preview(interaction, proposal)
            async def inspect(interaction):
                await interaction.response.defer(ephemeral=True)
                await show_jobs(interaction, rejected=True, reason="title", title=group["phrase"], replace=True)
            async def dismiss(interaction):
                await interaction.response.defer(ephemeral=True)
                await asyncio.to_thread(store.dismiss_title, owner, group["phrase"], group["evidence"], summary["version"])
                await show_suggestions(interaction, offset=offset, replace=True)
            button(view, "Review filter change", propose, style=discord.ButtonStyle.primary)
            button(view, "View these jobs", inspect)
            button(view, "Dismiss these jobs", dismiss, row=1)
            text += "\nDismiss hides these postings from review; it does not change your filters."
            if len(groups) > 1:
                async def next_suggestion(interaction):
                    await interaction.response.defer(ephemeral=True)
                    await show_suggestions(interaction, offset=(offset + 1) % len(groups), replace=True)
                button(view, "Next suggestion", next_suggestion, row=1)
        else:
            text = ("**No title-rule suggestions right now**\n"
                    "Suggestions cover unreviewed engineer/developer titles where the title rule is the only blocker. "
                    "This does not mean every exclusion is correct.\n"
                    f"\nOther unreviewed exclusions: {summary['unreviewed']}.")
        if summary['unknown_dates']:
            text += f"\nOriginal posting date unavailable: {summary['unknown_dates']} postings cannot pass your age limit."
        if summary['stale_boards']:
            text += f"\nSuggestions omit {summary['stale_boards']} boards with stale or failed checks."
        text += "\nControls expire in 10 minutes; reopen /review to continue."
        async def browse(interaction):
            await interaction.response.defer(ephemeral=True)
            await show_jobs(interaction, rejected=True, replace=True)
        button(view, "Browse all exclusions", browse, row=2)
        await display(interaction, text, view, replace=replace)

    class JobsView(PersonalView):
        def __init__(self, record, reasons, version, *, rejected, offset, location, days, reason=None, title=None,
                     learning_snapshot=None, decision=None):
            super().__init__()
            payload = json.loads(record["payload"])
            url = link(payload["url"])
            if url:
                self.add_item(discord.ui.Button(label="View posting", url=url))
            async def next_job(interaction):
                await interaction.response.defer(ephemeral=True)
                await show_jobs(interaction, rejected=rejected, offset=offset + 1, location=location, days=days, reason=reason, title=title, replace=True)
            button(self, "Next job", next_job)
            if offset:
                async def previous_job(interaction):
                    await interaction.response.defer(ephemeral=True)
                    await show_jobs(interaction, rejected=rejected, offset=offset - 1, location=location, days=days, reason=reason, title=title, replace=True)
                button(self, "Previous job", previous_job)
            if learning_snapshot is not None:
                async def update_decision(interaction, choice=None, *, undo=False, preference_reason=None):
                    if not await permitted(interaction):
                        return
                    await interaction.response.defer(ephemeral=True)
                    try:
                        expected = decision['id'] if decision else None
                        request = f'discord:{interaction.id}'
                        if undo:
                            await asyncio.to_thread(learning.undo, owner, learning_snapshot, request, expected_id=expected)
                        else:
                            await asyncio.to_thread(learning.decide, owner, learning_snapshot, choice, request,
                                                    expected_id=expected, reason=preference_reason)
                    except (OSError, ValueError, sqlite3.Error) as error:
                        message = str(error) if isinstance(error, ValueError) else 'Decision not confirmed. Reopen the job to check your saved choice before retrying.'
                        await interaction.followup.send(message, ephemeral=True)
                        return
                    await show_jobs(interaction, rejected=rejected, offset=offset, location=location, days=days,
                                    reason=reason, title=title, replace=True, anchor_key=record['key'],
                                    learning_notice='Decision saved; current choice is shown below. No model was trained.')
                async def yes(interaction):
                    await update_decision(interaction, 'yes')
                async def no(interaction):
                    await update_decision(interaction, 'no')
                async def skip(interaction):
                    if not await permitted(interaction):
                        return
                    await interaction.response.defer(ephemeral=True)
                    await show_jobs(interaction, rejected=rejected, offset=offset + 1, location=location, days=days,
                                    reason=reason, title=title, replace=True)
                for label, choice, action in (('Yes', 'yes', yes), ('No', 'no', no)):
                    selected = decision is not None and decision['choice'] == choice
                    button(self, label, action, row=3,
                           style=discord.ButtonStyle.primary if selected else discord.ButtonStyle.secondary)
                button(self, 'Skip', skip, row=3)
                if decision and decision['kind'] == 'decision':
                    async def undo(interaction):
                        await update_decision(interaction, undo=True)
                    button(self, 'Undo decision', undo, row=2)
                if decision and decision['choice'] is not None:
                    async def clear(interaction):
                        await update_decision(interaction)
                    button(self, 'Clear decision', clear, row=2)
                    options = [discord.SelectOption(label='No reason', value='none', default=decision['reason'] is None)]
                    options.extend(discord.SelectOption(label=label, value=value, default=decision['reason'] == value)
                                   for value, label in REASONS.items())
                    reason_select = discord.ui.Select(placeholder='Reason (optional)', options=options, row=4)
                    async def add_reason(interaction):
                        value = reason_select.values[0]
                        await update_decision(interaction, decision['choice'], preference_reason=None if value == 'none' else value)
                    reason_select.callback = add_reason
                    self.add_item(reason_select)
            if not rejected:
                return
            async def keep(interaction):
                await interaction.response.defer(ephemeral=True)
                await asyncio.to_thread(store.feedback, owner, record["key"], record["hash"], version, "excluded")
                await show_jobs(interaction, rejected=True, offset=offset, location=location, days=days, reason=reason, title=title, replace=True)
            async def allow_title(interaction):
                await interaction.response.defer(ephemeral=True)
                current = await asyncio.to_thread(store.job, record["key"])
                current_version, profile = await asyncio.to_thread(store.policy, owner)
                if current["hash"] != record["hash"] or current_version != version:
                    raise ReviewConflict("This posting or your settings changed. Refresh /review.")
                candidate = list(profile["title_keywords"])
                phrase = normal_phrase(payload["title"])
                if phrase in [normal_phrase(item) for item in candidate]:
                    raise ValueError("This title is already allowed; another rule is blocking this job.")
                candidate.append(phrase)
                proposal = await asyncio.to_thread(store.propose, owner, {"title_keywords": candidate})
                await interaction.followup.send(f"Proposed title phrase: **{safe(phrase,180)}**. This applies to future titles containing that phrase. Other exclusions still apply.", ephemeral=True)
                await publish_preview(interaction, proposal)
            button(self, "Dismiss this job", keep, row=1)
            if "title" in reasons:
                button(self, "Review title rule…", allow_title, row=1)
            async def suggestions(interaction):
                await interaction.response.defer(ephemeral=True)
                await show_suggestions(interaction, replace=True)
            button(self, "Back to suggestions", suggestions, row=2)

    async def show_jobs(interaction, *, rejected=False, offset=0, location=None, days=None, reason=None, title=None,
                        replace=False, learning_notice=None, anchor_key=None):
        version, profile = await asyncio.to_thread(store.policy, owner)
        rows, total = await asyncio.to_thread(store.jobs, owner, rejected=rejected, location=location, days=days,
                                            reason=reason, title=title, offset=0 if anchor_key else offset,
                                            limit=1_000_000 if anchor_key else 1)
        if anchor_key:
            found = next((index for index, item in enumerate(rows) if item[0]['key'] == anchor_key), None)
            rows = [rows[found]] if found is not None else []
            if found is not None:
                offset = found
        if not rows:
            view = PersonalView()
            if rejected:
                async def suggestions(interaction):
                    await interaction.response.defer(ephemeral=True)
                    await show_suggestions(interaction, replace=True)
                button(view, "Back to suggestions", suggestions)
            await display(interaction, (learning_notice + '\n' if learning_notice else '') + "No more postings in this view. Run /review or /jobs to start again. This view only covers last-observed active postings on monitored boards.", view, replace=replace)
            return
        record, match, reasons = rows[0]
        payload = json.loads(record["payload"])
        published = first_publication(payload)
        if published is not None and published > datetime.now(timezone.utc):
            published = None
        publication_label = published.astimezone(timezone.utc).strftime("%d %b %Y, %H:%M UTC") if published else "Unknown"
        text = (f"**{safe(payload['title'],200)}**\n{safe(record['company'],80)} · {safe(payload['location'],200)}\n"
                f"First published: {publication_label}\n")
        if reasons:
            text += "\nExcluded because:\n" + "\n".join("• " + REJECTION_LABELS.get(reason, reason) for reason in reasons)
        if record["error"] or time.time() - record["checked"] > 180:
            text += "\nLatest board observation is failed or stale; verify the listing is still open."
        if location is not None or days is not None:
            text += "\nTemporary search. Your alert settings are unchanged."
        snapshot, decision = None, None
        try:
            snapshot = await asyncio.to_thread(learning.capture, owner, record, version, profile,
                selection={'view': 'discord_review' if rejected else 'discord_jobs',
                           'location': location, 'days': days, 'reason': reason, 'title_group': title})
            decision = await asyncio.to_thread(learning.labels.latest, owner, snapshot)
            choice = decision['choice'].capitalize() if decision and decision['choice'] else 'Not labeled'
            text += f'\n\n{QUESTION}\nYour decision: **{choice}**. Skip records nothing. No model trained yet.'
            if decision and decision['reason']:
                text += '\nReason: ' + REASONS.get(decision['reason'], 'Unrecognized saved reason')
            if snapshot['candidate']['status'] == 'not_configured':
                text += '\nCandidate profile is not attached; labels need context confirmation before training.'
        except (OSError, ValueError, sqlite3.Error):
            snapshot = None
            text += '\n\nPreference labeling is unavailable. Browsing and alert rules are unchanged; reopen to retry.'
        if learning_notice:
            text += '\n' + learning_notice
        text += f"\n\n{offset + 1} of {total}. Controls expire in 10 minutes; reopen the command to continue."
        if rejected:
            text += "\nDismiss hides this job until its content or blocking reasons change; alert filters stay the same."
        view = JobsView(record, reasons, version, rejected=rejected, offset=offset, location=location, days=days, reason=reason,
                        title=title, learning_snapshot=snapshot, decision=decision)
        await display(interaction, text, view, replace=replace)

    class ReviewClient(discord.Client):
        def __init__(self):
            super().__init__(intents=discord.Intents.none(), allowed_mentions=discord.AllowedMentions.none())
            self.tree = app_commands.CommandTree(self)

        async def setup_hook(self):
            self.tree.copy_global_to(guild=discord.Object(id=guild_id))
            await self.tree.sync(guild=discord.Object(id=guild_id))

    client = ReviewClient()

    async def permitted(interaction):
        if interaction.user.id != owner or interaction.guild_id != guild_id:
            await interaction.response.send_message("This personal monitor is restricted to its configured owner and server.", ephemeral=True)
            return False
        return True

    @client.tree.command(name="filters", description="View and change your job-alert rules with a preview")
    async def filters(interaction: discord.Interaction):
        if not await permitted(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        version, profile = await asyncio.to_thread(store.policy, owner)
        status = await asyncio.to_thread(store.status, owner)
        active = bool(status and time.time() - status["heartbeat"] <= 30 and status["applied"] == version and not status["error"])
        note = "Loaded by the monitor" if active else "Saved; not yet confirmed active. Check monitor health if this persists."
        await interaction.followup.send(f"**Your alert rules**\n{settings_text(profile)}\n\n{note}.\nUnclear remote: {'Allowed with warning' if profile.get('allow_ambiguous_remote',False) else 'Excluded for review'}\nAllowed title phrases: {len(profile['title_keywords'])}\nControls expire in 10 minutes; reopen /filters to continue.", view=FiltersView(profile), ephemeral=True)

    @client.tree.command(name="review", description="See suggested title-rule changes, or inspect specific exclusions")
    @app_commands.choices(reason=[app_commands.Choice(name=label[:100], value=code) for code, label in REJECTION_LABELS.items()])
    @app_commands.describe(location="Optional text search within rejected job locations", reason="Optional rejection reason")
    async def review(interaction: discord.Interaction, location: str = None, reason: str = None):
        if not await permitted(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        if location is not None and (not location.strip() or len(location) > 100):
            raise ValueError("Enter location text of up to 100 characters")
        if location is None and reason is None:
            await show_suggestions(interaction)
        else:
            await show_jobs(interaction, rejected=True, location=location, reason=reason)

    @client.tree.command(name="jobs", description="Explore qualifying postings without changing your alert rules")
    @app_commands.describe(location="Optional city; overrides saved location only for this search", days="First-publication age in days; unknown dates do not qualify")
    async def jobs(interaction: discord.Interaction, location: str = None, days: app_commands.Range[int, 1, 365] = None):
        if not await permitted(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        if location is not None and (not location.strip() or len(location) > 100):
            raise ValueError("Enter a city name of up to 100 characters")
        await show_jobs(interaction, location=location.strip() if location else None, days=days)

    @client.tree.error
    async def command_error(interaction, error):
        await failure(interaction, getattr(error, "original", error))

    return client


def main():
    parser = argparse.ArgumentParser(description="Optional owner-only Discord job-review bot")
    parser.add_argument("--database", type=Path, default=Path("review.sqlite3"))
    parser.add_argument('--candidate', type=Path, default=Path.home() / '.config/job-monitor/candidate.json',
                        help='Optional private candidate profile for preference-label context; never a raw resume')
    parser.add_argument("--profile", type=Path, default=Path("profile.json"))
    parser.add_argument("--check", action="store_true", help="Validate bot construction offline without connecting or registering commands")
    args = parser.parse_args()
    owner = os.environ.get("JOB_MONITOR_REVIEW_USER_ID", "")
    guild = os.environ.get("JOB_MONITOR_REVIEW_GUILD_ID", "")
    if not owner.isdigit() or not guild.isdigit():
        parser.error("Set JOB_MONITOR_REVIEW_USER_ID and JOB_MONITOR_REVIEW_GUILD_ID to Discord IDs")
    try:
        store = ReviewStore(args.database)
        store.initialise(owner, json.loads(args.profile.read_text()))
        learning = LearningService(store, LearningStore(args.database.with_name('learning.sqlite3')), candidate_path=args.candidate)
        client = build_client(store, owner, guild, learning=learning)
        if args.check:
            print("Discord review commands constructed successfully; no connection, registration or message sent")
            return 0
        token = os.environ.get("JOB_MONITOR_REVIEW_BOT_TOKEN")
        if not token:
            raise ValueError("Set JOB_MONITOR_REVIEW_BOT_TOKEN in the private VM environment file")
        # Library default loggers can include HTTP context. Keep errors generic at this boundary.
        client.run(token, log_handler=None)
        return 0
    except Exception as error:
        print("Review bot stopped: " + ("install the review extra in its virtual environment" if isinstance(error, ImportError)
                                      else str(error) if isinstance(error, ValueError) else type(error).__name__))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
