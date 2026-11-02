"""Notification adapters; credentials never enter persisted history."""
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from job_monitor.health import timestamp


class NotificationError(ValueError):
    def __init__(self, message, retry_after=0):
        super().__init__(message)
        self.retry_after = retry_after if math.isfinite(retry_after) and retry_after > 0 else 0


def alert_text(events):
    lines = ["New job leads — verify the official adverts"]
    for event in events:
        job = event["job"]
        lines.extend(["", f"{event['kind']}: {event['company']} — {job['title']}",
                      job["location"], f"{job['date_type']}: {job['published_at'] or 'unknown'}",
                      f"First seen: {event['first_seen']}", event["screening"]["sponsorship"],
                      "; ".join(event["screening"]["concerns"]), job["url"]])
    return "\n".join(lines)


def discord_url():
    value = os.environ.get("DISCORD_WEBHOOK_URL", "")
    parts = urllib.parse.urlsplit(value)
    if (parts.scheme != "https" or parts.netloc != "discord.com"
            or not re.fullmatch(r"/api(?:/v\d+)?/webhooks/\d+/[A-Za-z0-9._-]+", parts.path)
            or parts.fragment or parts.query):
        raise NotificationError("Set DISCORD_WEBHOOK_URL to a Discord incoming webhook URL")
    return value + "?wait=true"


def shortened(text, limit):
    encoded = text.encode("utf-16-le")
    if len(encoded) <= limit * 2:
        return text
    return encoded[:(limit - 1) * 2].decode("utf-16-le", errors="ignore") + "…"


def concise(value, limit):
    return shortened(" ".join(str(value or "").split()), limit)


def heading(value, limit):
    return re.sub(r"([\\`*_~])", r"\\\1", concise(value, limit))


def relative_timestamp(value, now):
    seconds = timestamp(value)
    if seconds is None or seconds < 0 or seconds > now:
        return None
    return f"<t:{int(seconds)}:R>"


def discord_text(event, *, now=None):
    """Scan the role and freshness, then open the official advert in one tap."""
    job = event["job"]
    now = time.time() if now is None else now
    # Angle brackets protect parentheses in the destination; encode only
    # characters that would break the link, retaining the official URL.
    url = re.sub(r"[\s<>\\]", lambda match: urllib.parse.quote(match.group(), safe=""), job["url"])
    if len(url.encode("utf-16-le")) // 2 > 1000:
        raise NotificationError("Official job URL is too long; review this pending lead")
    lines = [f"**{heading(job['title'], 200)}**"]
    identity = heading(event['company'], 80)
    if job.get("location"):
        identity += " · " + concise(job["location"], 220)
    lines.append(identity)
    context = {"Newly discovered": "",
               "New match on an existing posting": "Existing posting · newly matched",
               "Present at first successful check": "First check · posting may already be older"}.get(event["kind"], event["kind"])
    if context:
        lines.append(concise(context, 120))
    published = relative_timestamp(job.get("published_at"), now)
    detected_at = event.get("queued_at")
    if not detected_at and event["kind"] == "Newly discovered":
        detected_at = event.get("first_seen")
    detected = relative_timestamp(detected_at, now)
    detection_label = "Matched" if event["kind"] == "New match on an existing posting" else "Detected"
    secondary_time = ""
    if published and job.get("date_type") == "first published":
        lines.append("First published " + published)
        if detected:
            secondary_time = "-# " + detection_label + " " + detected
    elif published and job.get("date_type") == "last published":
        lines.append("Last published " + published + " · first publication unknown")
        if detected:
            secondary_time = "-# " + detection_label + " " + detected
    else:
        lines.append("Posting time unavailable" + (" · " + detection_label.lower() + " " + detected if detected else ""))
    previous = timestamp(event.get("previous_check_at"))
    observed = timestamp(detected_at)
    if event["kind"] == "Newly discovered" and detected and previous is not None and observed is not None and observed - previous > 180:
        lines.append("Detected after a monitoring gap; verify posting age.")
    if event.get("source_check_failed"):
        lines.append("Board check unavailable; verify the listing is still open.")
    screening = event["screening"]
    checks = []
    for concern in screening.get("concerns", []):
        if concern == "Check required experience, vacancy sponsorship and start date":
            continue
        label = {"Confirm UK residence is allowed; remote scope is ambiguous": "Confirm UK eligibility for remote work.",
                 "Check experience level and active hiring": "Review seniority and active hiring.",
                 "Check the doctoral requirement": "Review the PhD requirement."}.get(concern, concern)
        checks.append("• " + concise(label, 180))
    sponsor = {"conditional wording — confirm vacancy": "Sponsorship conditional — verify for this role.",
               "advert mentions sponsorship — confirm vacancy": "Sponsorship mentioned — verify for this role."}.get(screening.get("sponsorship"))
    if sponsor:
        checks.append(sponsor)
    if checks:
        lines.extend(["", *checks])
    # Reserve the complete action link and secondary timestamp before bounding
    # metadata, so long titles or checks cannot remove the application path.
    action = "\n\n[View job & apply](<" + url + ">)"
    if secondary_time:
        action += "\n" + secondary_time
    budget = 1900 - len(action.encode("utf-16-le")) // 2
    return shortened("\n".join(lines), budget) + action


def discord_message(text):
    """Shared confirmed delivery for leads and explicit connection tests."""
    request = urllib.request.Request(discord_url(),
        data=json.dumps({"content": text, "allowed_mentions": {"parse": []}}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "PersonalJobMonitor/1.0"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.load(response)
        if not isinstance(result, dict) or not result.get("id"):
            raise NotificationError("Discord did not confirm message creation")
    except urllib.error.HTTPError as exc:
        delay = 0
        if exc.code == 429:
            try:
                delay = float(json.load(exc).get("retry_after", 0))
            except (ValueError, TypeError, AttributeError):
                pass
            if not delay and exc.headers:
                try:
                    delay = float(exc.headers.get("Retry-After", 0))
                except ValueError:
                    pass
        # Never include the exception's URL or response body: they may contain credentials.
        raise NotificationError(f"Discord HTTP {exc.code}; delivery remains pending", delay) from None
    except (urllib.error.URLError, OSError, ValueError) as exc:
        if isinstance(exc, NotificationError):
            raise
        raise NotificationError("Discord network or response failure; delivery remains pending") from None


def discord(events):
    """Send one bounded message; confirm creation before acknowledging the lead."""
    if len(events) != 1:
        raise NotificationError("Discord delivery requires one lead at a time")
    discord_message(discord_text(events[0]))


def sender_for(channel):
    if channel == "discord":
        discord_url()  # Fail before fetching boards if notification configuration is invalid.
        return discord
    if channel == "telegram":
        return telegram
    raise ValueError("Choose discord or telegram for notification delivery")


def test_discord():
    """Explicit live connectivity test; never modifies posting history."""
    discord_message("Job Monitor connection test successful.")
    print("Discord confirmed the test message")


def telegram(events):
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        raise ValueError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable alerts")
    text = alert_text(events)
    if len(text) > 3900:
        raise ValueError("Alert batch is too long; send smaller batches")
    request = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps({"chat_id": chat, "text": text, "disable_web_page_preview": True}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.load(response)
        if not result.get("ok"):
            raise ValueError("Telegram did not confirm delivery")
    except urllib.error.HTTPError as exc:
        raise ValueError(f"Telegram HTTP {exc.code}; delivery remains pending") from None
    except urllib.error.URLError:
        raise ValueError("Telegram network failure; delivery remains pending") from None
