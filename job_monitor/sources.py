"""Read-only adapters for public employer posting APIs."""
from datetime import datetime, timezone
from html.parser import HTMLParser
import re
import urllib.request
import json
import threading
import time

from job_monitor.screening import screen
from job_monitor.retry import retry_after_seconds


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {"p", "li", "br", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(data)


def plain(value):
    parser = PlainText()
    parser.feed(str(value or ""))
    return "".join(parser.parts)


def located(value, country):
    """Keep provider country metadata attached to its own location."""
    country = {"GB": "United Kingdom", "GBR": "United Kingdom", "UK": "United Kingdom",
               "US": "United States", "USA": "United States", "CA": "Canada", "CAN": "Canada"}.get(country, country)
    return ", ".join(str(part) for part in (value, country) if part)


def fetch(source, timeout=12):
    provider, board = source["provider"], source["board"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", board):
        raise ValueError("Invalid board name")
    endpoints = {
        "greenhouse": f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true",
        "lever": f"https://api.lever.co/v0/postings/{board}?mode=json",
        "ashby": f"https://api.ashbyhq.com/posting-api/job-board/{board}",
    }
    if provider not in endpoints:
        raise ValueError("Unsupported provider")
    request = urllib.request.Request(endpoints[provider], headers={"User-Agent": "PersonalJobMonitor/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return normalise(source, json.load(response))


def normalise(source, payload):
    provider = source["provider"]
    jobs = payload if provider == "lever" else payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        raise ValueError("Response has no valid job list")
    result = []
    for item in jobs:
        if not isinstance(item, dict):
            raise ValueError("Invalid job record; retain previous source state")
        if provider == "ashby" and item.get("isListed") is False:
            continue
        title = item.get("text" if provider == "lever" else "title", "")
        url = item.get({"greenhouse": "absolute_url", "lever": "hostedUrl", "ashby": "jobUrl"}[provider], "")
        if not title or not str(url).startswith("https://"):
            raise ValueError("Job record is missing its title or HTTPS URL")
        location = item.get("location", "")
        published, date_type, updated = "", "unknown", ""
        if provider == "greenhouse":
            location = location.get("name", "") if isinstance(location, dict) else location
            description = plain(item.get("content", ""))
            published = item.get("first_published", "")
            date_type = "first published" if published else "unknown"
            updated = item.get("updated_at", "")
        elif provider == "lever":
            categories = item.get("categories") or {}
            primary_location = str(categories.get("location") or "")
            locations = [located(primary_location, item.get("country"))]
            additional_locations = categories.get("allLocations") or []
            if not isinstance(additional_locations, list) or any(not isinstance(value, str) for value in additional_locations):
                raise ValueError("Invalid locations; retain previous source state")
            locations.extend(value for value in additional_locations if value != primary_location)
            if item.get("workplaceType") == "remote":
                locations.append("Remote")
            location = " | ".join(dict.fromkeys(value for value in locations if value))
            sections = [item.get("descriptionPlain") or plain(item.get("description", ""))]
            sections.extend(str(section.get("text", "")) + "\n" + plain(section.get("content", "")) for section in item.get("lists", []))
            sections.append(item.get("additionalPlain") or plain(item.get("additional", "")))
            description = "\n".join(sections)
            if isinstance(item.get("createdAt"), (int, float)):
                published = datetime.fromtimestamp(item["createdAt"] / 1000, timezone.utc).isoformat()
                date_type = "record created"
        else:
            address = (item.get("address") or {}).get("postalAddress") or {}
            locations = [located(location, address.get("addressCountry"))]
            locations.extend(located(loc.get("location", ""), (loc.get("address") or {}).get("addressCountry"))
                             for loc in item.get("secondaryLocations", []))
            location = " | ".join(locations)
            if item.get("isRemote") or item.get("workplaceType") == "Remote":
                location += " | Remote"
            description = item.get("descriptionPlain") or plain(item.get("descriptionHtml", ""))
            published = item.get("publishedAt", "")
            date_type = "last published" if published else "unknown"
        result.append({"id": str(item.get("id") or url), "title": str(title), "url": str(url),
                       "location": str(location), "description": str(description),
                       "published_at": published or "", "date_type": date_type, "updated_at": updated or ""})
    return result


class PublicationEnricher:
    """At most one bounded Greenhouse detail lookup per board poll, only for age filtering."""
    def __init__(self, *, opener=urllib.request.urlopen, clock=time.time):
        self.opener, self.clock = opener, clock
        self.cache, self.retry = {}, {}
        self.lock = threading.Lock()

    def enrich(self, source, jobs, profile):
        if source["provider"] != "greenhouse" or profile.get("max_posting_age_days") is None:
            return jobs
        board = source["board"]
        basic = {**profile, "max_posting_age_days": None}
        result = [dict(job) for job in jobs]
        attempted = False
        for job in result:
            key = (board, job["id"])
            if job.get("date_type") == "first published" or not str(job["id"]).isdigit() or not screen(job, basic):
                continue
            now = self.clock()
            with self.lock:
                cached = self.cache.get(key)
                blocked = max(self.retry.get(key, 0), self.retry.get(board, 0)) > now
            if cached:
                job.update(published_at=cached, date_type="first published")
                continue
            if attempted or blocked:
                continue
            attempted = True
            url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{job['id']}"
            request = urllib.request.Request(url, headers={"User-Agent": "PersonalJobMonitor/1.0"})
            try:
                with self.opener(request, timeout=3) as response:
                    detail = normalise(source, {"jobs": [json.load(response)]})[0]
                if detail["id"] != job["id"] or detail["url"] != job["url"]:
                    raise ValueError("Detail does not match the requested posting")
                if detail.get("date_type") == "first published":
                    job.update(published_at=detail["published_at"], date_type="first published")
                    with self.lock:
                        self.cache[key] = detail["published_at"]
                else:
                    with self.lock:
                        self.retry[key] = now + 3600
            except (OSError, ValueError, KeyError, TypeError) as error:
                delay = max(300, retry_after_seconds(getattr(error, "headers", None), now))
                with self.lock:
                    self.retry[key] = now + delay
                    if getattr(error, "code", 0) == 429:
                        self.retry[board] = now + delay
        return result
