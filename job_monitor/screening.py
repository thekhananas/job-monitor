"""Cheap lead screening; no resume scoring or eligibility verdicts."""
import re
from datetime import datetime, timezone


DEFAULT_EXCLUDED_TITLES = ["principal", "director", "head of", "engineering manager",
                           "software engineering manager"]
DEFAULT_REMOTE_REGIONS = ["europe", "emea", "worldwide", "global", "anywhere"]
REJECTION_LABELS = {
    "location": "Location does not meet your saved location/remote rules",
    "title": "Title does not match an allowed phrase",
    "excluded_title": "Title matches an excluded phrase",
    "staff_level": "Staff-level title is excluded (except member of technical staff)",
    "sponsorship": "Advert contains explicit negative sponsorship wording",
    "posting_age": "First publication is older than your posting-age limit",
    "posting_date_unknown": "First-publication date is unavailable or invalid",
}


def normal_phrase(value):
    value = re.sub(r"[-_/–—]", " ", value.casefold())
    value = re.sub(r"\bbackend\b", "back end", value)
    value = re.sub(r"\bfrontend\b", "front end", value)
    value = re.sub(r"\bfullstack\b", "full stack", value)
    return " ".join(value.split())


def has_phrase(value, phrases):
    return any(re.search(r"(?<!\w)" + re.escape(normal_phrase(phrase)) + r"(?!\w)", value)
               for phrase in phrases)


def validate_profile(profile):
    for name in ("title_keywords", "excluded_title_keywords", "ambiguous_remote_regions", "locations"):
        if name not in profile:
            continue
        values = profile[name]
        if (not isinstance(values, list) or any(not isinstance(value, str) or not value.strip() for value in values)
                or name in ("title_keywords", "locations") and not values):
            raise ValueError(f"{name} must contain nonempty text phrases")
    for name in ("allow_uk_remote", "allow_ambiguous_remote"):
        if name in profile and not isinstance(profile[name], bool):
            raise ValueError(f"{name} must be true or false")
    days = profile.get("max_posting_age_days")
    if days is not None and (type(days) is not int or not 1 <= days <= 365):
        raise ValueError("max_posting_age_days must be null or an integer from 1 to 365")


def first_publication(job):
    if job.get("date_type") != "first published":
        return None
    try:
        value = datetime.fromisoformat(job.get("published_at", "").replace("Z", "+00:00"))
        return value if value.tzinfo is not None else None
    except (ValueError, TypeError, AttributeError):
        return None


def posting_age_rejection(job, profile, now):
    days = profile.get("max_posting_age_days")
    if days is None:
        return None
    published = first_publication(job)
    if published is None or published > now:
        return "posting_date_unknown"
    if (now - published).total_seconds() > days * 86400:
        return "posting_age"
    return None


def location_eligibility(location, profile):
    """Advisory UK eligibility, with opt-in inclusion of ambiguous remote scopes."""
    location = normal_phrase(location)
    # A London in another country is not the London target; assess each listed location.
    locations = [part.strip() for part in re.split(r"[|;]", location) if part.strip()]
    targets = profile.get("locations", ["London"])
    local = any(has_phrase(part, targets) and not re.search(
        r"\b(?:ontario|canada|united states|usa|ohio|kentucky)\b|,\s*(?:ca|can|us)\b", part)
        for part in locations)
    if local:
        return next(target for target in targets if has_phrase(location, [target]))
    if not profile.get("allow_uk_remote", True) or not re.search(r"\bremote\b", location):
        return None
    # Broad regional labels do not override explicit country-only restrictions.
    if re.search(r"\b(?:us|usa|united states|canada|eu|european union) only\b|"
                 r"\bonly (?:us|usa|united states|canada|eu|european union)\b", location):
        return None
    if re.search(r"\b(?:uk|united kingdom|great britain|england|scotland|wales|northern ireland)\b", location):
        return "UK remote — confirm employer requirements"
    if profile.get("allow_ambiguous_remote", False):
        broad_region = has_phrase(location, profile.get("ambiguous_remote_regions", DEFAULT_REMOTE_REGIONS))
        unspecified = bool(locations) and all(re.fullmatch(
            r"(?:fully |100% )?remote(?: work| working| only)?", part) for part in locations)
        if broad_region or unspecified:
            return "Remote — UK eligibility unconfirmed"
    return None


def screen(job, profile, *, now=None):
    return screen_with_reasons(job, profile, now=now)[0]


def screen_with_reasons(job, profile, *, now=None):
    """Return the existing match and all blocking rules, without changing policy."""
    title = normal_phrase(job["title"])
    text = " ".join(job["description"].casefold().split())
    eligibility = location_eligibility(job["location"], profile)
    keywords = profile["title_keywords"]
    reasons = []
    if not eligibility:
        reasons.append("location")
    if not has_phrase(title, keywords):
        reasons.append("title")
    if has_phrase(title, profile.get("excluded_title_keywords", DEFAULT_EXCLUDED_TITLES)):
        reasons.append("excluded_title")
    if re.search(r"\bstaff\b", title) and "member of technical staff" not in title:
        reasons.append("staff_level")
    no_sponsor = re.search(r"\b(?:no visa sponsorship|no sponsorship|sponsorship is not available|sponsorship not available)\b|\b(?:do not|does not|cannot|can't|unable to|not able to|will not) (?:offer|provide) (?:visa )?sponsorship\b|\b(?:cannot|can't|unable to|will not) sponsor\b", text)
    if no_sponsor:
        reasons.append("sponsorship")
    age_reason = posting_age_rejection(job, profile, datetime.now(timezone.utc) if now is None else now)
    if age_reason:
        reasons.append(age_reason)
    if reasons:
        return None, reasons
    conditional = re.search(r"\b(?:case.by.case|not guaranteed|isn't guaranteed|isn’t guaranteed|may offer|may provide)\b", text)
    positive = re.search(r"\bvisa sponsorship (?:is )?(?:available|provided|offered)\b|\b(?:offer|provide) (?:visa )?sponsorship\b|\bsponsor (?:uk |work )?visas?\b", text)
    signal = "conditional wording — confirm vacancy" if conditional and "sponsor" in text else "advert mentions sponsorship — confirm vacancy" if positive else "unknown"
    concerns = []
    if eligibility == "Remote — UK eligibility unconfirmed":
        concerns.append("Confirm UK residence is allowed; remote scope is ambiguous")
    if re.search(r"\b(?:senior|lead|intern|internship|research scientist|talent pool|expression of interest)\b", title):
        concerns.append("Check experience level and active hiring")
    years = [int(value) for value in re.findall(r"\b(\d{1,2})\+? years? (?:of (?:relevant |professional )?experience|experience|building|shipping|working|as|in)\b", text)]
    if years and max(years) > profile["experience_years"]:
        concerns.append(f"Advert mentions {max(years)}+ years; check the requirement")
    if re.search(r"\bph\.?d\.? (?:is )?required\b", text):
        concerns.append("Check the doctoral requirement")
    return {"sponsorship": signal, "location_eligibility": eligibility,
            "concerns": concerns or ["Check required experience, vacancy sponsorship and start date"]}, []
