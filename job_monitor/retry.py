"""Interpret server retry hints without owning scheduling or backoff policy."""
from email.utils import parsedate_to_datetime
import math


def retry_after_seconds(headers, now):
    """Return a finite positive delay for seconds or an HTTP date, else zero."""
    value = headers.get("Retry-After", "") if headers else ""
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            seconds = parsedate_to_datetime(value).timestamp() - now
        except (ValueError, TypeError, OverflowError):
            return 0
    return seconds if math.isfinite(seconds) and seconds > 0 else 0
