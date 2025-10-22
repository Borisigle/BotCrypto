from __future__ import annotations

"""Utilities related to trading session bucketing."""

from datetime import datetime

__all__ = ["determine_session"]


def determine_session(timestamp: datetime) -> str:
    """Return the trading session label for the supplied timestamp.

    The service models three major trading sessions using UTC hour buckets:

    * ``asia``   – 00:00 <= hour < 08:00
    * ``london`` – 08:00 <= hour < 16:00
    * ``new_york`` – remaining hours

    Args:
        timestamp: Timestamp to classify. The function works with both naive and
            timezone-aware datetimes as long as they represent UTC wall-clock
            time.

    Returns:
        Session label (``asia``, ``london`` or ``new_york``).
    """

    hour = timestamp.hour
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 16:
        return "london"
    return "new_york"
