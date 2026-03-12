"""
job_seek/utils/deduplication.py
--------------------------------
Removes duplicate job records.

A duplicate is defined as two records sharing the same:
    (company_name, job_title, location, job_url)   — primary key
OR the same:
    (company_name, job_title, location)             — fuzzy key
    when job_url is missing / "Unknown" on one side.

The first occurrence of a duplicate is kept; subsequent ones are dropped.
"""

from __future__ import annotations

import re
from typing import Any

from job_seek.utils.logger import get_logger

log = get_logger(__name__)


def _norm(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace — for fuzzy matching."""
    s = str(s).lower().strip()
    s = re.sub(r"[^\w\s]", "", s)
    return re.sub(r"\s+", " ", s)


def deduplicate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Return a new list with duplicates removed.
    Logs how many records were dropped.
    """
    seen_exact: set[tuple] = set()
    seen_fuzzy: set[tuple] = set()
    kept: list[dict[str, Any]] = []

    for rec in records:
        url = rec.get("job_url", "Unknown") or "Unknown"
        company = _norm(rec.get("company_name", ""))
        title = _norm(rec.get("job_title", ""))
        location = _norm(rec.get("location", ""))

        exact_key = (company, title, location, _norm(url))
        fuzzy_key = (company, title, location)

        if exact_key in seen_exact:
            log.debug("Duplicate (exact): %s @ %s", rec.get("job_title"), rec.get("company_name"))
            continue

        if url in ("unknown", "") and fuzzy_key in seen_fuzzy:
            log.debug("Duplicate (fuzzy): %s @ %s", rec.get("job_title"), rec.get("company_name"))
            continue

        seen_exact.add(exact_key)
        seen_fuzzy.add(fuzzy_key)
        kept.append(rec)

    dropped = len(records) - len(kept)
    if dropped:
        log.info("Deduplication: %d records removed (%d kept)", dropped, len(kept))
    else:
        log.info("Deduplication: no duplicates found (%d records)", len(kept))

    return kept
