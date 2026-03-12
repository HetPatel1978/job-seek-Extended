"""
job_seek/utils/normalizer.py
-----------------------------
Converts raw scraped dicts into a canonical JobRecord dict with every
expected output column present (unknown fields → "Unknown").
"""

from __future__ import annotations

import re
from typing import Any

from job_seek.utils.logger import get_logger

log = get_logger(__name__)

# ── Output schema with default values ────────────────────────────────────────
OUTPUT_COLUMNS: list[str] = [
    "company_name",
    "job_title",
    "location",
    "country",
    "work_type",
    "employment_type",
    "seniority_level",
    "department",
    "job_url",
    "source_site",
    "date_posted",
    "salary",
    "visa_sponsorship",
    "requires_degree",
    "technical_category",
    "relevance_score",
    "keep_or_reject",
    "rejection_reason",
    "short_summary",
    "required_skills",
    "preferred_skills",
]

_DEFAULTS: dict[str, Any] = {col: "Unknown" for col in OUTPUT_COLUMNS}
_DEFAULTS.update(
    {
        "relevance_score": 0,
        "keep_or_reject": "Pending",
        "rejection_reason": "",
        "short_summary": "",
        "required_skills": "",
        "preferred_skills": "",
    }
)

# ── Seniority keyword map ─────────────────────────────────────────────────────
_SENIORITY_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(intern|internship|co.?op|student)\b", re.I), "Intern"),
    (re.compile(r"\b(junior|jr\.?|entry.?level|associate)\b", re.I), "Junior"),
    (re.compile(r"\b(senior|sr\.?|staff|principal|lead)\b", re.I), "Senior"),
    (re.compile(r"\b(manager|director|head of|vp|vice president)\b", re.I), "Manager"),
    (re.compile(r"\b(mid.?level|mid|ii|iii)\b", re.I), "Mid"),
]

# ── Country hints from location string ───────────────────────────────────────
_COUNTRY_HINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(usa?|united states|us)\b", re.I), "USA"),
    (re.compile(r"\b(uk|united kingdom|england|london)\b", re.I), "UK"),
    (re.compile(r"\b(canada|ca)\b", re.I), "Canada"),
    (re.compile(r"\b(germany|deutschland|berlin|munich)\b", re.I), "Germany"),
    (re.compile(r"\b(france|paris)\b", re.I), "France"),
    (re.compile(r"\b(australia|sydney|melbourne)\b", re.I), "Australia"),
    (re.compile(r"\b(india|bangalore|hyderabad|pune|mumbai)\b", re.I), "India"),
    (re.compile(r"\b(remote)\b", re.I), "Remote / Global"),
]

_REMOTE_RE = re.compile(r"\b(remote|work from home|wfh|distributed)\b", re.I)
_HYBRID_RE = re.compile(r"\b(hybrid)\b", re.I)


def _infer_seniority(title: str) -> str:
    for pattern, label in _SENIORITY_PATTERNS:
        if pattern.search(title):
            return label
    return "Unknown"


def _infer_country(location: str) -> str:
    for pattern, country in _COUNTRY_HINTS:
        if pattern.search(location):
            return country
    return "Unknown"


def _infer_work_type(location: str, title: str) -> str:
    text = f"{location} {title}"
    if _REMOTE_RE.search(text):
        return "Remote"
    if _HYBRID_RE.search(text):
        return "Hybrid"
    return "On-site"


def normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Accept a raw dict (keys vary by adapter) and return a fully-populated
    canonical record with all OUTPUT_COLUMNS present.
    """
    record: dict[str, Any] = dict(_DEFAULTS)

    # ── Direct field mapping (adapters may use different keys) ───────────────
    field_aliases: dict[str, list[str]] = {
        "company_name": ["company_name", "company", "employer"],
        "job_title": ["job_title", "title", "position", "role"],
        "location": ["location", "office", "city"],
        "job_url": ["job_url", "url", "link", "apply_url"],
        "source_site": ["source_site", "ats", "board", "source"],
        "date_posted": ["date_posted", "posted_at", "created_at", "posted_date"],
        "salary": ["salary", "compensation", "pay", "salary_range"],
        "department": ["department", "team", "division"],
        "employment_type": ["employment_type", "type", "job_type", "contract_type"],
        "visa_sponsorship": ["visa_sponsorship", "visa", "sponsorship"],
        "requires_degree": ["requires_degree", "degree", "education"],
        "required_skills": ["required_skills", "skills", "requirements"],
        "preferred_skills": ["preferred_skills", "nice_to_have"],
        "short_summary": ["short_summary", "summary", "description", "snippet"],
    }

    for canonical, aliases in field_aliases.items():
        for alias in aliases:
            val = raw.get(alias)
            if val and str(val).strip() and str(val).strip().lower() != "unknown":
                record[canonical] = str(val).strip()
                break

    # ── Inferred fields ───────────────────────────────────────────────────────
    title = record["job_title"]
    location = record["location"]

    if record["seniority_level"] == "Unknown":
        record["seniority_level"] = _infer_seniority(title)

    if record["country"] == "Unknown":
        record["country"] = _infer_country(location)

    if record["work_type"] == "Unknown":
        record["work_type"] = _infer_work_type(location, title)

    # ── Skills: ensure semicolon-separated strings ────────────────────────────
    for skill_col in ("required_skills", "preferred_skills"):
        val = record[skill_col]
        if isinstance(val, list):
            record[skill_col] = "; ".join(str(s) for s in val if s)
        elif val and val != "Unknown":
            # normalise commas → semicolons
            record[skill_col] = re.sub(r"\s*,\s*", "; ", str(val))

    return record
