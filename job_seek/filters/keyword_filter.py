"""
job_seek/filters/keyword_filter.py
------------------------------------
Rule-based CS / Software / IT job classifier.

Design
------
Each job is scored 0–100 based on three signals:

  1. Title signal   (0–60 pts)  — job title contains a strong tech keyword
  2. Boost signal   (0–20 pts)  — job description mentions tech skills/tools
  3. Penalty signal (0–30 pts)  — job title / description contains reject keywords
                                   (penalties subtract from score)

  Final score = clamp(title_score + boost_score - penalty_score, 0, 100)

  A job is KEPT when relevance_score >= THRESHOLD (default 70).

Extending
---------
  Add new accept keywords to TECH_TITLE_KEYWORDS or TECH_BOOST_KEYWORDS.
  Add new reject keywords to REJECT_KEYWORDS.
  Bump weights in _score_record() if needed.
"""

from __future__ import annotations

import re
from typing import Any

from job_seek.utils.logger import get_logger

log = get_logger(__name__)

RELEVANCE_THRESHOLD = 70  # minimum score to keep a job

# ─────────────────────────────────────────────────────────────────────────────
# ACCEPT KEYWORDS  (job title hits)
# Each entry: (regex pattern, score, technical_category label)
# ─────────────────────────────────────────────────────────────────────────────
_TITLE_RULES: list[tuple[re.Pattern, int, str]] = [
    # Software Engineering
    (re.compile(r"\b(software engineer(ing)?|software developer|software dev)\b", re.I), 75, "Software Engineering"),
    (re.compile(r"\b(swe|sde)\b", re.I), 73, "Software Engineering"),
    # Backend
    (re.compile(r"\b(backend|back.end|back end)\b", re.I), 73, "Backend Engineering"),
    (re.compile(r"\b(api engineer|platform engineer)\b", re.I), 72, "Backend Engineering"),
    # Frontend
    (re.compile(r"\b(frontend|front.end|front end|ui engineer|ux engineer)\b", re.I), 73, "Frontend Engineering"),
    (re.compile(r"\b(react developer|vue developer|angular developer)\b", re.I), 72, "Frontend Engineering"),
    # Full Stack
    (re.compile(r"\b(full.?stack|full stack)\b", re.I), 73, "Full Stack Development"),
    # Web
    (re.compile(r"\b(web developer|web engineer|web dev)\b", re.I), 71, "Web Development"),
    # Mobile
    (re.compile(r"\b(mobile developer|mobile engineer|ios developer|android developer|flutter|react native)\b", re.I), 73, "Mobile Development"),
    # DevOps / SRE / Cloud
    (re.compile(r"\b(devops|dev.ops)\b", re.I), 73, "DevOps"),
    (re.compile(r"\b(site reliability|sre)\b", re.I), 73, "SRE"),
    (re.compile(r"\b(cloud engineer|cloud architect|infrastructure engineer)\b", re.I), 73, "Cloud Engineering"),
    (re.compile(r"\b(platform engineer|systems engineer)\b", re.I), 71, "Systems Engineering"),
    # Data
    (re.compile(r"\b(data engineer|data pipeline|etl engineer)\b", re.I), 73, "Data Engineering"),
    (re.compile(r"\b(data scientist|data science|ml engineer|machine learning engineer)\b", re.I), 75, "Data Science / ML"),
    (re.compile(r"\b(ai engineer|artificial intelligence engineer|nlp engineer)\b", re.I), 75, "Data Science / ML"),
    (re.compile(r"\b(data analyst|analytics engineer|bi engineer|business intelligence engineer)\b", re.I), 71, "Database / BI"),
    # Security
    (re.compile(r"\b(security engineer|cybersecurity|infosec|appsec|application security|penetration tester|pentest)\b", re.I), 73, "Cybersecurity"),
    # QA
    (re.compile(r"\b(qa engineer|test engineer|automation engineer|quality assurance engineer|sdet)\b", re.I), 72, "QA / Test Automation"),
    # IT
    (re.compile(r"\b(it support|it administrator|it admin|systems administrator|sysadmin)\b", re.I), 71, "IT Support / Admin"),
    (re.compile(r"\b(network engineer|network administrator|network architect)\b", re.I), 71, "Network Engineering"),
    (re.compile(r"\b(database administrator|dba|database engineer)\b", re.I), 71, "Database / BI"),
    # Embedded / Firmware
    (re.compile(r"\b(embedded engineer|firmware engineer|embedded systems|rtos)\b", re.I), 73, "Embedded / Firmware"),
    # Generic technical titles — need description boost to clear 70
    (re.compile(r"\b(engineer|developer|programmer)\b", re.I), 50, "Software Engineering"),
    # Internships — only kept if there's a strong tech description boost
    (re.compile(r"\b(intern|internship|co.?op)\b", re.I), 30, "Intern"),
]

# ─────────────────────────────────────────────────────────────────────────────
# BOOST KEYWORDS  (description / title mentions — add up to 20 pts)
# ─────────────────────────────────────────────────────────────────────────────
_BOOST_KEYWORDS: list[re.Pattern] = [
    re.compile(r"\b(python|java|golang|go|rust|c\+\+|c#|ruby|scala|kotlin|swift|typescript|javascript|php)\b", re.I),
    re.compile(r"\b(django|flask|fastapi|spring|rails|laravel|express|next\.?js|nuxt|nest\.?js)\b", re.I),
    re.compile(r"\b(react|vue|angular|svelte|tailwind|css|html)\b", re.I),
    re.compile(r"\b(kubernetes|k8s|docker|terraform|ansible|helm|ci/cd|jenkins|github actions|gitlab)\b", re.I),
    re.compile(r"\b(aws|gcp|azure|cloud|s3|lambda|ec2|gke)\b", re.I),
    re.compile(r"\b(sql|postgresql|postgres|mysql|mongodb|redis|elasticsearch|cassandra|dynamodb)\b", re.I),
    re.compile(r"\b(machine learning|deep learning|neural network|pytorch|tensorflow|hugging face|llm|nlp|cv)\b", re.I),
    re.compile(r"\b(git|linux|bash|shell|unix|api|rest|graphql|grpc|microservice)\b", re.I),
    re.compile(r"\b(agile|scrum|sprint|jira|pull request|code review)\b", re.I),
    re.compile(r"\b(unit test|integration test|selenium|cypress|playwright|pytest|jest)\b", re.I),
]
_BOOST_PER_HIT = 4   # max 5 hits × 4 = 20 pts
_BOOST_MAX = 20

# ─────────────────────────────────────────────────────────────────────────────
# REJECT KEYWORDS  (strong signal this is a non-technical role)
# ─────────────────────────────────────────────────────────────────────────────
_REJECT_RULES: list[tuple[re.Pattern, int, str]] = [
    (re.compile(r"\b(sales|account executive|ae|account manager|bdr|sdr|business development)\b", re.I), 60, "Sales role"),
    (re.compile(r"\b(marketing|seo specialist|content writer|copywriter|brand manager|growth marketer)\b", re.I), 60, "Marketing role"),
    (re.compile(r"\b(recruiter|talent acquisition|sourcer|hr |human resources|people ops)\b", re.I), 60, "HR / Recruiting role"),
    (re.compile(r"\b(finance|accountant|controller|cfo|bookkeeper|payroll)\b", re.I), 60, "Finance role"),
    (re.compile(r"\b(customer success|customer support|customer service|support specialist|help desk agent)\b", re.I), 50, "Customer Success / Support"),
    (re.compile(r"\b(legal|counsel|attorney|paralegal|compliance officer)\b", re.I), 55, "Legal role"),
    (re.compile(r"\b(graphic design|ux designer|ui designer|visual designer|illustrat)\b", re.I), 40, "Design / Creative role"),
    (re.compile(r"\b(operations manager|office manager|executive assistant|administrative assistant|receptionist)\b", re.I), 60, "Admin / Operations"),
    (re.compile(r"\b(product manager|project manager|program manager|scrum master|agile coach)\b", re.I), 30, "Non-technical management"),
    (re.compile(r"\b(chef|cook|driver|warehouse|logistics|supply chain)\b", re.I), 60, "Non-tech role"),
]


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def _score_record(record: dict[str, Any]) -> tuple[int, str, str]:
    """
    Returns (score: int, technical_category: str, rejection_reason: str).
    """
    title = str(record.get("job_title", ""))
    description = str(record.get("short_summary", ""))
    search_text = f"{title} {description}"

    # ── Title score ───────────────────────────────────────────────────────────
    # Scan all rules and take the highest-scoring match.
    # This ensures "Frontend Developer" matches the specific frontend rule
    # (58 pts) rather than falling through to the generic "developer" rule
    # (40 pts).
    title_score = 0
    category = "Unknown"
    for pattern, pts, cat in _TITLE_RULES:
        if pattern.search(title):
            if pts > title_score:
                title_score = pts
                category = cat

    # ── Boost score ───────────────────────────────────────────────────────────
    boost_hits = sum(1 for p in _BOOST_KEYWORDS if p.search(search_text))
    boost_score = min(boost_hits * _BOOST_PER_HIT, _BOOST_MAX)

    # ── Penalty / reject ─────────────────────────────────────────────────────
    penalty = 0
    rejection_reason = ""
    for pattern, pts, reason in _REJECT_RULES:
        if pattern.search(title):
            penalty = max(penalty, pts)
            rejection_reason = reason

    raw_score = title_score + boost_score - penalty
    final_score = max(0, min(100, raw_score))

    return final_score, category, rejection_reason


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def classify_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Mutate *record* in-place to add:
        relevance_score, technical_category, keep_or_reject, rejection_reason
    Returns the modified record.
    """
    score, category, rejection_reason = _score_record(record)

    record["relevance_score"] = score
    record["technical_category"] = category if category != "Unknown" else record.get("technical_category", "Unknown")

    if score >= RELEVANCE_THRESHOLD:
        record["keep_or_reject"] = "Keep"
        record["rejection_reason"] = ""
    else:
        record["keep_or_reject"] = "Reject"
        record["rejection_reason"] = rejection_reason or "Low relevance score"

    return record


def filter_jobs(
    records: list[dict[str, Any]],
    threshold: int = RELEVANCE_THRESHOLD,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Classify every record and split into (kept, rejected).

    Parameters
    ----------
    records   : list of normalised job dicts
    threshold : minimum relevance_score to keep (default 70)

    Returns
    -------
    kept     : records with relevance_score >= threshold
    rejected : records below threshold (still have score & reason attached)
    """
    kept: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for rec in records:
        classify_record(rec)
        if rec["relevance_score"] >= threshold:
            kept.append(rec)
        else:
            rejected.append(rec)

    log.info(
        "Filtering complete: %d kept / %d rejected (threshold=%d)",
        len(kept), len(rejected), threshold,
    )
    return kept, rejected
