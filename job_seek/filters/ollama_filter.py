"""
job_seek/filters/ollama_filter.py
-----------------------------------
Optional LLM-assisted job classifier using a locally-running Ollama model.

Setup
-----
  1. Install Ollama: https://ollama.com/download
  2. Pull a model:   ollama pull mistral
  3. Run pipeline:   python scripts/run_pipeline.py --use-ollama

The Ollama server must be running (`ollama serve`) before the pipeline starts.

Fallback behaviour
------------------
If Ollama is unreachable or returns a malformed response, the module
automatically falls back to the keyword classifier for that record and
logs a warning.  The pipeline never crashes due to Ollama issues.

LLM prompt contract
--------------------
The model is asked to return a single JSON object:
  {
    "relevance_score": <int 0-100>,
    "technical_category": "<string>",
    "keep_or_reject": "Keep" | "Reject",
    "rejection_reason": "<string or empty>",
    "short_summary": "<1-2 sentence summary>",
    "required_skills": "<semicolon-separated>",
    "preferred_skills": "<semicolon-separated>"
  }
"""

from __future__ import annotations

import json
import re
from typing import Any

from job_seek.utils.logger import get_logger
from job_seek.filters.keyword_filter import classify_record as kw_classify, RELEVANCE_THRESHOLD

log = get_logger(__name__)

try:
    import httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "mistral"   # change to llama3, phi3, etc. as needed
OLLAMA_TIMEOUT = 60        # seconds per request

_SYSTEM_PROMPT = """You are a job relevance classifier for a software engineer job search.
Your task is to decide whether a job listing is related to Computer Science, Software, or IT.

Categories to KEEP:
Software Engineering, Backend Engineering, Frontend Engineering, Full Stack Development,
Web Development, Mobile Development, DevOps, Cloud Engineering, SRE, Data Engineering,
Data Science, Machine Learning / AI, Cybersecurity, QA / Test Automation, IT Support,
IT Administration, Systems Engineering, Network Engineering, Database / BI,
Embedded / Firmware, Technical Internships.

Categories to REJECT:
Sales, Marketing, HR, Recruiting, Finance, Customer Success, Admin, Legal, Operations,
Graphic Design, Non-technical support, Non-technical management.

Respond ONLY with a valid JSON object and no other text. Use this exact schema:
{
  "relevance_score": <integer 0-100>,
  "technical_category": "<string from kept categories or 'Non-technical'>",
  "keep_or_reject": "<'Keep' or 'Reject'>",
  "rejection_reason": "<empty string if keeping, else brief reason>",
  "short_summary": "<1-2 sentence plain-English summary of the role>",
  "required_skills": "<semicolon-separated list of skills mentioned as required>",
  "preferred_skills": "<semicolon-separated list of nice-to-have skills>"
}
"""

_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _call_ollama(title: str, description: str) -> dict[str, Any] | None:
    """Send one request to Ollama. Returns parsed dict or None on failure."""
    if not _HTTPX_AVAILABLE:
        log.warning("httpx not installed; cannot call Ollama")
        return None

    user_message = (
        f"Job title: {title}\n\n"
        f"Description:\n{description[:2000]}"   # cap at 2 000 chars
    )
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"{_SYSTEM_PROMPT}\n\nUser:\n{user_message}",
        "stream": False,
        "format": "json",
    }

    try:
        with httpx.Client(timeout=OLLAMA_TIMEOUT) as client:
            resp = client.post(OLLAMA_URL, json=payload)
            resp.raise_for_status()
            raw_text: str = resp.json().get("response", "")
    except Exception as exc:
        log.warning("Ollama request failed (%s): %s", type(exc).__name__, exc)
        return None

    # Extract JSON from response (model might wrap in markdown fences)
    match = _JSON_RE.search(raw_text)
    if not match:
        log.warning("Ollama returned no JSON in response: %s", raw_text[:200])
        return None

    try:
        return json.loads(match.group())
    except json.JSONDecodeError as exc:
        log.warning("Ollama JSON parse error: %s", exc)
        return None


def classify_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    LLM-powered classify. Falls back to keyword classifier on any failure.
    Mutates *record* in-place, returns it.
    """
    title = str(record.get("job_title", ""))
    description = str(record.get("short_summary", ""))

    result = _call_ollama(title, description)

    if result is None:
        log.debug("Falling back to keyword classifier for: %s", title)
        return kw_classify(record)

    # Populate record from LLM output (with fallbacks)
    try:
        record["relevance_score"] = int(result.get("relevance_score", 0))
    except (TypeError, ValueError):
        record["relevance_score"] = 0

    record["technical_category"] = result.get("technical_category", "Unknown") or "Unknown"
    record["keep_or_reject"] = result.get("keep_or_reject", "Reject")
    record["rejection_reason"] = result.get("rejection_reason", "") or ""
    record["short_summary"] = result.get("short_summary", record.get("short_summary", "")) or ""
    record["required_skills"] = result.get("required_skills", "") or ""
    record["preferred_skills"] = result.get("preferred_skills", "") or ""

    # Ensure keep_or_reject is consistent with the threshold
    if record["relevance_score"] < RELEVANCE_THRESHOLD and record["keep_or_reject"] == "Keep":
        record["keep_or_reject"] = "Reject"
        record["rejection_reason"] = record["rejection_reason"] or "Score below threshold"

    return record


def filter_jobs(
    records: list[dict[str, Any]],
    threshold: int = RELEVANCE_THRESHOLD,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Same interface as keyword_filter.filter_jobs."""
    kept, rejected = [], []

    for i, rec in enumerate(records):
        classify_record(rec)
        if rec["relevance_score"] >= threshold:
            kept.append(rec)
        else:
            rejected.append(rec)

        if (i + 1) % 10 == 0:
            log.info("Ollama filtering progress: %d / %d", i + 1, len(records))

    log.info(
        "Ollama filtering complete: %d kept / %d rejected (threshold=%d)",
        len(kept), len(rejected), threshold,
    )
    return kept, rejected
