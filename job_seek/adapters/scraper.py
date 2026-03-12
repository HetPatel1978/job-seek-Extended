"""
job_seek/adapters/scraper.py
------------------------------
Thin wrappers around the base repo's ATS adapters.

The base repo (viktor-shcherb/job-seek) already provides:
  - LeverAdapter
  - GreenhouseAdapter
  - WorkdayAdapter
  - AshbyAdapter

This module:
  1. Tries to import each base adapter
  2. If a base adapter is not available (e.g. the file hasn't been cloned yet),
     falls back to a self-contained HTTP scraper that works out-of-the-box
  3. Provides a single unified scrape_company() function used by run_pipeline.py

Each adapter returns a list of dicts with at minimum:
    job_title, job_url, location, company_name, source_site
"""

from __future__ import annotations

import time
from typing import Any

from job_seek.utils.logger import get_logger

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Attempt to import base-repo adapters
# ─────────────────────────────────────────────────────────────────────────────
try:
    from job_seek.lever import LeverAdapter as _BaseLever          # type: ignore[import]
    _LEVER_BASE = True
except ImportError:
    _LEVER_BASE = False

try:
    from job_seek.greenhouse import GreenhouseAdapter as _BaseGH   # type: ignore[import]
    _GH_BASE = True
except ImportError:
    _GH_BASE = False

try:
    from job_seek.workday import WorkdayAdapter as _BaseWD         # type: ignore[import]
    _WD_BASE = True
except ImportError:
    _WD_BASE = False

try:
    from job_seek.ashby import AshbyAdapter as _BaseAshby          # type: ignore[import]
    _ASHBY_BASE = True
except ImportError:
    _ASHBY_BASE = False


# ─────────────────────────────────────────────────────────────────────────────
# Fallback HTTP scrapers (no external dependencies beyond requests + bs4)
# ─────────────────────────────────────────────────────────────────────────────

def _get_session():
    """Return a requests.Session with a realistic User-Agent."""
    import requests
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _scrape_lever(company_name: str, url: str) -> list[dict[str, Any]]:
    """
    Lever public JSON API.

    Lever has two API formats — we try both:
      v0 (older):  https://api.lever.co/v0/postings/{slug}?mode=json&limit=250
      v1 (newer):  same endpoint, same slug — some tenants only respond to v0

    The slug must match what's on jobs.lever.co exactly.
    e.g. https://jobs.lever.co/netflix  →  slug = "netflix"

    If the company has migrated off Lever, you'll get a 404.
    In that case remove it from companies.json and use its actual ATS.
    """
    if _LEVER_BASE:
        try:
            adapter = _BaseLever(url)
            return adapter.get_jobs()
        except Exception as exc:
            log.warning("Base LeverAdapter failed (%s), using fallback", exc)

    import requests

    # Extract slug from URL e.g. https://jobs.lever.co/netflix → netflix
    slug = url.rstrip("/").split("/")[-1]

    session = _get_session()
    session.headers.update({"Accept": "application/json"})

    # Lever paginates via `offset` param when there are > 250 results
    all_jobs: list[dict[str, Any]] = []
    offset = 0

    while True:
        api_url = (
            f"https://api.lever.co/v0/postings/{slug}"
            f"?mode=json&limit=250&offset={offset}"
        )
        try:
            resp = session.get(api_url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.error("Lever API error for %s: %s", company_name, exc)
            break

        if not data:
            break

        for item in data:
            categories = item.get("categories", {})
            # createdAt is a Unix ms timestamp — convert to ISO date string
            created_ms = item.get("createdAt")
            date_posted = "Unknown"
            if created_ms:
                try:
                    from datetime import datetime, timezone
                    date_posted = datetime.fromtimestamp(
                        int(created_ms) / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                except Exception:
                    date_posted = str(created_ms)

            all_jobs.append({
                "company_name": company_name,
                "job_title": item.get("text", "Unknown"),
                "job_url": item.get("hostedUrl", url),
                "location": categories.get("location", "Unknown"),
                "department": categories.get("department", "Unknown"),
                "employment_type": categories.get("commitment", "Unknown"),
                "seniority_level": categories.get("level", "Unknown"),
                "date_posted": date_posted,
                "short_summary": item.get("descriptionPlain", "")[:600],
                "source_site": "Lever",
            })

        # If we got fewer than 250, we're on the last page
        if len(data) < 250:
            break
        offset += len(data)

    log.info("Lever: scraped %d jobs for %s", len(all_jobs), company_name)
    return all_jobs


def _scrape_greenhouse(company_name: str, url: str) -> list[dict[str, Any]]:
    """
    Greenhouse public JSON API.
    Endpoint: https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true

    The slug is the last path segment of boards.greenhouse.io/{slug}.
    If you get a 404, the company has migrated off Greenhouse.
    Check their careers page and update companies.json accordingly.
    """
    if _GH_BASE:
        try:
            adapter = _BaseGH(url)
            return adapter.get_jobs()
        except Exception as exc:
            log.warning("Base GreenhouseAdapter failed (%s), using fallback", exc)

    import re as _re
    import requests

    # slug = last path segment, lower-cased
    # boards.greenhouse.io/figma  →  figma
    # boards.greenhouse.io/stripe →  stripe
    slug = url.rstrip("/").split("/")[-1].lower()
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"

    session = _get_session()
    session.headers.update({"Accept": "application/json"})

    jobs: list[dict[str, Any]] = []
    try:
        resp = session.get(api_url, timeout=25)
        resp.raise_for_status()
        data = resp.json().get("jobs", [])
    except Exception as exc:
        log.error("Greenhouse API error for %s: %s", company_name, exc)
        return []

    # Strip HTML tags for short_summary
    _html_tag_re = _re.compile(r"<[^>]+>")

    for item in data:
        location_info = item.get("location", {})
        if isinstance(location_info, dict):
            location = location_info.get("name", "Unknown")
        else:
            location = str(location_info) if location_info else "Unknown"

        depts = item.get("departments", [])
        dept_name = depts[0].get("name", "Unknown") if depts else "Unknown"

        offices = item.get("offices", [])
        office_name = offices[0].get("name", "") if offices else ""

        # Combine location + office for richer location info
        if office_name and office_name.lower() not in location.lower():
            location = f"{location}, {office_name}".strip(", ")

        raw_content = item.get("content", "") or ""
        plain_text = _html_tag_re.sub(" ", raw_content)
        plain_text = _re.sub(r"\s+", " ", plain_text).strip()

        jobs.append({
            "company_name": company_name,
            "job_title": item.get("title", "Unknown"),
            "job_url": item.get("absolute_url", url),
            "location": location or "Unknown",
            "department": dept_name,
            "date_posted": item.get("updated_at", "Unknown"),
            "short_summary": plain_text[:600],
            "source_site": "Greenhouse",
        })

    log.info("Greenhouse: scraped %d jobs for %s", len(jobs), company_name)
    return jobs


def _scrape_ashby(company_name: str, url: str) -> list[dict[str, Any]]:
    """
    Ashby public REST API.
    Endpoint: POST https://api.ashbyhq.com/posting-api/job-board
    Body:     {"organizationHostedJobsPageName": "<slug>"}

    The slug is the last path segment of jobs.ashbyhq.com/{slug}.
    The 401 error was caused by missing Content-Type + Accept headers.
    Both are now explicitly set.
    """
    if _ASHBY_BASE:
        try:
            adapter = _BaseAshby(url)
            return adapter.get_jobs()
        except Exception as exc:
            log.warning("Base AshbyAdapter failed (%s), using fallback", exc)

    import requests

    slug = url.rstrip("/").split("/")[-1]
    api_url = "https://api.ashbyhq.com/posting-api/job-board"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    payload = {
        "organizationHostedJobsPageName": slug,
        "includeCompensation": True,
    }

    jobs: list[dict[str, Any]] = []
    try:
        resp = requests.post(api_url, json=payload, headers=headers, timeout=25)
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        log.error("Ashby API error for %s: %s", company_name, exc)
        return []

    # Ashby returns {"jobs": [...]} or {"results": [...]} depending on version
    items = body.get("jobs") or body.get("results") or []

    import re as _re

    for item in items:
        # Location: list of location names
        loc_list = item.get("locationNames") or item.get("location") or []
        if isinstance(loc_list, list):
            location = ", ".join(str(l) for l in loc_list) if loc_list else "Unknown"
        else:
            location = str(loc_list) or "Unknown"

        # Compensation
        comp = item.get("compensation", {}) or {}
        salary = "Unknown"
        if comp:
            low = comp.get("compensationTierSummary") or comp.get("summaryShort") or ""
            salary = str(low) if low else "Unknown"

        # Strip any HTML from description
        raw_desc = item.get("descriptionHtml") or item.get("description") or ""
        plain = _re.sub(r"<[^>]+>", " ", raw_desc)
        plain = _re.sub(r"\s+", " ", plain).strip()

        jobs.append({
            "company_name": company_name,
            "job_title": item.get("title", "Unknown"),
            "job_url": item.get("jobUrl") or item.get("applyUrl") or url,
            "location": location,
            "department": item.get("departmentName") or item.get("team") or "Unknown",
            "employment_type": item.get("employmentType", "Unknown"),
            "date_posted": item.get("publishedDate") or item.get("createdAt") or "Unknown",
            "short_summary": plain[:600],
            "salary": salary,
            "source_site": "Ashby",
        })

    log.info("Ashby: scraped %d jobs for %s", len(jobs), company_name)
    return jobs


def _scrape_workday(company_name: str, url: str) -> list[dict[str, Any]]:
    """
    Workday scraper using the undocumented but stable CXS JSON API.

    URL format in companies.json must be the full myworkdayjobs.com URL:
      https://{tenant}.wd{N}.myworkdayjobs.com/en-US/{JobBoardName}
      e.g. https://atlassian.wd5.myworkdayjobs.com/en-US/Atlassian

    The CXS API endpoint is built as:
      {tenant_root}/wday/cxs/{locale}/{board}/jobs
      e.g. https://atlassian.wd5.myworkdayjobs.com/wday/cxs/en-US/Atlassian/jobs

    This is a POST endpoint that accepts JSON pagination params.
    """
    if _WD_BASE:
        try:
            adapter = _BaseWD(url)
            return adapter.get_jobs()
        except Exception as exc:
            log.warning("Base WorkdayAdapter failed (%s), using fallback", exc)

    import re as _re
    import requests

    # Parse: https://atlassian.wd5.myworkdayjobs.com/en-US/Atlassian
    # → tenant_root = https://atlassian.wd5.myworkdayjobs.com
    # → path_part   = /en-US/Atlassian
    # → api_url     = https://atlassian.wd5.myworkdayjobs.com/wday/cxs/en-US/Atlassian/jobs
    tenant_match = _re.match(r"(https?://[^/]+)(/.+)", url.rstrip("/"))
    if not tenant_match:
        log.error("Cannot parse Workday URL: %s — expected format: https://company.wdN.myworkdayjobs.com/en-US/BoardName", url)
        return []

    tenant_root = tenant_match.group(1)
    path_part = tenant_match.group(2)          # /en-US/Atlassian
    api_url = f"{tenant_root}/wday/cxs{path_part}/jobs"

    session = _get_session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Calypso-CSRF-Token": "dummy",       # Workday requires this header (value doesn't matter for public boards)
    })

    body = {
        "appliedFacets": {},
        "limit": 20,
        "offset": 0,
        "searchText": "",
    }

    all_jobs: list[dict[str, Any]] = []
    max_pages = 20   # safety cap (20 pages × 20 = 400 jobs max)

    for page in range(max_pages):
        try:
            resp = session.post(api_url, json=body, timeout=25)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            log.error("Workday API error for %s (page %d): %s", company_name, page, exc)
            break

        job_postings = payload.get("jobPostings", [])
        if not job_postings:
            break

        for item in job_postings:
            job_path = item.get("externalPath", "")
            # Build full job URL: tenant_root + /job/JobTitle/job/ID
            job_url = f"{tenant_root}{job_path}" if job_path else url

            all_jobs.append({
                "company_name": company_name,
                "job_title": item.get("title", "Unknown"),
                "job_url": job_url,
                "location": item.get("locationsText", "Unknown"),
                "date_posted": item.get("postedOn", "Unknown"),
                "short_summary": item.get("jobDescription", "")[:600],
                "source_site": "Workday",
            })

        total = payload.get("total", 0)
        body["offset"] += len(job_postings)
        if body["offset"] >= total:
            break
        time.sleep(0.3)

    log.info("Workday: scraped %d jobs for %s", len(all_jobs), company_name)
    return all_jobs


# ─────────────────────────────────────────────────────────────────────────────
# Unified scrape_company()
# ─────────────────────────────────────────────────────────────────────────────

_ATS_DISPATCH: dict[str, Any] = {
    "lever": _scrape_lever,
    "greenhouse": _scrape_greenhouse,
    "ashby": _scrape_ashby,
    "workday": _scrape_workday,
}


def scrape_company(company: dict[str, str]) -> list[dict[str, Any]]:
    """
    Scrape one company entry from companies.json.

    Parameters
    ----------
    company : dict with keys  company_name, ats, url

    Returns
    -------
    List of raw job dicts (not yet normalised or filtered).
    """
    name = company.get("company_name", "Unknown")
    ats = company.get("ats", "").lower().strip()
    url = company.get("url", "")

    if not url:
        log.warning("No URL for company: %s — skipping", name)
        return []

    fn = _ATS_DISPATCH.get(ats)
    if fn is None:
        log.warning("Unknown ATS type '%s' for company %s — skipping", ats, name)
        return []

    log.info("Scraping %s via %s …", name, ats.capitalize())
    try:
        return fn(name, url)
    except Exception as exc:
        log.error("Unexpected error scraping %s: %s", name, exc, exc_info=True)
        return []
