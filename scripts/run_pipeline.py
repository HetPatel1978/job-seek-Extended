#!/usr/bin/env python3
"""
scripts/run_pipeline.py
------------------------
End-to-end job discovery and filtering pipeline.

Usage
-----
  # Basic run (keyword filter, all companies in companies.json)
  python scripts/run_pipeline.py

  # Specify a custom companies file
  python scripts/run_pipeline.py --companies my_companies.json

  # Use Ollama LLM filter instead of keyword filter
  python scripts/run_pipeline.py --use-ollama

  # Lower the relevance threshold (default 70)
  python scripts/run_pipeline.py --threshold 60

  # Change output directory
  python scripts/run_pipeline.py --output-dir results/

  # Scrape only specific companies by name (comma-separated)
  python scripts/run_pipeline.py --only "Stripe,Figma"

  # Dry run: scrape and normalise only, skip filtering and export
  python scripts/run_pipeline.py --dry-run

  # Verbose debug logging
  python scripts/run_pipeline.py --debug

Pipeline stages
---------------
  1. Load companies list from JSON
  2. Scrape each company via its ATS adapter
  3. Normalise every raw record into canonical schema
  4. Deduplicate
  5. Save raw_jobs.csv (all scraped + normalised jobs)
  6. Filter: classify each job with keyword or Ollama classifier
  7. Save filtered_cs_jobs.csv + filtered_cs_jobs.xlsx (kept jobs only)
  8. Print summary
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

# ── Make sure job_seek is importable from repo root ──────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import click

from job_seek.utils.logger import get_logger
from job_seek.utils.normalizer import normalize
from job_seek.utils.deduplication import deduplicate
from job_seek.adapters.scraper import scrape_company
from job_seek.filters import get_filter_fn
from job_seek.exporters.csv_xlsx import export_all

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CLI definition
# ─────────────────────────────────────────────────────────────────────────────

@click.command()
@click.option(
    "--companies",
    default="companies.json",
    show_default=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to companies JSON file.",
)
@click.option(
    "--output-dir",
    default="data",
    show_default=True,
    help="Directory for output files.",
)
@click.option(
    "--threshold",
    default=70,
    show_default=True,
    type=click.IntRange(0, 100),
    help="Minimum relevance score to keep a job (0–100).",
)
@click.option(
    "--use-ollama",
    is_flag=True,
    default=False,
    help="Use local Ollama LLM for filtering instead of keyword rules.",
)
@click.option(
    "--only",
    default=None,
    help="Comma-separated list of company names to scrape (case-insensitive).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Scrape and normalise only; skip filtering and export.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable DEBUG-level logging.",
)
def main(
    companies: str,
    output_dir: str,
    threshold: int,
    use_ollama: bool,
    only: str | None,
    dry_run: bool,
    debug: bool,
) -> None:
    import logging
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    start_time = time.time()

    log.info("=" * 60)
    log.info("job-seek pipeline starting")
    log.info("  companies file : %s", companies)
    log.info("  output dir     : %s", output_dir)
    log.info("  threshold      : %d", threshold)
    log.info("  filter mode    : %s", "Ollama LLM" if use_ollama else "Keyword rules")
    log.info("=" * 60)

    # ── 1. Load companies ─────────────────────────────────────────────────────
    companies_path = Path(companies)
    try:
        company_list: list[dict] = json.loads(companies_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Failed to load companies file: %s", exc)
        sys.exit(1)

    if only:
        only_set = {n.strip().lower() for n in only.split(",")}
        company_list = [c for c in company_list if c.get("company_name", "").lower() in only_set]
        log.info("Filtered to %d companies: %s", len(company_list), only)

    if not company_list:
        log.error("No companies to scrape. Check --only flag or companies file.")
        sys.exit(1)

    log.info("Companies to scrape: %d", len(company_list))

    # ── 2. Scrape ─────────────────────────────────────────────────────────────
    all_raw: list[dict] = []

    for i, company in enumerate(company_list, start=1):
        name = company.get("company_name", "Unknown")
        log.info("[%d/%d] Scraping: %s", i, len(company_list), name)
        raw_jobs = scrape_company(company)
        all_raw.extend(raw_jobs)
        log.info("  → %d raw jobs fetched", len(raw_jobs))

    log.info("Total raw jobs scraped: %d", len(all_raw))

    if not all_raw:
        log.warning("No jobs scraped — check network connectivity and company URLs.")
        if not dry_run:
            sys.exit(0)

    # ── 3. Normalise ──────────────────────────────────────────────────────────
    log.info("Normalising records …")
    normalised = [normalize(r) for r in all_raw]
    log.info("Normalisation complete: %d records", len(normalised))

    # ── 4. Deduplicate ────────────────────────────────────────────────────────
    log.info("Deduplicating …")
    unique = deduplicate(normalised)

    if dry_run:
        log.info("Dry run — stopping after normalisation/dedup.")
        log.info("  %d unique records found", len(unique))
        _print_summary(unique, [], time.time() - start_time)
        return

    # ── 5. Save raw CSV ───────────────────────────────────────────────────────
    from job_seek.exporters.csv_xlsx import write_csv
    raw_csv_path = Path(output_dir) / "raw_jobs.csv"
    write_csv(unique, raw_csv_path)

    # ── 6. Filter ─────────────────────────────────────────────────────────────
    log.info("Filtering with %s …", "Ollama LLM" if use_ollama else "keyword rules")
    filter_jobs = get_filter_fn(use_ollama=use_ollama)
    kept, rejected = filter_jobs(unique, threshold=threshold)

    # ── 7. Export filtered results ────────────────────────────────────────────
    export_all(
        raw_records=unique,
        filtered_records=kept,
        output_dir=output_dir,
    )

    # ── 8. Summary ────────────────────────────────────────────────────────────
    _print_summary(unique, kept, time.time() - start_time)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(all_records: list, kept: list, elapsed: float) -> None:
    from collections import Counter

    log.info("")
    log.info("━" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("━" * 60)
    log.info("  Total unique jobs scraped : %d", len(all_records))
    log.info("  Jobs kept (CS/IT roles)   : %d", len(kept))
    log.info("  Jobs rejected             : %d", len(all_records) - len(kept))
    log.info("  Elapsed time              : %.1f s", elapsed)

    if kept:
        cat_counts = Counter(str(r.get("technical_category", "Unknown")) for r in kept)
        log.info("")
        log.info("  Top categories:")
        for cat, count in cat_counts.most_common(10):
            log.info("    %-35s %d", cat, count)

    log.info("━" * 60)
    log.info("Output files:")
    log.info("  data/raw_jobs.csv")
    log.info("  data/filtered_cs_jobs.csv")
    log.info("  data/filtered_cs_jobs.xlsx")
    log.info("━" * 60)


if __name__ == "__main__":
    main()
