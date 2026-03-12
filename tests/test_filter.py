"""
tests/test_filter.py
---------------------
Unit tests for the keyword filter.

Run with:
  pytest tests/test_filter.py -v
"""

import pytest
from job_seek.filters.keyword_filter import classify_record, filter_jobs, RELEVANCE_THRESHOLD


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_job(title: str, summary: str = "") -> dict:
    return {
        "company_name": "TestCo",
        "job_title": title,
        "short_summary": summary,
        "location": "Remote",
        "job_url": f"https://example.com/{title.replace(' ', '-')}",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Individual record classification
# ─────────────────────────────────────────────────────────────────────────────

class TestClassifyRecord:

    def test_senior_software_engineer_is_kept(self):
        rec = _make_job("Senior Software Engineer", "Python, AWS, Docker")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep"
        assert rec["relevance_score"] >= RELEVANCE_THRESHOLD

    def test_frontend_developer_is_kept(self):
        rec = _make_job("Frontend Developer", "React, TypeScript, CSS")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep"

    def test_data_scientist_is_kept(self):
        rec = _make_job("Data Scientist", "PyTorch, machine learning, Python")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep"
        assert "Data Science" in rec["technical_category"]

    def test_devops_engineer_is_kept(self):
        rec = _make_job("DevOps Engineer", "Kubernetes, Terraform, AWS")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep"

    def test_sales_rep_is_rejected(self):
        rec = _make_job("Account Executive", "Drive sales, manage pipeline")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Reject"
        assert rec["relevance_score"] < RELEVANCE_THRESHOLD

    def test_recruiter_is_rejected(self):
        rec = _make_job("Technical Recruiter", "Talent acquisition, sourcing")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Reject"

    def test_marketing_manager_is_rejected(self):
        rec = _make_job("Marketing Manager", "Brand strategy, campaigns")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Reject"

    def test_backend_engineer_with_tech_summary_is_kept(self):
        rec = _make_job(
            "Backend Engineer",
            "You will build REST APIs in Go, manage PostgreSQL, deploy on AWS Lambda."
        )
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep"
        assert rec["relevance_score"] >= RELEVANCE_THRESHOLD

    def test_intern_with_tech_skills_is_kept(self):
        rec = _make_job("Software Engineering Intern", "Python, Django, unit testing")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep"

    def test_intern_without_tech_skills_may_be_rejected(self):
        rec = _make_job("Marketing Intern", "social media, content creation")
        classify_record(rec)
        assert rec["keep_or_reject"] == "Reject"

    def test_score_is_between_0_and_100(self):
        for title in ["CEO", "Janitor", "Senior ML Engineer", "Sales Director"]:
            rec = _make_job(title)
            classify_record(rec)
            assert 0 <= rec["relevance_score"] <= 100, f"Score out of range for: {title}"

    def test_category_set_for_technical_role(self):
        rec = _make_job("Machine Learning Engineer")
        classify_record(rec)
        assert rec["technical_category"] != "Unknown"

    def test_rejection_reason_populated_for_rejected(self):
        rec = _make_job("Account Executive")
        classify_record(rec)
        assert rec["rejection_reason"] != ""


# ─────────────────────────────────────────────────────────────────────────────
# Batch filtering
# ─────────────────────────────────────────────────────────────────────────────

class TestFilterJobs:

    def _batch(self):
        return [
            _make_job("Senior Software Engineer", "Python, AWS"),
            _make_job("Data Engineer", "Spark, Kafka, SQL"),
            _make_job("Account Executive", "SaaS sales"),
            _make_job("HR Business Partner", "People operations"),
            _make_job("Frontend Developer", "React, TypeScript"),
            _make_job("Product Manager", "Roadmap, stakeholders"),
            _make_job("DevOps Engineer", "Kubernetes, CI/CD"),
        ]

    def test_returns_two_lists(self):
        kept, rejected = filter_jobs(self._batch())
        assert isinstance(kept, list)
        assert isinstance(rejected, list)

    def test_total_preserved(self):
        batch = self._batch()
        kept, rejected = filter_jobs(batch)
        assert len(kept) + len(rejected) == len(batch)

    def test_known_tech_jobs_are_kept(self):
        batch = self._batch()
        kept, _ = filter_jobs(batch)
        kept_titles = {r["job_title"] for r in kept}
        assert "Senior Software Engineer" in kept_titles
        assert "Data Engineer" in kept_titles
        assert "Frontend Developer" in kept_titles
        assert "DevOps Engineer" in kept_titles

    def test_known_non_tech_jobs_are_rejected(self):
        batch = self._batch()
        _, rejected = filter_jobs(batch)
        rejected_titles = {r["job_title"] for r in rejected}
        assert "Account Executive" in rejected_titles
        assert "HR Business Partner" in rejected_titles

    def test_custom_threshold(self):
        batch = self._batch()
        kept_strict, _ = filter_jobs(batch, threshold=90)
        kept_loose, _ = filter_jobs(batch, threshold=30)
        assert len(kept_strict) <= len(kept_loose)

    def test_all_records_have_keep_or_reject_set(self):
        batch = self._batch()
        kept, rejected = filter_jobs(batch)
        for rec in kept + rejected:
            assert rec["keep_or_reject"] in ("Keep", "Reject")

    def test_kept_records_all_meet_threshold(self):
        batch = self._batch()
        kept, _ = filter_jobs(batch, threshold=70)
        for rec in kept:
            assert rec["relevance_score"] >= 70

    def test_empty_input(self):
        kept, rejected = filter_jobs([])
        assert kept == []
        assert rejected == []


# ─────────────────────────────────────────────────────────────────────────────
# Real ATS sample data (STEP 7 — testing against realistic payloads)
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_LEVER_JOB = {
    "company_name": "Stripe",
    "job_title": "Backend Engineer, Payments Infrastructure",
    "location": "San Francisco, CA",
    "department": "Engineering",
    "employment_type": "Full-time",
    "source_site": "Lever",
    "job_url": "https://jobs.lever.co/stripe/abc123",
    "short_summary": (
        "Build and scale the core payments processing systems that move money "
        "globally. You'll work with Go, Python, AWS, PostgreSQL, and Kafka."
    ),
}

SAMPLE_GREENHOUSE_JOB = {
    "company_name": "Notion",
    "job_title": "Senior Frontend Engineer",
    "location": "New York, NY",
    "department": "Product Engineering",
    "employment_type": "Full-time",
    "source_site": "Greenhouse",
    "job_url": "https://boards.greenhouse.io/notion/jobs/456",
    "short_summary": (
        "Work on Notion's web client using React, TypeScript, and modern CSS. "
        "Experience with performance optimisation and accessibility is a plus."
    ),
}

SAMPLE_ASHBY_JOB = {
    "company_name": "Linear",
    "job_title": "Site Reliability Engineer",
    "location": "Remote",
    "department": "Infrastructure",
    "employment_type": "Full-time",
    "source_site": "Ashby",
    "job_url": "https://jobs.ashbyhq.com/linear/789",
    "short_summary": (
        "Own the reliability and scalability of Linear's infrastructure. "
        "Kubernetes, Terraform, AWS, on-call rotations, and SLO management."
    ),
}

SAMPLE_SALES_JOB = {
    "company_name": "SalesForce",
    "job_title": "Account Executive, Mid-Market",
    "location": "Chicago, IL",
    "department": "Sales",
    "employment_type": "Full-time",
    "source_site": "Greenhouse",
    "job_url": "https://boards.greenhouse.io/salesforce/jobs/999",
    "short_summary": (
        "Drive new business revenue by prospecting and closing mid-market deals. "
        "Meet and exceed quarterly sales quotas."
    ),
}


class TestRealWorldSamples:

    def test_lever_backend_engineer_kept(self):
        rec = dict(SAMPLE_LEVER_JOB)
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep", f"Score was {rec['relevance_score']}"

    def test_greenhouse_senior_frontend_kept(self):
        rec = dict(SAMPLE_GREENHOUSE_JOB)
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep", f"Score was {rec['relevance_score']}"

    def test_ashby_sre_kept(self):
        rec = dict(SAMPLE_ASHBY_JOB)
        classify_record(rec)
        assert rec["keep_or_reject"] == "Keep", f"Score was {rec['relevance_score']}"
        assert "SRE" in rec["technical_category"] or "Cloud" in rec["technical_category"]

    def test_sales_job_rejected(self):
        rec = dict(SAMPLE_SALES_JOB)
        classify_record(rec)
        assert rec["keep_or_reject"] == "Reject", f"Score was {rec['relevance_score']}"
