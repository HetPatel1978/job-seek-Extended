# job-seek — Extended Pipeline

A fully local, free job discovery and filtering pipeline built on top of
`viktor-shcherb/job-seek`.  Scrapes ATS job boards (Lever, Greenhouse,
Workday, Ashby), filters for CS / Software / IT roles, and exports results
to CSV and XLSX.

## Quick Start

```bash
# 1. Clone the base repo and enter it
git clone https://github.com/viktor-shcherb/job-seek.git
cd job-seek

# 2. Install dependencies
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Add the extension files from this package (copy everything in this zip
#    on top of the cloned repo — files that already exist will be extended,
#    new files will be added)

# 4. Run the full pipeline
python scripts/run_pipeline.py --companies companies.json

# 5. Check output
ls data/
#   raw_jobs.csv
#   filtered_cs_jobs.csv
#   filtered_cs_jobs.xlsx
```

## Folder Layout

```
job-seek/
├── job_seek/
│   ├── adapters/          # Lever, Greenhouse, Workday, Ashby (base repo)
│   ├── filters/
│   │   ├── __init__.py
│   │   ├── keyword_filter.py   # Rule-based classifier (default, no LLM)
│   │   └── ollama_filter.py    # Optional: local LLM via Ollama
│   ├── exporters/
│   │   ├── __init__.py
│   │   └── csv_xlsx.py         # CSV + XLSX export
│   └── utils/
│       ├── __init__.py
│       ├── deduplication.py
│       ├── logger.py
│       └── normalizer.py       # Field normalizer / enricher
├── scripts/
│   └── run_pipeline.py         # End-to-end CLI runner
├── tests/
│   ├── test_filter.py
│   └── test_export.py
├── companies.json              # List of companies + ATS types to scrape
├── data/                       # Output directory (git-ignored)
├── logs/                       # Log files (git-ignored)
└── requirements.txt
```

## Filtering

Default: pure keyword/rule-based (no internet, no GPU, instant).
Optional: swap to Ollama for LLM-assisted scoring:

```bash
# Install Ollama: https://ollama.com
ollama pull mistral
python scripts/run_pipeline.py --companies companies.json --use-ollama
```

## Output columns

`company_name`, `job_title`, `location`, `country`, `work_type`,
`employment_type`, `seniority_level`, `department`, `job_url`,
`source_site`, `date_posted`, `salary`, `visa_sponsorship`,
`requires_degree`, `technical_category`, `relevance_score`,
`keep_or_reject`, `rejection_reason`, `short_summary`,
`required_skills`, `preferred_skills`
