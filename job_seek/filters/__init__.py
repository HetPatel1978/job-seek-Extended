"""
job_seek/filters/__init__.py
------------------------------
Exposes a single get_filter_fn() factory so the runner script can swap
between keyword and Ollama classifiers with a single flag.
"""

from __future__ import annotations
from typing import Callable, Any


def get_filter_fn(use_ollama: bool = False) -> Callable:
    """
    Return the appropriate filter_jobs function.

    Parameters
    ----------
    use_ollama : if True, use the Ollama LLM filter; otherwise keyword filter.

    Returns
    -------
    filter_jobs(records, threshold) -> (kept, rejected)
    """
    if use_ollama:
        from job_seek.filters.ollama_filter import filter_jobs
        return filter_jobs
    else:
        from job_seek.filters.keyword_filter import filter_jobs
        return filter_jobs
