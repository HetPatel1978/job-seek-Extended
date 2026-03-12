"""
job_seek/utils/logger.py
------------------------
Central logging configuration for the entire pipeline.
All modules import `get_logger(__name__)` — they never touch basicConfig directly.
"""

import logging
import sys
from pathlib import Path

try:
    import colorlog  # optional pretty colours in terminal
    _COLORLOG_AVAILABLE = True
except ImportError:
    _COLORLOG_AVAILABLE = False

# Where log files land (created automatically)
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"

_INITIALISED = False


def _initialise_root_logger(level: int = logging.INFO) -> None:
    global _INITIALISED
    if _INITIALISED:
        return
    _INITIALISED = True

    root = logging.getLogger()
    root.setLevel(level)

    fmt_file = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # ── File handler (always plain text) ─────────────────────────────────────
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt_file, datefmt=datefmt))
    root.addHandler(fh)

    # ── Console handler (colour if available, else plain) ────────────────────
    if _COLORLOG_AVAILABLE:
        fmt_console = (
            "%(log_color)s%(asctime)s%(reset)s | "
            "%(log_color)s%(levelname)-8s%(reset)s | "
            "%(cyan)s%(name)s%(reset)s | %(message)s"
        )
        ch = colorlog.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(
            colorlog.ColoredFormatter(
                fmt_console,
                datefmt=datefmt,
                log_colors={
                    "DEBUG": "white",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        )
    else:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(logging.Formatter(fmt_file, datefmt=datefmt))

    root.addHandler(ch)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a named logger, initialising root handlers on first call."""
    _initialise_root_logger(level)
    return logging.getLogger(name)
