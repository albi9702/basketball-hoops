"""Centralized logging configuration for the basketball-hoops project."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_LOG_FILE = LOG_DIR / "scraper.log"


def configure_logging(
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    log_to_console: bool = False,
) -> logging.Logger:
    """
    Configure and return the root logger for the application.

    Parameters
    ----------
    level : int
        Logging level (e.g., logging.INFO, logging.DEBUG).
    log_file : Optional[Path]
        Path to the log file. Defaults to logs/scraper.log.
    log_to_console : bool
        If True, also emit logs to stderr.

    Returns
    -------
    logging.Logger
        The configured root logger.
    """
    log_file = log_file or DEFAULT_LOG_FILE
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger("basketball_hoops")
    root_logger.setLevel(level)

    # Avoid adding duplicate handlers on repeated calls
    if not root_logger.handlers:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        if log_to_console:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Return a child logger under the basketball_hoops namespace.

    Parameters
    ----------
    name : str
        Logger name (typically __name__ of the calling module).

    Returns
    -------
    logging.Logger
    """
    return logging.getLogger(f"basketball_hoops.{name}")
