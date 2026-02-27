"""
config.py
---------
Single source of truth for all paths, constants, and pipeline settings.
All other modules import from here — never hardcode paths elsewhere.

To override the pipeline root on a specific machine, set the environment variable:
    set PIPELINE_ROOT=D:\\MyData\\DataPipeline
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Root directories
# ---------------------------------------------------------------------------

# The repo itself (one level up from this file: utils/../)
REPO_ROOT = Path(__file__).resolve().parent.parent

# Where all pipeline data lives — override via env var if needed
PIPELINE_ROOT = Path(os.environ.get("PIPELINE_ROOT", REPO_ROOT / "data_pipeline"))

# Exchange rate CSVs produced by the IMF fetcher
DATA_DIR = REPO_ROOT / "data"

# ---------------------------------------------------------------------------
# Pipeline stage directories
# ---------------------------------------------------------------------------

INPUT_DIR       = PIPELINE_ROOT / "1_input"
PRE_DIR         = PIPELINE_ROOT / "2_preprocessing"
HOT_DIR         = PIPELINE_ROOT / "3_processing_hotfolder"
ARCHIVE_DIR     = PIPELINE_ROOT / "4_archive"
ERROR_DIR       = PIPELINE_ROOT / "5_error"
LOG_DIR         = PIPELINE_ROOT / "6_logs"
VALIDATION_DIR  = DATA_DIR / "validation_reports"

# ---------------------------------------------------------------------------
# IMF API settings
# ---------------------------------------------------------------------------

IMF_FLOW_REF        = "IMF.STA,ER"
IMF_KEY             = ".USD_XDC.PA_RT.M"
IMF_API_TIMEOUT_SEC = 30
IMF_START_YEAR      = 2000   # used by historical backfill

# ---------------------------------------------------------------------------
# REST Countries API settings
# ---------------------------------------------------------------------------

REST_COUNTRIES_TIMEOUT_SEC  = 5
REST_COUNTRIES_MAX_WORKERS  = 10      # parallel threads for currency lookups
CURRENCY_CACHE_FILE         = DATA_DIR / "currency_cache.json"

# Hardcoded overrides for territories the REST Countries API mis-maps
SPECIAL_CURRENCY_OVERRIDES = {
    "CUW":  "XCG",   # Curaçao
    "SXM":  "XCG",   # Sint Maarten
    "G163": "EUR",   # Eurozone aggregate
}

# ---------------------------------------------------------------------------
# Validation thresholds
# ---------------------------------------------------------------------------

MAX_REASONABLE_RATE         = 100_000
MIN_REASONABLE_RATE         = 0.0001
MAX_MONTH_ON_MONTH_CHANGE   = 0.30    # 30% — flag but don't fail
EXPECTED_MIN_COUNTRY_COUNT  = 50      # below this a month's data is suspicious

# ---------------------------------------------------------------------------
# Scheduling (Europe/Zurich — matches prefect.yaml)
# ---------------------------------------------------------------------------

SCHEDULE_TIMEZONE = "Europe/Zurich"

# ---------------------------------------------------------------------------
# Helper: ensure all runtime directories exist
# ---------------------------------------------------------------------------

def ensure_dirs():
    """Call once at startup to create all required directories."""
    for d in [DATA_DIR, INPUT_DIR, PRE_DIR, HOT_DIR,
              ARCHIVE_DIR, ERROR_DIR, LOG_DIR, VALIDATION_DIR]:
        d.mkdir(parents=True, exist_ok=True)
