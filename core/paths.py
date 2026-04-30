from pathlib import Path

# Base directories (robust even if you move folders around)
CORE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CORE_DIR.parent

CONFIG_DIR = PROJECT_DIR / "config"
TASKS_FILE = CONFIG_DIR / "tasks.json"
DAILY_METRICS_FILE = CORE_DIR / "daily_metrics.json"
COACHING_LOG_FILE = CORE_DIR / "coaching_log.json"

ANALYTICS_DIR = PROJECT_DIR / "analytics"
PERFORMANCE_CONFIG_FILE = ANALYTICS_DIR / "performance_config.json"
PERFORMANCE_LOG_FILE = ANALYTICS_DIR / "performance_log.json"
OVERRIDES_FILE = ANALYTICS_DIR / "overrides.json"