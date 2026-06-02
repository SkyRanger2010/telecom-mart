"""Настройки Django для dashbot (mart-dashboard)."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY", "dashbot-dev-only-change-me-in-docker-secret-key-0123456789"
)

DEBUG = os.environ.get("DJANGO_DEBUG", "0").strip() == "1"

ALLOWED_HOSTS = [
    h.strip()
    for h in os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",")
    if h.strip()
]
if not ALLOWED_HOSTS:
    ALLOWED_HOSTS = ["*"]

csrf_extra = os.environ.get("DJANGO_CSRF_TRUSTED_ORIGINS", "").strip()
CSRF_TRUSTED_ORIGINS = [o.strip() for o in csrf_extra.split(",") if o.strip()]

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse").strip()
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_INTERNAL_HTTP_PORT", "8123") or "8123")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DB", "serving").strip()
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default").strip()
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "").strip()

# При старте dashbot: ALTER ADD недостающих метрик kpi_inflow_daily / kpi_outflow_daily (старые CH-таблицы).
CLICKHOUSE_ENSURE_FLOW_METRICS = (
    os.environ.get("CLICKHOUSE_ENSURE_FLOW_METRICS", "1").strip() == "1"
)

LLM_API_URL = os.environ.get("LLM_API_URL", "http://llm-api:8080").rstrip("/")
SQLGUARD_API_URL = os.environ.get("SQLGUARD_API_URL", "http://sqlguard-api:8080").rstrip("/")

NL2SQL_MAX_ROWS = int(os.environ.get("NL2SQL_MAX_ROWS", "500"))
# В проде: deepseek-chat (см. .env LLM_UPSTREAM_*); без upstream — stub-model.
NL2SQL_LLM_MODEL = os.environ.get("NL2SQL_LLM_MODEL", "stub-model")

# Таблица ClickHouse, которую пересоздаёт пайплайн «дашборд по запросу» после sqlguard.
QUERY_DASHBOARD_TABLE = os.environ.get("QUERY_DASHBOARD_TABLE", "dashboard_query_stub").strip()

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "vitrina",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "martdash.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.messages.context_processors.messages",
                "vitrina.context_processors.vitrina_nav",
            ],
        },
    },
]

WSGI_APPLICATION = "martdash.wsgi.application"

DB_PATH = BASE_DIR / ".dashbot" / "dashbot.sqlite3"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DB_PATH,
    },
}

LANGUAGE_CODE = "ru-ru"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "INFO"},
    "loggers": {
        "vitrina": {"handlers": ["console"], "level": "DEBUG" if DEBUG else "INFO"},
    },
}
