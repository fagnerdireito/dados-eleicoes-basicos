"""Configuracao do aplicativo e da conexao PostgreSQL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import dotenv_values


APP_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = APP_ROOT.parent / ".env"
ASSETS_DIR = APP_ROOT / "assets"
GEOJSON_DIR = ASSETS_DIR / "geojson"


@dataclass(frozen=True)
class Settings:
    database_url: str
    statement_timeout_ms: int = 45_000
    cache_ttl_seconds: int = 900


def _first(values: dict[str, str | None], *keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key) or values.get(key)
        if value:
            return str(value)
    return default


def load_settings() -> Settings:
    """Carrega PGSQL_VECTOR_* do ambiente ou do .env da raiz do projeto."""
    env_file = Path(os.getenv("APP_ENV_FILE", str(DEFAULT_ENV_FILE))).expanduser()
    values = dotenv_values(env_file) if env_file.exists() else {}

    explicit_url = os.getenv("DATABASE_URL") or values.get("DATABASE_URL")
    if explicit_url:
        url = str(explicit_url)
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return Settings(database_url=url)

    host = _first(values, "PGSQL_VECTOR_HOST", default="127.0.0.1")
    port = _first(values, "PGSQL_VECTOR_PORT", default="5432")
    database = _first(values, "PGSQL_VECTOR_DATABASE", default="eleicoes")
    username = _first(values, "PGSQL_VECTOR_USERNAME", default="postgres")
    password = _first(values, "PGSQL_VECTOR_PASSWORD")
    credentials = quote_plus(username)
    if password:
        credentials += f":{quote_plus(password)}"
    url = f"postgresql+psycopg2://{credentials}@{host}:{port}/{database}"
    return Settings(database_url=url)
