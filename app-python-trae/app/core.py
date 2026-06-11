from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_URL = "postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes"
SOURCE_IMAGES_DIR = Path(
    "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/.vscode/imagens-sistema"
)
REFERENCE_IMAGES = [
    "WhatsApp Image 2026-06-11 at 10.58.00.jpeg",
    "WhatsApp Image 2026-06-11 at 10.58.32.jpeg",
    "WhatsApp Image 2026-06-11 at 10.58.59.jpeg",
    "WhatsApp Image 2026-06-11 at 10.59.25.jpeg",
    "WhatsApp Image 2026-06-11 at 10.59.50.jpeg",
    "WhatsApp Image 2026-06-11 at 11.00.12.jpeg",
    "WhatsApp Image 2026-06-11 at 11.01.05.jpeg",
    "WhatsApp Image 2026-06-11 at 11.01.40.jpeg",
    "WhatsApp Image 2026-06-11 at 11.03.04.jpeg",
]


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    return create_engine(database_url(), pool_pre_ping=True, future=True)


def run_df(sql: str, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=dict(params or {}))


@st.cache_data(ttl=300, show_spinner=False)
def table_exists(name: str) -> bool:
    df = run_df(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = :name
        LIMIT 1
        """,
        {"name": name},
    )
    return not df.empty


def fmt_int(value: int | float | None) -> str:
    number = 0 if value is None else int(value)
    return f"{number:,}".replace(",", ".")


def fmt_pct(value: float | None, digits: int = 2) -> str:
    number = 0.0 if value is None else float(value)
    return f"{number:.{digits}f}%".replace(".", ",")
