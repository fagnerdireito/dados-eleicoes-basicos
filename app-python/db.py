"""Conexão com PostgreSQL e helpers de execução de queries."""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Mapping

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DEFAULT_DATABASE_URL = "postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes"


def _database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    return create_engine(_database_url(), pool_pre_ping=True, future=True)


def run_df(sql: str, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
    """Executa SQL e devolve DataFrame. Use dentro de funções com @st.cache_data."""
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=dict(params or {}))


@st.cache_data(ttl=300, show_spinner=False)
def table_exists(name: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = :name
        LIMIT 1
    """
    return not run_df(sql, {"name": name}).empty


@lru_cache(maxsize=1)
def is_municipal(ano: int) -> bool:
    """2020 e 2024 são eleições municipais; 2022 é geral."""
    return int(ano) % 4 == 0 and int(ano) >= 2020
