"""Acesso somente leitura ao PostgreSQL com cache de resultados."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Mapping

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from election_app.config import load_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = load_settings()
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=2,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def configure_read_only(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("SET default_transaction_read_only = on")
        cursor.execute(f"SET statement_timeout = '{settings.statement_timeout_ms}ms'")
        cursor.close()

    return engine


@st.cache_data(ttl=900, show_spinner=False)
def _cached_query(sql: str, params_json: str) -> pd.DataFrame:
    params = json.loads(params_json)
    with get_engine().connect() as connection:
        return pd.read_sql_query(text(sql), connection, params=params)


def query_df(sql: str, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
    payload = json.dumps(dict(params or {}), sort_keys=True, default=str)
    return _cached_query(sql, payload)


def clear_query_cache() -> None:
    _cached_query.clear()


def check_connection() -> tuple[bool, str]:
    try:
        row = query_df("SELECT current_database() AS database, current_user AS username")
        return True, f"{row.iloc[0]['database']} / {row.iloc[0]['username']}"
    except Exception as exc:  # A mensagem precisa chegar a tela de diagnostico.
        return False, str(exc)
