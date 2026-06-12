"""Conexão com PostgreSQL e helpers de execução de queries.

A URL de conexão é montada exclusivamente a partir das variáveis
`PGSQL_VECTOR_*` no `.env` da raiz do projeto (mesma pasta que contém
`app-python/`, `go_postgres/`, `dados/` etc.):

    PGSQL_VECTOR_HOST
    PGSQL_VECTOR_PORT
    PGSQL_VECTOR_DATABASE
    PGSQL_VECTOR_USERNAME
    PGSQL_VECTOR_PASSWORD

Se essas variáveis não estiverem preenchidas, o app falha rápido com
mensagem clara em vez de tentar fallback para outras chaves.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Carrega o .env da raiz do projeto (pai do app-python/) antes de qualquer
# leitura de variáveis de ambiente. `override=False` para respeitar variáveis
# já definidas no shell (ex.: `export PGSQL_VECTOR_HOST=...`).
_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / ".env", override=False)


class _MissingEnvError(RuntimeError):
    """Erro disparado quando o bloco PGSQL_VECTOR_* não está completo."""


def _require(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if not value:
        raise _MissingEnvError(
            f"Variável `{key}` não definida. Verifique o `.env` na raiz do projeto "
            "e garanta que o bloco PGSQL_VECTOR_* esteja descomentado e preenchido."
        )
    return value


@lru_cache(maxsize=1)
def _database_url() -> str:
    host = _require("PGSQL_VECTOR_HOST")
    database = _require("PGSQL_VECTOR_DATABASE")
    user = _require("PGSQL_VECTOR_USERNAME")
    port = os.environ.get("PGSQL_VECTOR_PORT", "5432").strip() or "5432"
    password = os.environ.get("PGSQL_VECTOR_PASSWORD", "")

    # Escapa user/password para suportar caracteres como `*`, `@`, `:`.
    userinfo = quote_plus(user)
    if password:
        userinfo = f"{userinfo}:{quote_plus(password)}"
    return f"postgresql://{userinfo}@{host}:{port}/{database}?sslmode=disable"


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    try:
        url = _database_url()
    except _MissingEnvError as exc:
        st.error(str(exc))
        st.stop()
    return create_engine(url, pool_pre_ping=True, future=True)


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
