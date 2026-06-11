"""Conexão com PostgreSQL e helpers de execução de queries.

A URL de conexão vem do `.env` na raiz do projeto (mesma usada pelos scripts Go).
Ordem de precedência:

1. variável `DATABASE_URL` no ambiente (override explícito)
2. bloco `PGSQL_ELEICAO_*` no `.env`  (HOST, PORT, DATABASE, USERNAME, PASSWORD)
3. bloco `PGSQL_VECTOR_*` no `.env`  (mesma estrutura — usado em ambiente local)
4. fallback hardcoded para `127.0.0.1:5432` (último recurso, só pra dev local)
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

# Carrega o .env da raiz do projeto (pai do app-python/) **antes** de qualquer
# leitura de variáveis de ambiente. `override=False` para respeitar variáveis
# já definidas no shell (ex.: DATABASE_URL exportado manualmente).
_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / ".env", override=False)


def _build_dsn_from_block(prefix: str) -> str | None:
    """Monta DSN postgres:// a partir de PREFIX_HOST/PORT/DATABASE/USERNAME/PASSWORD.
    Devolve `None` se não houver pelo menos host + database + username."""
    host = os.environ.get(f"{prefix}_HOST")
    db = os.environ.get(f"{prefix}_DATABASE")
    user = os.environ.get(f"{prefix}_USERNAME")
    if not (host and db and user):
        return None
    port = os.environ.get(f"{prefix}_PORT", "5432")
    password = os.environ.get(f"{prefix}_PASSWORD", "")
    # Escapa user/password para suportar caracteres como `*`, `@`, `:`.
    userinfo = quote_plus(user)
    if password:
        userinfo = f"{userinfo}:{quote_plus(password)}"
    return f"postgresql://{userinfo}@{host}:{port}/{db}?sslmode=disable"


def _database_url() -> str:
    # 1. override explícito via env do shell
    direct = os.environ.get("DATABASE_URL")
    if direct:
        return direct
    # 2. bloco do .env preferencial — eleição
    for prefix in ("PGSQL_ELEICAO", "PGSQL_VECTOR"):
        dsn = _build_dsn_from_block(prefix)
        if dsn:
            return dsn
    # 3. fallback dev local
    return "postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes?sslmode=disable"


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
