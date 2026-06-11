"""Funções de consulta para catálogos (filtros)."""
from __future__ import annotations

from typing import Optional

import pandas as pd
import streamlit as st

from db import run_df

TTL = 3600


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_anos() -> list[int]:
    """Lista anos disponíveis em bulletin_de_urna."""
    df = run_df(
        'SELECT DISTINCT "ANO_ELEICAO"::int AS ano FROM bulletin_de_urna ORDER BY 1'
    )
    return df["ano"].tolist() if not df.empty else []


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_ufs(ano: int) -> list[str]:
    """Lista UFs disponíveis para o ano selecionado."""
    df = run_df(
        'SELECT DISTINCT "SG_UF" AS uf FROM bulletin_de_urna '
        'WHERE "ANO_ELEICAO" = :ano ORDER BY 1',
        {"ano": str(ano)},
    )
    return df["uf"].tolist() if not df.empty else []


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_municipios(ano: int, uf: str) -> pd.DataFrame:
    """Lista municípios disponíveis para ano e UF."""
    return run_df(
        '''
        SELECT DISTINCT "CD_MUNICIPIO" AS cd, "NM_MUNICIPIO" AS nm
        FROM bulletin_de_urna
        WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        ORDER BY 2
        ''',
        {"ano": str(ano), "uf": uf},
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_cargos(ano: int, uf: str, cd_municipio: Optional[str]) -> pd.DataFrame:
    """Lista cargos disponíveis para os filtros."""
    where = ['"ANO_ELEICAO" = :ano', '"SG_UF" = :uf']
    params = {"ano": str(ano), "uf": uf}
    if cd_municipio:
        where.append('"CD_MUNICIPIO" = :cd')
        params["cd"] = cd_municipio
    sql = f'''
        SELECT DISTINCT "CD_CARGO_PERGUNTA" AS cd, "DS_CARGO_PERGUNTA" AS ds
        FROM bulletin_de_urna
        WHERE {' AND '.join(where)}
        ORDER BY 1
    '''
    return run_df(sql, params)


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_candidatos(
    ano: int, uf: str, cd_municipio: Optional[str], cd_cargo: str, limit: int = 200
) -> pd.DataFrame:
    """Lista candidatos ordenados por votos."""
    where = ['"ANO_ELEICAO" = :ano', '"SG_UF" = :uf', '"CD_CARGO_PERGUNTA" = :cargo',
             'COALESCE("DS_TIPO_VOTAVEL", \'\') NOT IN (\'Branco\', \'Nulo\')']
    params = {"ano": str(ano), "uf": uf, "cargo": cd_cargo, "lim": limit}
    if cd_municipio:
        where.append('"CD_MUNICIPIO" = :cd')
        params["cd"] = cd_municipio
    sql = f'''
        SELECT "NR_VOTAVEL" AS nr,
               MAX("NM_VOTAVEL") AS nm,
               MAX("SG_PARTIDO") AS sg_partido,
               SUM("QT_VOTOS"::bigint) AS votos
        FROM bulletin_de_urna
        WHERE {' AND '.join(where)}
        GROUP BY 1
        ORDER BY votos DESC
        LIMIT :lim
    '''
    return run_df(sql, params)