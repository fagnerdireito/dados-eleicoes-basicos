"""Funções de consulta — uma por necessidade da UI. Todas cacheadas por parâmetros."""
from __future__ import annotations

from typing import Optional

import pandas as pd
import streamlit as st

from db import run_df

TTL = 3600

# ---------------------------------------------------------------------------
# Catálogos para os filtros globais
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def listar_anos() -> list[int]:
    df = run_df(
        'SELECT DISTINCT "ANO_ELEICAO"::int AS ano FROM boletim_de_urna ORDER BY 1'
    )
    return df["ano"].tolist()


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_ufs(ano: int) -> list[str]:
    df = run_df(
        'SELECT DISTINCT "SG_UF" AS uf FROM boletim_de_urna '
        'WHERE "ANO_ELEICAO" = :ano ORDER BY 1',
        {"ano": str(ano)},
    )
    return df["uf"].tolist()


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_municipios(ano: int, uf: str) -> pd.DataFrame:
    return run_df(
        '''
        SELECT DISTINCT "CD_MUNICIPIO" AS cd, "NM_MUNICIPIO" AS nm
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        ORDER BY 2
        ''',
        {"ano": str(ano), "uf": uf},
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_cargos(ano: int, uf: str, cd_municipio: Optional[str]) -> pd.DataFrame:
    where = ['"ANO_ELEICAO" = :ano', '"SG_UF" = :uf']
    params = {"ano": str(ano), "uf": uf}
    if cd_municipio:
        where.append('"CD_MUNICIPIO" = :cd')
        params["cd"] = cd_municipio
    sql = f'''
        SELECT DISTINCT "CD_CARGO_PERGUNTA" AS cd, "DS_CARGO_PERGUNTA" AS ds
        FROM boletim_de_urna
        WHERE {' AND '.join(where)}
        ORDER BY 1
    '''
    return run_df(sql, params)


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_candidatos(
    ano: int, uf: str, cd_municipio: Optional[str], cd_cargo: str, limit: int = 200
) -> pd.DataFrame:
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
        FROM boletim_de_urna
        WHERE {' AND '.join(where)}
        GROUP BY 1
        ORDER BY votos DESC
        LIMIT :lim
    '''
    return run_df(sql, params)


# ---------------------------------------------------------------------------
# Aba "Resumo no município"
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def resumo_candidato_municipio(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_votavel: str
) -> dict:
    """Métricas principais para o card de resumo."""
    base_filter = (
        '"ANO_ELEICAO" = :ano AND "SG_UF" = :uf AND "CD_MUNICIPIO" = :cd '
        'AND "CD_CARGO_PERGUNTA" = :cargo'
    )
    p = {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo, "nr": nr_votavel}

    # Votos do candidato + posição
    rank = run_df(
        f'''
        WITH agg AS (
            SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
                   SUM("QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna
            WHERE {base_filter}
              AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
            GROUP BY 1
        )
        SELECT nr, nm, votos,
               RANK() OVER (ORDER BY votos DESC) AS posicao,
               (SELECT COUNT(*) FROM agg) AS total_cands
        FROM agg ORDER BY votos DESC
        ''',
        p,
    )
    minha = rank[rank["nr"] == nr_votavel]
    votos_cand = int(minha["votos"].iloc[0]) if not minha.empty else 0
    posicao = int(minha["posicao"].iloc[0]) if not minha.empty else None

    # Composição (válidos / brancos / nulos) — totais únicos por seção para aptos/comparec
    comp = run_df(
        f'''
        SELECT
          COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" NOT IN ('Branco', 'Nulo')
                           THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
          COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco'
                           THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
          COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo'
                           THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
        FROM boletim_de_urna
        WHERE {base_filter}
        ''',
        p,
    ).iloc[0]

    secoes = run_df(
        f'''
        SELECT SUM("QT_APTOS"::bigint) AS aptos,
               SUM("QT_COMPARECIMENTO"::bigint) AS comparec,
               SUM("QT_ABSTENCOES"::bigint) AS abstenc
        FROM (
          SELECT DISTINCT "NR_ZONA", "NR_SECAO",
                          "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES"
          FROM boletim_de_urna
          WHERE {base_filter}
        ) s
        ''',
        p,
    ).iloc[0]

    # Liderança em locais
    lider = run_df(
        f'''
        WITH por_local AS (
          SELECT "NR_LOCAL_VOTACAO" AS lv, "NR_VOTAVEL" AS nr,
                 SUM("QT_VOTOS"::bigint) AS votos
          FROM boletim_de_urna
          WHERE {base_filter}
            AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
          GROUP BY 1, 2
        ),
        rank_local AS (
          SELECT lv, nr, votos,
                 RANK() OVER (PARTITION BY lv ORDER BY votos DESC) AS rk
          FROM por_local
        )
        SELECT COUNT(DISTINCT lv) FILTER (WHERE nr = :nr AND rk = 1) AS lideres,
               COUNT(DISTINCT lv) AS total_locais
        FROM rank_local
        ''',
        p,
    ).iloc[0]

    aptos = int(secoes["aptos"] or 0)
    comparec = int(secoes["comparec"] or 0)
    abstenc = int(secoes["abstenc"] or 0)
    validos = int(comp["validos"] or 0)
    brancos = int(comp["brancos"] or 0)
    nulos = int(comp["nulos"] or 0)

    return {
        "nm_candidato": minha["nm"].iloc[0] if not minha.empty else None,
        "votos_cand": votos_cand,
        "posicao": posicao,
        "total_cands": int(rank["total_cands"].iloc[0]) if not rank.empty else 0,
        "pct_validos": (votos_cand / validos * 100) if validos else 0.0,
        "validos": validos,
        "brancos": brancos,
        "nulos": nulos,
        "aptos": aptos,
        "comparec": comparec,
        "abstenc": abstenc,
        "pct_comparec": (comparec / aptos * 100) if aptos else 0.0,
        "lideres": int(lider["lideres"] or 0),
        "total_locais": int(lider["total_locais"] or 0),
    }


# ---------------------------------------------------------------------------
# Aba "Onde estão os votos no estado"
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def votos_candidato_por_municipio(
    ano: int, uf: str, cd_cargo: str, nr_votavel: str
) -> pd.DataFrame:
    """Inclui código IBGE (via JOIN com municipio_tse_ibge) para o coroplético."""
    return run_df(
        '''
        SELECT b."CD_MUNICIPIO" AS cd,
               MAX(b."NM_MUNICIPIO") AS nm,
               MAX(m.cd_municipio_ibge) AS cd_ibge,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        LEFT JOIN municipio_tse_ibge m
          ON m.sg_uf = b."SG_UF"
         AND m.cd_municipio_tse = b."CD_MUNICIPIO"
        WHERE b."ANO_ELEICAO" = :ano AND b."SG_UF" = :uf
          AND b."CD_CARGO_PERGUNTA" = :cargo AND b."NR_VOTAVEL" = :nr
        GROUP BY b."CD_MUNICIPIO"
        ORDER BY votos DESC
        ''',
        {"ano": str(ano), "uf": uf, "cargo": cd_cargo, "nr": nr_votavel},
    )


# ---------------------------------------------------------------------------
# Aba "Onde estão os votos no município" (mapa de bolhas) — só 2024
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def votos_candidato_por_local(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_votavel: str
) -> pd.DataFrame:
    return run_df(
        '''
        SELECT lv."NM_LOCAL_VOTACAO" AS nm_local,
               lv."NM_BAIRRO" AS bairro,
               lv."NR_LATITUDE"::float AS lat,
               lv."NR_LONGITUDE"::float AS lng,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        JOIN local_votacao lv
          ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
         AND lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"
        WHERE b."ANO_ELEICAO" = :ano AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd
          AND b."CD_CARGO_PERGUNTA" = :cargo AND b."NR_VOTAVEL" = :nr
          AND lv."NR_LATITUDE" NOT IN ('-1', '') AND lv."NR_LATITUDE" IS NOT NULL
        GROUP BY 1, 2, 3, 4
        ORDER BY votos DESC
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio,
         "cargo": cd_cargo, "nr": nr_votavel},
    )


# ---------------------------------------------------------------------------
# Aba "Ranking geral no município" (comparativo 2 anos)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def ranking_municipio(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, top: int = 10
) -> pd.DataFrame:
    return run_df(
        f'''
        WITH agg AS (
          SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
                 MAX("SG_PARTIDO") AS partido, SUM("QT_VOTOS"::bigint) AS votos
          FROM boletim_de_urna
          WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
            AND "CD_MUNICIPIO" = :cd AND "CD_CARGO_PERGUNTA" = :cargo
            AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
          GROUP BY 1
        ),
        tot AS (SELECT SUM(votos) AS t FROM agg)
        SELECT nr, nm, partido, votos,
               (votos::float / NULLIF((SELECT t FROM tot), 0) * 100) AS pct
        FROM agg ORDER BY votos DESC LIMIT {int(top)}
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo},
    )


# ---------------------------------------------------------------------------
# Aba "Síntese territorial"
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def sintese_territorial(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str
) -> pd.DataFrame:
    return run_df(
        '''
        WITH por_local AS (
          SELECT "NR_LOCAL_VOTACAO" AS lv, "NR_VOTAVEL" AS nr,
                 MAX("NM_VOTAVEL") AS nm, MAX("SG_PARTIDO") AS partido,
                 SUM("QT_VOTOS"::bigint) AS votos
          FROM boletim_de_urna
          WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
            AND "CD_MUNICIPIO" = :cd AND "CD_CARGO_PERGUNTA" = :cargo
            AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
          GROUP BY 1, 2
        ),
        rk AS (
          SELECT lv, nr, nm, partido, votos,
                 RANK() OVER (PARTITION BY lv ORDER BY votos DESC) AS pos
          FROM por_local
        )
        SELECT nr, nm, partido, COUNT(DISTINCT lv) AS locais
        FROM rk WHERE pos = 1
        GROUP BY 1, 2, 3
        ORDER BY locais DESC
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo},
    )


# ---------------------------------------------------------------------------
# Aba "Card local de votação"
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def locais_do_municipio(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str
) -> pd.DataFrame:
    return run_df(
        '''
        SELECT "NR_LOCAL_VOTACAO" AS nr_local,
               MAX("DS_CARGO_PERGUNTA") AS cargo,
               SUM("QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
          AND "CD_MUNICIPIO" = :cd AND "CD_CARGO_PERGUNTA" = :cargo
        GROUP BY 1 ORDER BY 1
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo},
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def nome_local(ano: int, uf: str, cd_municipio: str, nr_local: str) -> str | None:
    df = run_df(
        '''
        SELECT MAX("NM_LOCAL_VOTACAO") AS nm
        FROM local_votacao
        WHERE "AA_ELEICAO" = :ano AND "SG_UF" = :uf
          AND "CD_MUNICIPIO" = :cd AND "NR_LOCAL_VOTACAO" = :lv
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "lv": nr_local},
    )
    if df.empty:
        return None
    return df["nm"].iloc[0]


@st.cache_data(ttl=TTL, show_spinner=False)
def top_candidatos_no_local(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_local: str, top: int = 10
) -> dict:
    base = (
        '"ANO_ELEICAO" = :ano AND "SG_UF" = :uf AND "CD_MUNICIPIO" = :cd '
        'AND "CD_CARGO_PERGUNTA" = :cargo AND "NR_LOCAL_VOTACAO" = :lv'
    )
    p = {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo, "lv": nr_local, "top": top}

    ranking = run_df(
        f'''
        WITH agg AS (
          SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
                 MAX("SG_PARTIDO") AS partido, SUM("QT_VOTOS"::bigint) AS votos
          FROM boletim_de_urna
          WHERE {base} AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
          GROUP BY 1
        ),
        tot AS (SELECT SUM(votos) AS t FROM agg)
        SELECT nr, nm, partido, votos,
               votos::float / NULLIF((SELECT t FROM tot), 0) * 100 AS pct
        FROM agg ORDER BY votos DESC LIMIT :top
        ''',
        p,
    )

    totais = run_df(
        f'''
        SELECT
          COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" NOT IN ('Branco', 'Nulo')
                            THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
          COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco'
                            THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
          COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo'
                            THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
        FROM boletim_de_urna WHERE {base}
        ''',
        p,
    ).iloc[0]

    return {
        "ranking": ranking,
        "validos": int(totais["validos"] or 0),
        "brancos": int(totais["brancos"] or 0),
        "nulos": int(totais["nulos"] or 0),
    }


# ---------------------------------------------------------------------------
# Aba "Votos por bairro" (fallback via local_votacao, só 2024)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def votos_por_bairro(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_votavel: str
) -> pd.DataFrame:
    return run_df(
        '''
        SELECT COALESCE(NULLIF(TRIM(lv."NM_BAIRRO"), ''), '(sem bairro)') AS bairro,
               MAX(b."NM_VOTAVEL") AS nm_votavel,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        JOIN local_votacao lv
          ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
         AND lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"
        WHERE b."ANO_ELEICAO" = :ano AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd AND b."CD_CARGO_PERGUNTA" = :cargo
          AND b."NR_VOTAVEL" = :nr
        GROUP BY 1
        ORDER BY votos DESC
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo, "nr": nr_votavel},
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def votos_por_local_candidato(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_votavel: str
) -> pd.DataFrame:
    return run_df(
        '''
        SELECT lv."NM_LOCAL_VOTACAO" AS local,
               MAX(b."NM_VOTAVEL") AS nm_votavel,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        JOIN local_votacao lv
          ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
         AND lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"
        WHERE b."ANO_ELEICAO" = :ano AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd AND b."CD_CARGO_PERGUNTA" = :cargo
          AND b."NR_VOTAVEL" = :nr
        GROUP BY 1
        ORDER BY votos DESC
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo, "nr": nr_votavel},
    )
