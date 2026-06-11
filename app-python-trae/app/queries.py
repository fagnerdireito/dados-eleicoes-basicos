from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from app.core import run_df, table_exists

TTL = 3600


def is_municipal(ano: int) -> bool:
    return int(ano) % 4 == 0


def _base_filters(
    ano: int,
    uf: str,
    cd_municipio: str | None = None,
    cd_cargo: str | None = None,
) -> tuple[str, dict[str, Any]]:
    where = ['"ANO_ELEICAO" = :ano', '"SG_UF" = :uf']
    params: dict[str, Any] = {"ano": str(ano), "uf": uf}
    if cd_municipio:
        where.append('"CD_MUNICIPIO" = :cd_municipio')
        params["cd_municipio"] = str(cd_municipio)
    if cd_cargo:
        where.append('"CD_CARGO_PERGUNTA" = :cd_cargo')
        params["cd_cargo"] = str(cd_cargo)
    return " AND ".join(where), params


@st.cache_data(ttl=TTL, show_spinner=False)
def list_years() -> list[int]:
    df = run_df('SELECT DISTINCT "ANO_ELEICAO"::int AS ano FROM boletim_de_urna ORDER BY 1')
    return df["ano"].tolist()


@st.cache_data(ttl=TTL, show_spinner=False)
def list_ufs(ano: int) -> list[str]:
    df = run_df(
        'SELECT DISTINCT "SG_UF" AS uf FROM boletim_de_urna WHERE "ANO_ELEICAO" = :ano ORDER BY 1',
        {"ano": str(ano)},
    )
    return df["uf"].tolist()


@st.cache_data(ttl=TTL, show_spinner=False)
def list_municipios(ano: int, uf: str) -> pd.DataFrame:
    return run_df(
        """
        SELECT "CD_MUNICIPIO" AS cd, MAX("NM_MUNICIPIO") AS nm
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        GROUP BY 1
        ORDER BY 2
        """,
        {"ano": str(ano), "uf": uf},
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def list_cargos(ano: int, uf: str, cd_municipio: str | None) -> pd.DataFrame:
    where, params = _base_filters(ano, uf, cd_municipio)
    return run_df(
        f"""
        SELECT "CD_CARGO_PERGUNTA" AS cd, MAX("DS_CARGO_PERGUNTA") AS ds
        FROM boletim_de_urna
        WHERE {where}
        GROUP BY 1
        ORDER BY 2
        """,
        params,
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def list_candidatos(
    ano: int,
    uf: str,
    cd_municipio: str | None,
    cd_cargo: str,
    limit: int = 50,
) -> pd.DataFrame:
    where, params = _base_filters(ano, uf, cd_municipio, cd_cargo)
    params["limit"] = limit
    return run_df(
        f"""
        SELECT "NR_VOTAVEL" AS nr,
               MAX("NM_VOTAVEL") AS nm,
               MAX("SG_PARTIDO") AS partido,
               SUM("QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna
        WHERE {where}
          AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
        GROUP BY 1
        ORDER BY votos DESC
        LIMIT :limit
        """,
        params,
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def summary_context(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
    nr_votavel: str,
) -> dict[str, Any]:
    where, params = _base_filters(ano, uf, cd_municipio, cd_cargo)
    params["nr_votavel"] = str(nr_votavel)

    ranking = run_df(
        f"""
        WITH agg AS (
            SELECT "NR_VOTAVEL" AS nr,
                   MAX("NM_VOTAVEL") AS nm,
                   SUM("QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna
            WHERE {where}
              AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
            GROUP BY 1
        )
        SELECT nr, nm, votos,
               RANK() OVER (ORDER BY votos DESC) AS posicao,
               COUNT(*) OVER () AS total_candidatos
        FROM agg
        ORDER BY votos DESC
        """,
        params,
    )
    atual = ranking[ranking["nr"].astype(str) == str(nr_votavel)]

    composicao = run_df(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo') THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
            COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
            COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
        FROM boletim_de_urna
        WHERE {where}
        """,
        params,
    ).iloc[0]

    secoes = run_df(
        f"""
        SELECT
            SUM("QT_APTOS"::bigint) AS aptos,
            SUM("QT_COMPARECIMENTO"::bigint) AS comparecimento,
            SUM("QT_ABSTENCOES"::bigint) AS abstencoes
        FROM (
            SELECT DISTINCT "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO", "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES"
            FROM boletim_de_urna
            WHERE {where}
        ) secoes_unicas
        """,
        params,
    ).iloc[0]

    lideranca = run_df(
        f"""
        WITH por_local AS (
            SELECT "NR_LOCAL_VOTACAO" AS nr_local,
                   "NR_VOTAVEL" AS nr,
                   SUM("QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna
            WHERE {where}
              AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
            GROUP BY 1, 2
        ), ranked AS (
            SELECT nr_local, nr, votos,
                   RANK() OVER (PARTITION BY nr_local ORDER BY votos DESC) AS pos
            FROM por_local
        )
        SELECT
            COUNT(*) FILTER (WHERE nr = :nr_votavel AND pos = 1) AS liderancas,
            COUNT(DISTINCT nr_local) AS locais
        FROM ranked
        """,
        params,
    ).iloc[0]

    votos = int(atual["votos"].iloc[0]) if not atual.empty else 0
    validos = int(composicao["validos"] or 0)
    aptos = int(secoes["aptos"] or 0)
    comparecimento = int(secoes["comparecimento"] or 0)

    return {
        "votos_candidato": votos,
        "posicao": int(atual["posicao"].iloc[0]) if not atual.empty else None,
        "total_candidatos": int(atual["total_candidatos"].iloc[0]) if not atual.empty else 0,
        "validos": validos,
        "brancos": int(composicao["brancos"] or 0),
        "nulos": int(composicao["nulos"] or 0),
        "abstencoes": int(secoes["abstencoes"] or 0),
        "comparecimento": comparecimento,
        "pct_validos": (votos / validos * 100) if validos else 0.0,
        "pct_comparecimento": (comparecimento / aptos * 100) if aptos else 0.0,
        "liderancas": int(lideranca["liderancas"] or 0),
        "locais": int(lideranca["locais"] or 0),
    }


@st.cache_data(ttl=TTL, show_spinner=False)
def turnout_by_uf(ano: int, uf: str) -> dict[str, Any]:
    row = run_df(
        """
        SELECT
            SUM("QT_APTOS"::bigint) AS aptos,
            SUM("QT_COMPARECIMENTO"::bigint) AS comparecimento,
            SUM("QT_ABSTENCOES"::bigint) AS abstencoes
        FROM (
            SELECT DISTINCT "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO", "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES"
            FROM boletim_de_urna
            WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        ) secoes_unicas
        """,
        {"ano": str(ano), "uf": uf},
    ).iloc[0]
    aptos = int(row["aptos"] or 0)
    comparecimento = int(row["comparecimento"] or 0)
    abstencoes = int(row["abstencoes"] or 0)
    return {
        "comparecimento": comparecimento,
        "abstencoes": abstencoes,
        "pct_comparecimento": (comparecimento / aptos * 100) if aptos else 0.0,
        "pct_abstencao": (abstencoes / aptos * 100) if aptos else 0.0,
    }


@st.cache_data(ttl=TTL, show_spinner=False)
def profile_breakdowns_available() -> bool:
    names = ["perfil_eleitorado", "perfil_eleitorado_secao", "comparecimento_abstencao"]
    return any(table_exists(name) for name in names)


@st.cache_data(ttl=TTL, show_spinner=False)
def votes_by_municipio(ano: int, uf: str, cd_cargo: str, nr_votavel: str) -> pd.DataFrame:
    return run_df(
        """
        SELECT "CD_MUNICIPIO" AS cd,
               MAX("NM_MUNICIPIO") AS nm,
               SUM("QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :ano
          AND "SG_UF" = :uf
          AND "CD_CARGO_PERGUNTA" = :cd_cargo
          AND "NR_VOTAVEL" = :nr_votavel
        GROUP BY 1
        ORDER BY votos DESC, nm
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_cargo": str(cd_cargo),
            "nr_votavel": str(nr_votavel),
        },
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def votes_by_local(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
    nr_votavel: str,
) -> pd.DataFrame:
    if not table_exists("local_votacao"):
        return pd.DataFrame()
    return run_df(
        """
        SELECT b."NR_LOCAL_VOTACAO" AS nr_local,
               COALESCE(MAX(lv."NM_LOCAL_VOTACAO"), 'Local ' || b."NR_LOCAL_VOTACAO") AS nm_local,
               MAX(lv."NM_BAIRRO") AS bairro,
               NULLIF(MAX(lv."NR_LATITUDE"), '')::double precision AS lat,
               NULLIF(MAX(lv."NR_LONGITUDE"), '')::double precision AS lng,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        LEFT JOIN local_votacao lv
          ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
         AND lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"
        WHERE b."ANO_ELEICAO" = :ano
          AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd_municipio
          AND b."CD_CARGO_PERGUNTA" = :cd_cargo
          AND b."NR_VOTAVEL" = :nr_votavel
        GROUP BY 1
        ORDER BY votos DESC, nm_local
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
            "nr_votavel": str(nr_votavel),
        },
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def ranking_for_year(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
    top: int = 10,
) -> pd.DataFrame:
    return run_df(
        f"""
        WITH agg AS (
            SELECT "NR_VOTAVEL" AS nr,
                   MAX("NM_VOTAVEL") AS nm,
                   MAX("SG_PARTIDO") AS partido,
                   SUM("QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna
            WHERE "ANO_ELEICAO" = :ano
              AND "SG_UF" = :uf
              AND "CD_MUNICIPIO" = :cd_municipio
              AND "CD_CARGO_PERGUNTA" = :cd_cargo
              AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
            GROUP BY 1
        ), total AS (
            SELECT SUM(votos) AS total_votos FROM agg
        )
        SELECT nr, nm, partido, votos,
               (votos::double precision / NULLIF((SELECT total_votos FROM total), 0) * 100) AS pct
        FROM agg
        ORDER BY votos DESC, nm
        LIMIT {int(top)}
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
        },
    )


def previous_cycle_year(ano: int) -> int | None:
    years = [item for item in list_years() if item < int(ano)]
    if not years:
        return None
    target = int(ano) - 4
    return target if target in years else max(years)


@st.cache_data(ttl=TTL, show_spinner=False)
def territorial_synthesis(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
) -> pd.DataFrame:
    return run_df(
        """
        WITH por_local AS (
            SELECT "NR_LOCAL_VOTACAO" AS nr_local,
                   "NR_VOTAVEL" AS nr,
                   MAX("NM_VOTAVEL") AS nm,
                   MAX("SG_PARTIDO") AS partido,
                   SUM("QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna
            WHERE "ANO_ELEICAO" = :ano
              AND "SG_UF" = :uf
              AND "CD_MUNICIPIO" = :cd_municipio
              AND "CD_CARGO_PERGUNTA" = :cd_cargo
              AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
            GROUP BY 1, 2
        ), ranked AS (
            SELECT nr_local, nr, nm, partido, votos,
                   RANK() OVER (PARTITION BY nr_local ORDER BY votos DESC) AS pos
            FROM por_local
        )
        SELECT nr, nm, partido, COUNT(DISTINCT nr_local) AS locais
        FROM ranked
        WHERE pos = 1
        GROUP BY 1, 2, 3
        ORDER BY locais DESC, nm
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
        },
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def list_locais(ano: int, uf: str, cd_municipio: str, cd_cargo: str) -> pd.DataFrame:
    if table_exists("local_votacao"):
        return run_df(
            """
            SELECT b."NR_LOCAL_VOTACAO" AS nr_local,
                   COALESCE(MAX(lv."NM_LOCAL_VOTACAO"), 'Local ' || b."NR_LOCAL_VOTACAO") AS nm_local,
                   SUM(CASE WHEN COALESCE(b."DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo') THEN b."QT_VOTOS"::bigint ELSE 0 END) AS validos,
                   SUM(CASE WHEN b."DS_TIPO_VOTAVEL" = 'Branco' THEN b."QT_VOTOS"::bigint ELSE 0 END) AS brancos,
                   SUM(CASE WHEN b."DS_TIPO_VOTAVEL" = 'Nulo' THEN b."QT_VOTOS"::bigint ELSE 0 END) AS nulos
            FROM boletim_de_urna b
            LEFT JOIN local_votacao lv
              ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
             AND lv."SG_UF" = b."SG_UF"
             AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
             AND lv."NR_ZONA" = b."NR_ZONA"
             AND lv."NR_SECAO" = b."NR_SECAO"
            WHERE b."ANO_ELEICAO" = :ano
              AND b."SG_UF" = :uf
              AND b."CD_MUNICIPIO" = :cd_municipio
              AND b."CD_CARGO_PERGUNTA" = :cd_cargo
            GROUP BY 1
            ORDER BY validos DESC, nm_local
            """,
            {
                "ano": str(ano),
                "uf": uf,
                "cd_municipio": str(cd_municipio),
                "cd_cargo": str(cd_cargo),
            },
        )
    return run_df(
        """
        SELECT "NR_LOCAL_VOTACAO" AS nr_local,
               'Local ' || "NR_LOCAL_VOTACAO" AS nm_local,
               SUM(CASE WHEN COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo') THEN "QT_VOTOS"::bigint ELSE 0 END) AS validos,
               SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco' THEN "QT_VOTOS"::bigint ELSE 0 END) AS brancos,
               SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo' THEN "QT_VOTOS"::bigint ELSE 0 END) AS nulos
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :ano
          AND "SG_UF" = :uf
          AND "CD_MUNICIPIO" = :cd_municipio
          AND "CD_CARGO_PERGUNTA" = :cd_cargo
        GROUP BY 1
        ORDER BY validos DESC, nm_local
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
        },
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def local_card(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
    nr_local: str,
    nr_votavel: str,
) -> dict[str, Any]:
    ranking = run_df(
        """
        WITH agg AS (
            SELECT "NR_VOTAVEL" AS nr,
                   MAX("NM_VOTAVEL") AS nm,
                   MAX("SG_PARTIDO") AS partido,
                   SUM("QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna
            WHERE "ANO_ELEICAO" = :ano
              AND "SG_UF" = :uf
              AND "CD_MUNICIPIO" = :cd_municipio
              AND "CD_CARGO_PERGUNTA" = :cd_cargo
              AND "NR_LOCAL_VOTACAO" = :nr_local
              AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
            GROUP BY 1
        ), total AS (
            SELECT SUM(votos) AS total_votos FROM agg
        )
        SELECT nr, nm, partido, votos,
               (votos::double precision / NULLIF((SELECT total_votos FROM total), 0) * 100) AS pct,
               RANK() OVER (ORDER BY votos DESC) AS pos
        FROM agg
        ORDER BY votos DESC, nm
        LIMIT 10
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
            "nr_local": str(nr_local),
        },
    )
    totals = run_df(
        """
        SELECT
            COALESCE(SUM(CASE WHEN COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo') THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
            COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
            COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :ano
          AND "SG_UF" = :uf
          AND "CD_MUNICIPIO" = :cd_municipio
          AND "CD_CARGO_PERGUNTA" = :cd_cargo
          AND "NR_LOCAL_VOTACAO" = :nr_local
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
            "nr_local": str(nr_local),
        },
    ).iloc[0]
    destaque = ranking[ranking["nr"].astype(str) == str(nr_votavel)]
    return {
        "ranking": ranking,
        "validos": int(totals["validos"] or 0),
        "brancos": int(totals["brancos"] or 0),
        "nulos": int(totals["nulos"] or 0),
        "destaque": destaque.iloc[0].to_dict() if not destaque.empty else None,
    }


@st.cache_data(ttl=TTL, show_spinner=False)
def candidate_breakdowns(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
    nr_votavel: str,
    nm_municipio: str | None,
) -> dict[str, pd.DataFrame]:
    municipios = votes_by_municipio(ano, uf, cd_cargo, nr_votavel)
    municipio = pd.DataFrame(
        [
            {
                "municipio": nm_municipio or "(nao identificado)",
                "ano": int(ano),
                "votos": int(municipios[municipios["cd"].astype(str) == str(cd_municipio)]["votos"].sum()),
            }
        ]
    )
    if not table_exists("local_votacao"):
        return {"municipio": municipio, "bairro": pd.DataFrame(), "local": pd.DataFrame()}

    bairro = run_df(
        """
        SELECT COALESCE(NULLIF(TRIM(MAX(lv."NM_BAIRRO")), ''), '(sem bairro)') AS bairro,
               :ano::int AS ano,
               SUM(b."QT_VOTOS"::bigint) AS votos,
               MAX(b."NM_VOTAVEL") AS nm_votavel
        FROM boletim_de_urna b
        LEFT JOIN local_votacao lv
          ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
         AND lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"
        WHERE b."ANO_ELEICAO" = :ano
          AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd_municipio
          AND b."CD_CARGO_PERGUNTA" = :cd_cargo
          AND b."NR_VOTAVEL" = :nr_votavel
        GROUP BY 1
        ORDER BY votos DESC, bairro
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
            "nr_votavel": str(nr_votavel),
        },
    )
    local = run_df(
        """
        SELECT COALESCE(MAX(lv."NM_LOCAL_VOTACAO"), 'Local ' || b."NR_LOCAL_VOTACAO") AS local,
               :ano::int AS ano,
               SUM(b."QT_VOTOS"::bigint) AS votos,
               MAX(b."NM_VOTAVEL") AS nm_votavel
        FROM boletim_de_urna b
        LEFT JOIN local_votacao lv
          ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
         AND lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"
        WHERE b."ANO_ELEICAO" = :ano
          AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd_municipio
          AND b."CD_CARGO_PERGUNTA" = :cd_cargo
          AND b."NR_VOTAVEL" = :nr_votavel
        GROUP BY b."NR_LOCAL_VOTACAO"
        ORDER BY votos DESC, local
        """,
        {
            "ano": str(ano),
            "uf": uf,
            "cd_municipio": str(cd_municipio),
            "cd_cargo": str(cd_cargo),
            "nr_votavel": str(nr_votavel),
        },
    )
    return {"municipio": municipio, "bairro": bairro, "local": local}
