"""Consultas eleitorais. Nenhuma funcao altera o banco de dados."""

from __future__ import annotations

from typing import Any

import pandas as pd

from election_app.database import query_df
from election_app.models import ElectionScope


BASE_SCOPE = """
    b.\"ANO_ELEICAO\" = :year
    AND b.\"CD_ELEICAO\" = :election_code
    AND b.\"NR_TURNO\" = :round_number
    AND b.\"SG_UF\" = :uf
"""


def list_elections() -> pd.DataFrame:
    return query_df(
        """
        SELECT b."ANO_ELEICAO"::int AS year,
               b."CD_ELEICAO" AS election_code,
               MAX(b."DS_ELEICAO") AS election_name,
               b."NR_TURNO"::int AS round_number,
               MAX(b."NM_TIPO_ELEICAO") AS election_type,
               MAX(b."DT_PLEITO") AS election_date,
               BOOL_OR(UPPER(b."DS_CARGO_PERGUNTA") IN ('PREFEITO', 'VEREADOR')) AS is_municipal
        FROM boletim_de_urna b
        GROUP BY 1, 2, 4
        ORDER BY year DESC, election_name, round_number
        """
    )


def list_ufs(year: int, election_code: str, round_number: int) -> list[str]:
    df = query_df(
        """
        SELECT DISTINCT "SG_UF" AS uf
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :year
          AND "CD_ELEICAO" = :election_code
          AND "NR_TURNO" = :round_number
        ORDER BY 1
        """,
        {"year": str(year), "election_code": election_code, "round_number": str(round_number)},
    )
    return df["uf"].tolist()


def list_municipalities(year: int, election_code: str, round_number: int, uf: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT DISTINCT "CD_MUNICIPIO" AS code, "NM_MUNICIPIO" AS name
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :year
          AND "CD_ELEICAO" = :election_code
          AND "NR_TURNO" = :round_number
          AND "SG_UF" = :uf
        ORDER BY name
        """,
        {
            "year": str(year),
            "election_code": election_code,
            "round_number": str(round_number),
            "uf": uf,
        },
    )


def list_offices(
    year: int,
    election_code: str,
    round_number: int,
    uf: str,
    municipality_code: str | None,
) -> pd.DataFrame:
    municipality_filter = "AND \"CD_MUNICIPIO\" = :municipality_code" if municipality_code else ""
    return query_df(
        f"""
        SELECT code, name
        FROM (
          SELECT DISTINCT "CD_CARGO_PERGUNTA" AS code, "DS_CARGO_PERGUNTA" AS name
          FROM boletim_de_urna
          WHERE "ANO_ELEICAO" = :year
            AND "CD_ELEICAO" = :election_code
            AND "NR_TURNO" = :round_number
            AND "SG_UF" = :uf
            {municipality_filter}
        ) offices
        ORDER BY code::int
        """,
        {
            "year": str(year),
            "election_code": election_code,
            "round_number": str(round_number),
            "uf": uf,
            "municipality_code": municipality_code,
        },
    )


def list_candidates(
    year: int,
    election_code: str,
    round_number: int,
    uf: str,
    municipality_code: str | None,
    office_code: str,
) -> pd.DataFrame:
    municipality_filter = "AND \"CD_MUNICIPIO\" = :municipality_code" if municipality_code else ""
    return query_df(
        f"""
        SELECT "NR_VOTAVEL" AS number,
               MAX("NM_VOTAVEL") AS name,
               MAX(NULLIF("SG_PARTIDO", '#NULO#')) AS party,
               SUM(NULLIF("QT_VOTOS", '')::bigint) AS votes
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO" = :year
          AND "CD_ELEICAO" = :election_code
          AND "NR_TURNO" = :round_number
          AND "SG_UF" = :uf
          AND "CD_CARGO_PERGUNTA" = :office_code
          AND LOWER(COALESCE("DS_TIPO_VOTAVEL", '')) = 'nominal'
          {municipality_filter}
        GROUP BY "NR_VOTAVEL"
        ORDER BY votes DESC, name
        LIMIT 500
        """,
        {
            "year": str(year),
            "election_code": election_code,
            "round_number": str(round_number),
            "uf": uf,
            "municipality_code": municipality_code,
            "office_code": office_code,
        },
    )


def data_capabilities(scope: ElectionScope) -> dict[str, bool]:
    params = scope.sql_params
    df = query_df(
        """
        SELECT
          EXISTS (
            SELECT 1 FROM perfil_eleitorado p
            WHERE p."ANO_ELEICAO" = :year AND p."SG_UF" = :uf
          ) AS electorate_profile,
          EXISTS (
            SELECT 1 FROM local_votacao l
            WHERE l."AA_ELEICAO" = :year AND l."NR_TURNO" = :round_number
              AND l."SG_UF" = :uf
          ) AS voting_locations,
          EXISTS (
            SELECT 1 FROM local_votacao l
            WHERE l."AA_ELEICAO" = :year AND l."NR_TURNO" = :round_number
              AND l."SG_UF" = :uf
              AND NULLIF(l."NR_LATITUDE", '') IS NOT NULL
              AND l."NR_LATITUDE" <> '-1'
          ) AS coordinates,
          EXISTS (
            SELECT 1 FROM consulta_cand c
            WHERE c."ANO_ELEICAO" = :year AND c."SG_UF" = :uf
          ) AS candidate_registry
        """,
        params,
    )
    return {key: bool(df.iloc[0][key]) for key in df.columns}


def _municipality_clause(scope: ElectionScope, alias: str = "b") -> str:
    return f'AND {alias}."CD_MUNICIPIO" = :municipality_code' if scope.has_municipality else ""


def municipal_summary(scope: ElectionScope) -> dict[str, Any]:
    if not scope.has_municipality:
        return {}
    params = scope.sql_params
    summary = query_df(
        f"""
        WITH nominal AS (
          SELECT b."NR_VOTAVEL" AS number,
                 MAX(b."NM_VOTAVEL") AS name,
                 SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
          FROM boletim_de_urna b
          WHERE {BASE_SCOPE}
            AND b."CD_MUNICIPIO" = :municipality_code
            AND b."CD_CARGO_PERGUNTA" = :office_code
            AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
          GROUP BY b."NR_VOTAVEL"
        ), ranked AS (
          SELECT *, ROW_NUMBER() OVER (ORDER BY votes DESC, number) AS position
          FROM nominal
        ), composition AS (
          SELECT
            SUM(CASE WHEN LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) NOT IN ('branco', 'nulo')
                     THEN NULLIF(b."QT_VOTOS", '')::bigint ELSE 0 END) AS valid_votes,
            SUM(CASE WHEN LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'branco'
                     THEN NULLIF(b."QT_VOTOS", '')::bigint ELSE 0 END) AS blank_votes,
            SUM(CASE WHEN LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nulo'
                     THEN NULLIF(b."QT_VOTOS", '')::bigint ELSE 0 END) AS null_votes
          FROM boletim_de_urna b
          WHERE {BASE_SCOPE}
            AND b."CD_MUNICIPIO" = :municipality_code
            AND b."CD_CARGO_PERGUNTA" = :office_code
        ), sections AS (
          SELECT SUM(apt::bigint) AS eligible,
                 SUM(attendance::bigint) AS attendance,
                 SUM(abstentions::bigint) AS abstentions
          FROM (
            SELECT DISTINCT b."NR_ZONA", b."NR_SECAO",
                   NULLIF(b."QT_APTOS", '') AS apt,
                   NULLIF(b."QT_COMPARECIMENTO", '') AS attendance,
                   NULLIF(b."QT_ABSTENCOES", '') AS abstentions
            FROM boletim_de_urna b
            WHERE {BASE_SCOPE}
              AND b."CD_MUNICIPIO" = :municipality_code
              AND b."CD_CARGO_PERGUNTA" = :office_code
          ) s
        ), local_votes AS (
          SELECT b."NR_LOCAL_VOTACAO" AS location_number,
                 b."NR_VOTAVEL" AS candidate_number,
                 SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
          FROM boletim_de_urna b
          WHERE {BASE_SCOPE}
            AND b."CD_MUNICIPIO" = :municipality_code
            AND b."CD_CARGO_PERGUNTA" = :office_code
            AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
          GROUP BY 1, 2
        ), leaders AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY location_number ORDER BY votes DESC, candidate_number
          ) AS position
          FROM local_votes
        )
        SELECT r.votes AS candidate_votes,
               r.position,
               c.valid_votes, c.blank_votes, c.null_votes,
               s.eligible, s.attendance, s.abstentions,
               COUNT(*) FILTER (
                 WHERE l.position = 1 AND l.candidate_number = :candidate_number
               ) AS led_locations,
               COUNT(DISTINCT l.location_number) AS total_locations
        FROM ranked r
        CROSS JOIN composition c
        CROSS JOIN sections s
        CROSS JOIN leaders l
        WHERE r.number = :candidate_number
        GROUP BY r.votes, r.position, c.valid_votes, c.blank_votes, c.null_votes,
                 s.eligible, s.attendance, s.abstentions
        """,
        params,
    )
    if summary.empty:
        return {}
    result = summary.iloc[0].to_dict()
    valid = int(result.get("valid_votes") or 0)
    candidate = int(result.get("candidate_votes") or 0)
    eligible = int(result.get("eligible") or 0)
    attendance = int(result.get("attendance") or 0)
    result["vote_share"] = candidate / valid * 100 if valid else 0.0
    result["attendance_rate"] = attendance / eligible * 100 if eligible else 0.0
    return result


def state_turnout(scope: ElectionScope) -> dict[str, Any]:
    df = query_df(
        f"""
        SELECT SUM(eligible::bigint) AS eligible,
               SUM(attendance::bigint) AS attendance,
               SUM(abstentions::bigint) AS abstentions
        FROM (
          SELECT DISTINCT b."CD_MUNICIPIO", b."NR_ZONA", b."NR_SECAO",
                 NULLIF(b."QT_APTOS", '') AS eligible,
                 NULLIF(b."QT_COMPARECIMENTO", '') AS attendance,
                 NULLIF(b."QT_ABSTENCOES", '') AS abstentions
          FROM boletim_de_urna b
          WHERE {BASE_SCOPE}
            AND b."CD_CARGO_PERGUNTA" = :office_code
        ) s
        """,
        scope.sql_params,
    )
    result = df.iloc[0].to_dict()
    eligible = int(result.get("eligible") or 0)
    attendance = int(result.get("attendance") or 0)
    abstentions = int(result.get("abstentions") or 0)
    return {
        "eligible": eligible,
        "attendance": attendance,
        "abstentions": abstentions,
        "attendance_rate": attendance / eligible * 100 if eligible else 0,
        "abstention_rate": abstentions / eligible * 100 if eligible else 0,
    }


def electorate_profile(scope: ElectionScope) -> tuple[pd.DataFrame, pd.DataFrame]:
    params = {"year": str(scope.election.year), "uf": scope.uf}
    age = query_df(
        """
        SELECT COALESCE(NULLIF("DS_FAIXA_ETARIA", '#NULO#'), 'Nao informado') AS label,
               SUM(NULLIF("QT_ELEITORES_PERFIL", '')::bigint) AS voters
        FROM perfil_eleitorado
        WHERE "ANO_ELEICAO" = :year AND "SG_UF" = :uf
        GROUP BY 1
        ORDER BY MIN(NULLIF("CD_FAIXA_ETARIA", '')::int) NULLS FIRST
        """,
        params,
    )
    education = query_df(
        """
        SELECT COALESCE(NULLIF("DS_GRAU_ESCOLARIDADE", '#NULO#'), 'Nao informado') AS label,
               SUM(NULLIF("QT_ELEITORES_PERFIL", '')::bigint) AS voters
        FROM perfil_eleitorado
        WHERE "ANO_ELEICAO" = :year AND "SG_UF" = :uf
        GROUP BY 1
        ORDER BY voters DESC
        """,
        params,
    )
    return age, education


def votes_by_municipality(scope: ElectionScope) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT b."CD_MUNICIPIO" AS municipality_code,
               MAX(b."NM_MUNICIPIO") AS municipality,
               MAX(m.cd_municipio_ibge) AS ibge_code,
               SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
        FROM boletim_de_urna b
        LEFT JOIN municipio_tse_ibge m
          ON m.sg_uf = b."SG_UF" AND m.cd_municipio_tse = b."CD_MUNICIPIO"
        WHERE {BASE_SCOPE}
          AND b."CD_CARGO_PERGUNTA" = :office_code
          AND b."NR_VOTAVEL" = :candidate_number
          AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
        GROUP BY b."CD_MUNICIPIO"
        ORDER BY votes DESC, municipality
        """,
        scope.sql_params,
    )


def votes_by_location(scope: ElectionScope) -> pd.DataFrame:
    if not scope.has_municipality:
        return pd.DataFrame()
    return query_df(
        f"""
        SELECT b."NR_LOCAL_VOTACAO" AS location_number,
               COALESCE(MAX(NULLIF(l."NM_LOCAL_VOTACAO", '')),
                        'Local ' || b."NR_LOCAL_VOTACAO") AS location,
               COALESCE(MAX(NULLIF(l."NM_BAIRRO", '')), 'Nao informado') AS neighborhood,
               NULLIF(MAX(NULLIF(l."NR_LATITUDE", '-1')), '')::double precision AS latitude,
               NULLIF(MAX(NULLIF(l."NR_LONGITUDE", '-1')), '')::double precision AS longitude,
               SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
        FROM boletim_de_urna b
        LEFT JOIN local_votacao l
          ON l."AA_ELEICAO" = b."ANO_ELEICAO"
         AND l."NR_TURNO" = b."NR_TURNO"
         AND l."SG_UF" = b."SG_UF"
         AND l."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND l."NR_ZONA" = b."NR_ZONA"
         AND l."NR_SECAO" = b."NR_SECAO"
        WHERE {BASE_SCOPE}
          AND b."CD_MUNICIPIO" = :municipality_code
          AND b."CD_CARGO_PERGUNTA" = :office_code
          AND b."NR_VOTAVEL" = :candidate_number
          AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
        GROUP BY b."NR_LOCAL_VOTACAO"
        ORDER BY votes DESC, location
        """,
        scope.sql_params,
    )


def ranking(scope: ElectionScope, year: int | None = None) -> pd.DataFrame:
    if not scope.has_municipality:
        return pd.DataFrame()
    if year is None or year == scope.election.year:
        params = scope.sql_params
        election_filter = f"{BASE_SCOPE} AND b.\"CD_CARGO_PERGUNTA\" = :office_code"
    else:
        params = {
            "year": str(year),
            "uf": scope.uf,
            "municipality_code": scope.municipality_code,
            "office_name": scope.office_name.upper(),
        }
        election_filter = """
            b."ANO_ELEICAO" = :year AND b."NR_TURNO" = '1'
            AND b."SG_UF" = :uf AND UPPER(b."DS_CARGO_PERGUNTA") = :office_name
        """
    return query_df(
        f"""
        WITH agg AS (
          SELECT b."NR_VOTAVEL" AS number,
                 MAX(b."NM_VOTAVEL") AS name,
                 MAX(NULLIF(b."SG_PARTIDO", '#NULO#')) AS party,
                 SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
          FROM boletim_de_urna b
          WHERE {election_filter}
            AND b."CD_MUNICIPIO" = :municipality_code
            AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
          GROUP BY b."NR_VOTAVEL"
        )
        SELECT number, name, party, votes,
               votes::double precision / NULLIF(SUM(votes) OVER (), 0) * 100 AS percentage
        FROM agg
        ORDER BY votes DESC, number
        LIMIT 10
        """,
        params,
    )


def previous_cycle_year(scope: ElectionScope) -> int | None:
    if not scope.has_municipality:
        return None
    df = query_df(
        """
        SELECT MAX("ANO_ELEICAO"::int) AS year
        FROM boletim_de_urna
        WHERE "ANO_ELEICAO"::int < :current_year
          AND "SG_UF" = :uf
          AND "CD_MUNICIPIO" = :municipality_code
          AND UPPER("DS_CARGO_PERGUNTA") = :office_name
        """,
        {
            "current_year": scope.election.year,
            "uf": scope.uf,
            "municipality_code": scope.municipality_code,
            "office_name": scope.office_name.upper(),
        },
    )
    value = df.iloc[0]["year"]
    return int(value) if pd.notna(value) else None


def territorial_leaders(scope: ElectionScope) -> pd.DataFrame:
    if not scope.has_municipality:
        return pd.DataFrame()
    return query_df(
        f"""
        WITH local_votes AS (
          SELECT b."NR_LOCAL_VOTACAO" AS location_number,
                 b."NR_VOTAVEL" AS number,
                 MAX(b."NM_VOTAVEL") AS name,
                 MAX(NULLIF(b."SG_PARTIDO", '#NULO#')) AS party,
                 SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
          FROM boletim_de_urna b
          WHERE {BASE_SCOPE}
            AND b."CD_MUNICIPIO" = :municipality_code
            AND b."CD_CARGO_PERGUNTA" = :office_code
            AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
          GROUP BY 1, 2
        ), leaders AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY location_number ORDER BY votes DESC, number
          ) AS position
          FROM local_votes
        )
        SELECT number, MAX(name) AS name, MAX(party) AS party,
               COUNT(*) AS locations
        FROM leaders
        WHERE position = 1
        GROUP BY number
        ORDER BY locations DESC, number
        """,
        scope.sql_params,
    )


def location_options(scope: ElectionScope) -> pd.DataFrame:
    if not scope.has_municipality:
        return pd.DataFrame()
    return query_df(
        f"""
        SELECT b."NR_LOCAL_VOTACAO" AS number,
               COALESCE(MAX(NULLIF(l."NM_LOCAL_VOTACAO", '')),
                        'Local ' || b."NR_LOCAL_VOTACAO") AS name
        FROM boletim_de_urna b
        LEFT JOIN local_votacao l
          ON l."AA_ELEICAO" = b."ANO_ELEICAO"
         AND l."NR_TURNO" = b."NR_TURNO"
         AND l."SG_UF" = b."SG_UF"
         AND l."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND l."NR_ZONA" = b."NR_ZONA"
         AND l."NR_SECAO" = b."NR_SECAO"
        WHERE {BASE_SCOPE}
          AND b."CD_MUNICIPIO" = :municipality_code
          AND b."CD_CARGO_PERGUNTA" = :office_code
        GROUP BY b."NR_LOCAL_VOTACAO"
        ORDER BY name
        """,
        scope.sql_params,
    )


def location_result(scope: ElectionScope, location_number: str) -> tuple[pd.DataFrame, dict[str, int]]:
    params = {**scope.sql_params, "location_number": location_number}
    ranking_df = query_df(
        f"""
        WITH agg AS (
          SELECT b."NR_VOTAVEL" AS number,
                 MAX(b."NM_VOTAVEL") AS name,
                 MAX(NULLIF(b."SG_PARTIDO", '#NULO#')) AS party,
                 SUM(NULLIF(b."QT_VOTOS", '')::bigint) AS votes
          FROM boletim_de_urna b
          WHERE {BASE_SCOPE}
            AND b."CD_MUNICIPIO" = :municipality_code
            AND b."CD_CARGO_PERGUNTA" = :office_code
            AND b."NR_LOCAL_VOTACAO" = :location_number
            AND LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nominal'
          GROUP BY b."NR_VOTAVEL"
        )
        SELECT number, name, party, votes,
               votes::double precision / NULLIF(SUM(votes) OVER (), 0) * 100 AS percentage
        FROM agg ORDER BY votes DESC, number LIMIT 10
        """,
        params,
    )
    composition = query_df(
        f"""
        SELECT
          SUM(CASE WHEN LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) NOT IN ('branco', 'nulo')
                   THEN NULLIF(b."QT_VOTOS", '')::bigint ELSE 0 END) AS valid_votes,
          SUM(CASE WHEN LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'branco'
                   THEN NULLIF(b."QT_VOTOS", '')::bigint ELSE 0 END) AS blank_votes,
          SUM(CASE WHEN LOWER(COALESCE(b."DS_TIPO_VOTAVEL", '')) = 'nulo'
                   THEN NULLIF(b."QT_VOTOS", '')::bigint ELSE 0 END) AS null_votes
        FROM boletim_de_urna b
        WHERE {BASE_SCOPE}
          AND b."CD_MUNICIPIO" = :municipality_code
          AND b."CD_CARGO_PERGUNTA" = :office_code
          AND b."NR_LOCAL_VOTACAO" = :location_number
        """,
        params,
    ).iloc[0]
    return ranking_df, {key: int(composition[key] or 0) for key in composition.index}


def geographic_detail(scope: ElectionScope) -> tuple[pd.DataFrame, pd.DataFrame]:
    locations = votes_by_location(scope)
    if locations.empty:
        return pd.DataFrame(), pd.DataFrame()
    neighborhoods = (
        locations.groupby("neighborhood", as_index=False)["votes"]
        .sum()
        .sort_values(["votes", "neighborhood"], ascending=[False, True])
    )
    return neighborhoods, locations
