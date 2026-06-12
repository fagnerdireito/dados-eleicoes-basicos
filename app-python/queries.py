"""Funções de consulta — uma por necessidade da UI. Todas cacheadas por parâmetros."""
from __future__ import annotations

from typing import Optional

import pandas as pd
import streamlit as st

from db import run_df, table_exists

TTL = 3600

# Locais de votação são cadastrais (UF + município + zona + seção), sem vínculo ao ano.
_LV_JOIN = '''
        JOIN local_votacao lv
          ON lv."SG_UF" = b."SG_UF"
         AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
         AND lv."NR_ZONA" = b."NR_ZONA"
         AND lv."NR_SECAO" = b."NR_SECAO"'''


# ---------------------------------------------------------------------------
# Aba "Perfil do eleitorado" (UF)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def turnout_uf(ano: int, uf: str) -> dict:
    """Comparecimento e abstenção totais da UF (a partir do boletim de urna).

    Agrega por seção distinta (CD_MUNICIPIO+NR_ZONA+NR_SECAO) — QT_APTOS,
    QT_COMPARECIMENTO e QT_ABSTENCOES são repetidos por candidato no boletim.
    """
    df = run_df(
        '''
        SELECT SUM("QT_APTOS"::bigint)          AS aptos,
               SUM("QT_COMPARECIMENTO"::bigint) AS comparec,
               SUM("QT_ABSTENCOES"::bigint)     AS abstenc
        FROM (
          SELECT DISTINCT "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO",
                          "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES"
          FROM boletim_de_urna
          WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        ) s
        ''',
        {"ano": str(ano), "uf": uf},
    )
    if df.empty:
        return {"aptos": 0, "comparec": 0, "abstenc": 0, "pct_comparec": 0.0, "pct_abstenc": 0.0}
    aptos = int(df["aptos"].iloc[0] or 0)
    comparec = int(df["comparec"].iloc[0] or 0)
    abstenc = int(df["abstenc"].iloc[0] or 0)
    return {
        "aptos": aptos,
        "comparec": comparec,
        "abstenc": abstenc,
        "pct_comparec": (comparec / aptos * 100) if aptos else 0.0,
        "pct_abstenc": (abstenc / aptos * 100) if aptos else 0.0,
    }


@st.cache_data(ttl=TTL, show_spinner=False)
def perfil_faixa_etaria(ano: int, uf: str) -> pd.DataFrame:
    """Eleitorado por faixa etária na UF — ordenado pela própria faixa (idade asc)."""
    return run_df(
        '''
        SELECT COALESCE(NULLIF("DS_FAIXA_ETARIA", '#NULO#'), 'Não informado') AS label,
               SUM(NULLIF("QT_ELEITORES_PERFIL", '')::bigint) AS eleitores
        FROM perfil_eleitorado
        WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        GROUP BY 1
        ORDER BY MIN(NULLIF("CD_FAIXA_ETARIA", '')::int) NULLS LAST
        ''',
        {"ano": str(ano), "uf": uf},
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def perfil_escolaridade(ano: int, uf: str) -> pd.DataFrame:
    """Eleitorado por escolaridade na UF — ordenado por nº de eleitores DESC."""
    return run_df(
        '''
        SELECT COALESCE(NULLIF("DS_GRAU_ESCOLARIDADE", '#NULO#'), 'Não informado') AS label,
               SUM(NULLIF("QT_ELEITORES_PERFIL", '')::bigint) AS eleitores
        FROM perfil_eleitorado
        WHERE "ANO_ELEICAO" = :ano AND "SG_UF" = :uf
        GROUP BY 1
        ORDER BY eleitores DESC
        ''',
        {"ano": str(ano), "uf": uf},
    )

# ---------------------------------------------------------------------------
# Catálogos para os filtros globais
# (preferem catalogo_boletim — gerado por go_postgres/13_build_catalogo_filtros.go)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def _usa_catalogo_filtros() -> bool:
    return table_exists("catalogo_boletim")


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_anos() -> list[int]:
    if _usa_catalogo_filtros():
        df = run_df("SELECT DISTINCT ano::int AS ano FROM catalogo_boletim ORDER BY 1")
    else:
        df = run_df(
            'SELECT DISTINCT "ANO_ELEICAO"::int AS ano FROM boletim_de_urna ORDER BY 1'
        )
    return df["ano"].tolist()


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_ufs(ano: int) -> list[str]:
    if _usa_catalogo_filtros():
        df = run_df(
            "SELECT DISTINCT sg_uf AS uf FROM catalogo_boletim "
            "WHERE ano = :ano ORDER BY 1",
            {"ano": str(ano)},
        )
    else:
        df = run_df(
            'SELECT DISTINCT "SG_UF" AS uf FROM boletim_de_urna '
            'WHERE "ANO_ELEICAO" = :ano ORDER BY 1',
            {"ano": str(ano)},
        )
    return df["uf"].tolist()


@st.cache_data(ttl=TTL, show_spinner=False)
def listar_municipios(ano: int, uf: str) -> pd.DataFrame:
    if _usa_catalogo_filtros():
        return run_df(
            '''
            SELECT DISTINCT cd_municipio AS cd, nm_municipio AS nm
            FROM catalogo_boletim
            WHERE ano = :ano AND sg_uf = :uf
            ORDER BY 2
            ''',
            {"ano": str(ano), "uf": uf},
        )
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
    if _usa_catalogo_filtros():
        where = ["ano = :ano", "sg_uf = :uf"]
        params = {"ano": str(ano), "uf": uf}
        if cd_municipio:
            where.append("cd_municipio = :cd")
            params["cd"] = cd_municipio
        sql = f'''
            SELECT DISTINCT cd_cargo AS cd, ds_cargo AS ds
            FROM catalogo_boletim
            WHERE {' AND '.join(where)}
            ORDER BY 2
        '''
        return run_df(sql, params)

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
    if _usa_catalogo_filtros():
        where = ["ano = :ano", "sg_uf = :uf", "cd_cargo = :cargo"]
        params = {"ano": str(ano), "uf": uf, "cargo": cd_cargo}
        if cd_municipio:
            where.append("cd_municipio = :cd")
            params["cd"] = cd_municipio
        sql = f'''
            SELECT nr_votavel AS nr,
                   MAX(nm_votavel) AS nm,
                   MAX(sg_partido) AS sg_partido,
                   SUM(total_votos) AS votos
            FROM catalogo_boletim
            WHERE {' AND '.join(where)}
            GROUP BY nr_votavel
            ORDER BY MAX(nm_votavel) ASC
        '''
        return run_df(sql, params)

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
        ORDER BY MAX("NM_VOTAVEL") ASC
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
    ano: int,
    uf: str,
    cd_cargo: str,
    nr_votavel: str,
    cd_municipio: Optional[str] = None,
) -> pd.DataFrame:
    """Inclui código IBGE (via JOIN com municipio_tse_ibge) para o coroplético."""
    where = [
        'b."ANO_ELEICAO" = :ano',
        'b."SG_UF" = :uf',
        'b."CD_CARGO_PERGUNTA" = :cargo',
        'b."NR_VOTAVEL" = :nr',
    ]
    params: dict = {"ano": str(ano), "uf": uf, "cargo": cd_cargo, "nr": nr_votavel}
    if cd_municipio:
        where.append('b."CD_MUNICIPIO" = :cd')
        params["cd"] = cd_municipio
    return run_df(
        f'''
        SELECT b."CD_MUNICIPIO" AS cd,
               MAX(b."NM_MUNICIPIO") AS nm,
               MAX(m.cd_municipio_ibge) AS cd_ibge,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        LEFT JOIN municipio_tse_ibge m
          ON m.sg_uf = b."SG_UF"
         AND m.cd_municipio_tse = b."CD_MUNICIPIO"
        WHERE {' AND '.join(where)}
        GROUP BY b."CD_MUNICIPIO"
        ORDER BY votos DESC
        ''',
        params,
    )


# ---------------------------------------------------------------------------
# Aba "Onde estão os votos no município" (mapa de bolhas)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def votos_candidato_por_local(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_votavel: str
) -> pd.DataFrame:
    return run_df(
        f'''
        SELECT lv."NM_LOCAL_VOTACAO" AS nm_local,
               lv."NM_BAIRRO" AS bairro,
               lv."NR_LATITUDE"::float AS lat,
               lv."NR_LONGITUDE"::float AS lng,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        {_LV_JOIN}
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
# Aba "Votos por local de votação"
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
        WHERE "SG_UF" = :uf
          AND "CD_MUNICIPIO" = :cd AND "NR_LOCAL_VOTACAO" = :lv
        ''',
        {"uf": uf, "cd": cd_municipio, "lv": nr_local},
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
# Aba "Votos por bairro" (via local_votacao)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def votos_por_bairro(
    ano: int, uf: str, cd_municipio: str, cd_cargo: str, nr_votavel: str
) -> pd.DataFrame:
    return run_df(
        f'''
        SELECT COALESCE(NULLIF(TRIM(lv."NM_BAIRRO"), ''), '(sem bairro)') AS bairro,
               MAX(b."NM_VOTAVEL") AS nm_votavel,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        {_LV_JOIN}
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
        f'''
        SELECT lv."NM_LOCAL_VOTACAO" AS local,
               MAX(b."NM_VOTAVEL") AS nm_votavel,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        {_LV_JOIN}
        WHERE b."ANO_ELEICAO" = :ano AND b."SG_UF" = :uf
          AND b."CD_MUNICIPIO" = :cd AND b."CD_CARGO_PERGUNTA" = :cargo
          AND b."NR_VOTAVEL" = :nr
        GROUP BY 1
        ORDER BY votos DESC
        ''',
        {"ano": str(ano), "uf": uf, "cd": cd_municipio, "cargo": cd_cargo, "nr": nr_votavel},
    )


# ---------------------------------------------------------------------------
# Aba "Comparativo de candidatos" (zona / bairro / seção)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def comparativo_votos_territorio(
    ano: int,
    uf: str,
    cd_municipio: str,
    cd_cargo: str,
    nrs: tuple[str, ...],
    dimensao: str,
) -> pd.DataFrame:
    """Votos dos candidatos selecionados por território + total geral do território.

    Retorna colunas: territorio, nr, nm, votos, total_territorio.
    `total_territorio` é a soma de votos nominais + legenda de TODOS os candidatos
    naquele território (descontando branco/nulo), pra calcular o percentual real
    do candidato no local — não só o percentual entre os selecionados.
    """
    if not nrs:
        return pd.DataFrame()

    # Configuração por dimensão: como derivar o "território" e quais
    # campos do boletim entram no GROUP BY.
    lv_join = _LV_JOIN.strip()
    if table_exists("local_votacao"):
        secao_local_select = (
            "MAX(COALESCE(NULLIF(TRIM(lv.\"NM_LOCAL_VOTACAO\"), ''), "
            "'Local ' || b.\"NR_LOCAL_VOTACAO\")) AS nm_local,"
        )
        secao_join = lv_join
        secao_group_extra = (
            'b."NR_ZONA", b."NR_SECAO", lv."NM_LOCAL_VOTACAO", b."NR_LOCAL_VOTACAO"'
        )
    else:
        secao_local_select = "MAX('Local ' || b.\"NR_LOCAL_VOTACAO\") AS nm_local,"
        secao_join = ""
        secao_group_extra = 'b."NR_ZONA", b."NR_SECAO", b."NR_LOCAL_VOTACAO"'

    dim_cfg = {
        "zona": {
            "territorio_expr": 'b."NR_ZONA"',
            "join_clause": "",
            "group_extra": 'b."NR_ZONA"',
            "local_select": "NULL::text AS nm_local,",
        },
        "secao": {
            "territorio_expr": 'b."NR_ZONA" || \' · Seção \' || b."NR_SECAO"',
            "join_clause": secao_join,
            "group_extra": secao_group_extra,
            "local_select": secao_local_select,
        },
        "bairro": {
            "territorio_expr": (
                'COALESCE(NULLIF(TRIM(lv."NM_BAIRRO"), \'\'), \'(sem bairro)\')'
            ),
            "join_clause": lv_join,
            "group_extra": 'lv."NM_BAIRRO"',
            "local_select": "NULL::text AS nm_local,",
        },
        "local": {
            "territorio_expr": (
                'COALESCE(NULLIF(TRIM(lv."NM_LOCAL_VOTACAO"), \'\'), '
                "'Local ' || b.\"NR_LOCAL_VOTACAO\")"
            ),
            "join_clause": lv_join,
            "group_extra": 'lv."NM_LOCAL_VOTACAO", b."NR_LOCAL_VOTACAO"',
            "local_select": "NULL::text AS nm_local,",
        },
    }
    if dimensao not in dim_cfg:
        raise ValueError(f"Dimensão inválida: {dimensao}")
    cfg = dim_cfg[dimensao]

    # WHERE comum a todas dimensões — exclui Branco/Nulo, mas NÃO filtra
    # NR_VOTAVEL (queremos o total de todos os candidatos no território).
    base_where = [
        'b."ANO_ELEICAO" = :ano',
        'b."SG_UF" = :uf',
        'b."CD_MUNICIPIO" = :cd',
        'b."CD_CARGO_PERGUNTA" = :cargo',
        """COALESCE(b."DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')""",
    ]
    params: dict = {
        "ano": str(ano),
        "uf": uf,
        "cd": cd_municipio,
        "cargo": cd_cargo,
        "nrs": list(nrs),
    }

    sql = f"""
        WITH all_votes AS (
            SELECT {cfg['territorio_expr']} AS territorio,
                   {cfg['local_select']}
                   b."NR_VOTAVEL" AS nr,
                   MAX(b."NM_VOTAVEL") AS nm,
                   SUM(b."QT_VOTOS"::bigint) AS votos
            FROM boletim_de_urna b
            {cfg['join_clause']}
            WHERE {' AND '.join(base_where)}
            GROUP BY {cfg['territorio_expr']}, {cfg['group_extra']}, b."NR_VOTAVEL"
        ),
        with_total AS (
            SELECT territorio, nm_local, nr, nm, votos,
                   SUM(votos) OVER (PARTITION BY territorio) AS total_territorio
            FROM all_votes
        )
        SELECT territorio, nm_local, nr, nm, votos, total_territorio
        FROM with_total
        WHERE nr = ANY(:nrs)
        ORDER BY territorio, votos DESC
    """

    return run_df(sql, params)
