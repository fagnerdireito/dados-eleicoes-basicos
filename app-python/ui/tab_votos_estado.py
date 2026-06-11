"""Aba 'Onde estão os votos no estado'.

Coroplético via Plotly + GeoJSON local de `external/geodata-br` (CC0).
Match feito por código IBGE (7 dígitos), obtido via JOIN com a tabela
`municipio_tse_ibge` no banco.
"""
from __future__ import annotations

import json
from pathlib import Path

import plotly.express as px
import streamlit as st

from components import fmt_int, section_title
from queries import votos_candidato_por_municipio

GEODATA_DIR = Path(__file__).resolve().parent.parent / "external" / "geodata-br" / "geojson"

# código IBGE de UF → 2 primeiros dígitos do código IBGE municipal
UF_CODIGO_IBGE = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15", "AP": "16", "TO": "17",
    "MA": "21", "PI": "22", "CE": "23", "RN": "24", "PB": "25", "PE": "26", "AL": "27",
    "SE": "28", "BA": "29",
    "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
    "PR": "41", "SC": "42", "RS": "43",
    "MS": "50", "MT": "51", "GO": "52", "DF": "53",
}


@st.cache_data(ttl=86400, show_spinner=False)
def _load_geojson(uf: str) -> dict | None:
    cd = UF_CODIGO_IBGE.get(uf)
    if not cd:
        return None
    path = GEODATA_DIR / f"geojs-{cd}-mun.json"
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def render(ctx: dict) -> None:
    section_title(
        "Onde estão os votos no estado",
        f"Votação de {ctx['nm_candidato']} por município — {ctx['uf']}",
    )

    df = votos_candidato_por_municipio(
        ctx["ano"], ctx["uf"], ctx["cd_cargo"], ctx["nr_votavel"]
    )
    if df.empty:
        st.info("Sem votos do candidato selecionado nesta UF.")
        return

    geo = _load_geojson(ctx["uf"])
    if geo is None:
        st.warning(
            f"GeoJSON do estado **{ctx['uf']}** não encontrado em `external/geodata-br/geojson/`. "
            "Confirme que o repositório foi clonado com `git clone https://github.com/tbrugz/geodata-br "
            "app-python/external/geodata-br`."
        )
        return

    col_map, col_lista = st.columns([2, 1])

    with col_map:
        df_map = df.dropna(subset=["cd_ibge"]).copy()
        df_map["cd_ibge"] = df_map["cd_ibge"].astype(str).str[:7]
        df_map["votos"] = df_map["votos"].astype(int)

        fig = px.choropleth_map(
            df_map,
            geojson=geo,
            locations="cd_ibge",
            featureidkey="properties.id",
            color="votos",
            hover_name="nm",
            hover_data={"votos": ":,", "cd_ibge": False},
            color_continuous_scale="Blues",
            map_style="carto-positron",
            zoom=5,
            center=_uf_center(ctx["uf"]),
            opacity=0.85,
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=520,
            coloraxis_colorbar=dict(title="Votos"),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        nao_casados = df["cd_ibge"].isna().sum()
        if nao_casados:
            st.caption(
                f"⚠️ {nao_casados} município(s) sem código IBGE — verifique `municipio_tse_ibge`."
            )

    with col_lista:
        st.markdown("#### Top municípios (votos do candidato)")
        for i, r in enumerate(df.head(30).itertuples(index=False), start=1):
            st.markdown(
                f"<div style='display:flex;justify-content:space-between;"
                f"padding:0.25rem 0;border-bottom:1px solid #eef1f5'>"
                f"<div><span style='color:#5b6b80'>{i}.</span> {r.nm}</div>"
                f"<div style='font-weight:700'>{fmt_int(r.votos)}</div></div>",
                unsafe_allow_html=True,
            )


def _uf_center(uf: str) -> dict:
    centros = {
        "AC": {"lat": -9.0,  "lon": -70.0},
        "AL": {"lat": -9.5,  "lon": -36.6},
        "AM": {"lat": -4.0,  "lon": -64.0},
        "AP": {"lat":  1.0,  "lon": -52.0},
        "BA": {"lat": -12.5, "lon": -41.7},
        "CE": {"lat": -5.5,  "lon": -39.5},
        "DF": {"lat": -15.8, "lon": -47.9},
        "ES": {"lat": -19.5, "lon": -40.6},
        "GO": {"lat": -15.9, "lon": -49.5},
        "MA": {"lat": -5.4,  "lon": -45.0},
        "MG": {"lat": -18.5, "lon": -44.5},
        "MS": {"lat": -20.5, "lon": -54.5},
        "MT": {"lat": -12.6, "lon": -55.5},
        "PA": {"lat": -4.5,  "lon": -52.5},
        "PB": {"lat": -7.1,  "lon": -36.7},
        "PE": {"lat": -8.5,  "lon": -37.5},
        "PI": {"lat": -7.5,  "lon": -42.5},
        "PR": {"lat": -24.7, "lon": -51.7},
        "RJ": {"lat": -22.3, "lon": -42.7},
        "RN": {"lat": -5.8,  "lon": -36.6},
        "RO": {"lat": -11.0, "lon": -62.6},
        "RR": {"lat":  2.2,  "lon": -61.0},
        "RS": {"lat": -29.7, "lon": -53.3},
        "SC": {"lat": -27.2, "lon": -50.5},
        "SE": {"lat": -10.6, "lon": -37.5},
        "SP": {"lat": -22.3, "lon": -48.8},
        "TO": {"lat": -10.2, "lon": -48.3},
    }
    return centros.get(uf, {"lat": -15.0, "lon": -50.0})
