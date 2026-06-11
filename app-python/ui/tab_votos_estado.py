"""Aba 'Onde estão os votos no estado'.

Coroplético estático via Plotly + GeoJSON local de `external/geodata-br` (CC0).
Match feito por código IBGE (7 dígitos), obtido via JOIN com a tabela
`municipio_tse_ibge` no banco.

Sem tiles, zoom ou pan — apenas o vetor colorido na tela.
Labels: caixa branca com borda azul no centróide aproximado do polígono.
"""
from __future__ import annotations

import json
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

from components import fmt_int, section_title
from queries import votos_candidato_por_municipio

GEODATA_DIR = Path(__file__).resolve().parent.parent / "external" / "geodata-br" / "geojson"

UF_CODIGO_IBGE = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15", "AP": "16", "TO": "17",
    "MA": "21", "PI": "22", "CE": "23", "RN": "24", "PB": "25", "PE": "26", "AL": "27",
    "SE": "28", "BA": "29",
    "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
    "PR": "41", "SC": "42", "RS": "43",
    "MS": "50", "MT": "51", "GO": "52", "DF": "53",
}

_PLOTLY_STATIC = {"staticPlot": True, "displayModeBar": False}


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


@st.cache_data(ttl=86400, show_spinner=False)
def _centroides(uf: str) -> dict[str, tuple[float, float]]:
    geo = _load_geojson(uf)
    if not geo:
        return {}
    out: dict[str, tuple[float, float]] = {}
    for feat in geo.get("features", []):
        cd = str(feat.get("properties", {}).get("id", ""))[:7]
        if not cd:
            continue
        geom = feat.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates") or []
        if gtype == "Polygon" and coords:
            ring = coords[0]
        elif gtype == "MultiPolygon" and coords:
            ring = max(coords, key=lambda poly: len(poly[0]))[0]
        else:
            continue
        if not ring:
            continue
        n = len(ring)
        lon = sum(p[0] for p in ring) / n
        lat = sum(p[1] for p in ring) / n
        out[cd] = (lat, lon)
    return out


def _filter_geo_municipios(geo: dict, cd_ibge: set[str]) -> dict:
    if not cd_ibge:
        return geo
    features = [
        feat
        for feat in geo.get("features", [])
        if str(feat.get("properties", {}).get("id", ""))[:7] in cd_ibge
    ]
    return {**geo, "features": features}


def _build_coropletico(
    geo: dict,
    df_map,
    df_lbl,
    centroides: dict[str, tuple[float, float]],
    font_size: int,
) -> go.Figure:
    votos_por_cd = dict(zip(df_map["cd_ibge"], df_map["votos"]))
    nome_por_cd = dict(zip(df_map["cd_ibge"], df_map["nm"]))

    locations: list[str] = []
    votos: list[int] = []
    nomes: list[str] = []
    for feat in geo.get("features", []):
        cd = str(feat.get("properties", {}).get("id", ""))[:7]
        if not cd:
            continue
        locations.append(cd)
        votos.append(int(votos_por_cd.get(cd, 0)))
        nomes.append(nome_por_cd.get(cd, feat.get("properties", {}).get("name", "")))

    fig = go.Figure(
        go.Choropleth(
            geojson=geo,
            locations=locations,
            z=votos,
            featureidkey="properties.id",
            colorscale="Blues",
            zmin=0,
            marker_line_width=0.4,
            marker_line_color="#0b2545",
            colorbar_title="Votos",
            hovertemplate="%{customdata}: %{z:,}<extra></extra>",
            customdata=nomes,
        )
    )

    lbl_lats: list[float] = []
    lbl_lons: list[float] = []
    lbl_texts: list[str] = []
    lbl_sizes: list[int] = []
    for r in df_lbl.itertuples(index=False):
        c = centroides.get(r.cd_ibge)
        if not c:
            continue
        txt = fmt_int(r.votos)
        lbl_lats.append(c[0])
        lbl_lons.append(c[1])
        lbl_texts.append(txt)
        lbl_sizes.append(max(24, len(txt) * (font_size - 2) + 14))

    if lbl_texts:
        fig.add_trace(
            go.Scattergeo(
                lon=lbl_lons,
                lat=lbl_lats,
                text=lbl_texts,
                mode="markers+text",
                marker=dict(
                    size=lbl_sizes,
                    color="#ffffff",
                    line=dict(width=1, color="#1f6feb"),
                    symbol="square",
                ),
                textfont=dict(size=font_size, color="#0b2545", family="Arial Black"),
                hoverinfo="skip",
            )
        )

    fig.update_geos(fitbounds="locations", visible=False, projection_type="mercator")
    fig.update_layout(
        height=1000,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="#ffffff",
        dragmode=False,
    )
    return fig


def render(ctx: dict) -> None:
    cd_municipio = ctx.get("cd_municipio")
    nm_municipio = ctx.get("nm_municipio")
    if nm_municipio:
        subtitle = f"Votação de {ctx['nm_candidato']} — {nm_municipio}/{ctx['uf']}"
    else:
        subtitle = f"Votação de {ctx['nm_candidato']} por município — {ctx['uf']}"

    section_title("Onde estão os votos no estado", subtitle)

    df = votos_candidato_por_municipio(
        ctx["ano"],
        ctx["uf"],
        ctx["cd_cargo"],
        ctx["nr_votavel"],
        cd_municipio,
    )
    if df.empty:
        st.info("Sem votos do candidato selecionado nesta UF.")
        return

    geo = _load_geojson(ctx["uf"])
    if geo is None:
        st.warning(
            f"GeoJSON do estado **{ctx['uf']}** não encontrado em `external/geodata-br/geojson/`. "
            "Clone via `git clone https://github.com/tbrugz/geodata-br "
            "app-python/external/geodata-br`."
        )
        return

    df_map = df.dropna(subset=["cd_ibge"]).copy()
    df_map["cd_ibge"] = df_map["cd_ibge"].astype(str).str[:7]
    df_map["votos"] = df_map["votos"].astype(int)

    n_munic = len(df_map)
    c_a, c_b, _ = st.columns([1.3, 1, 3.7])
    so_top = False
    if cd_municipio:
        c_a.caption("Município selecionado nos filtros.")
    else:
        so_top = c_a.toggle(
            "Mostrar só top 50",
            value=n_munic > 80,
            help="Rotula apenas os 50 municípios mais votados (recomendado em UFs grandes).",
        )
    font_size = c_b.slider("Tamanho do rótulo", 1, 14, 11)

    col_map, col_lista = st.columns([2, 1])

    with col_map:
        if cd_municipio:
            geo = _filter_geo_municipios(geo, set(df_map["cd_ibge"]))

        df_lbl = df_map.copy()
        if so_top:
            df_lbl = df_lbl.head(50)

        fig = _build_coropletico(
            geo,
            df_map,
            df_lbl,
            _centroides(ctx["uf"]),
            font_size,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config=_PLOTLY_STATIC,
            key=(
                f"mapa_{ctx['uf']}_{ctx['ano']}_{ctx['nr_votavel']}_"
                f"{cd_municipio or 'uf'}_{so_top}_{font_size}"
            ),
        )

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
