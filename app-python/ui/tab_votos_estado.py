"""Aba 'Onde estão os votos no estado'.

Coroplético via Folium + GeoJSON local de `external/geodata-br` (CC0).
Match feito por código IBGE (7 dígitos), obtido via JOIN com a tabela
`municipio_tse_ibge` no banco.

Labels: `folium.Marker(DivIcon)` desenhando uma `<div>` branca retangular
com borda azul fina, posicionada no centróide aproximado do polígono.
Sempre visível, igual à imagem de referência.
"""
from __future__ import annotations

import json
from pathlib import Path

import folium
import streamlit as st
from folium.features import DivIcon
from streamlit_folium import st_folium

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


def _uf_center(uf: str) -> tuple[float, float]:
    centros = {
        "AC": (-9.0, -70.0), "AL": (-9.5, -36.6), "AM": (-4.0, -64.0),
        "AP": (1.0, -52.0), "BA": (-12.5, -41.7), "CE": (-5.5, -39.5),
        "DF": (-15.8, -47.9), "ES": (-19.5, -40.6), "GO": (-15.9, -49.5),
        "MA": (-5.4, -45.0), "MG": (-18.5, -44.5), "MS": (-20.5, -54.5),
        "MT": (-12.6, -55.5), "PA": (-4.5, -52.5), "PB": (-7.1, -36.7),
        "PE": (-8.5, -37.5), "PI": (-7.5, -42.5), "PR": (-24.7, -51.7),
        "RJ": (-22.3, -42.7), "RN": (-5.8, -36.6), "RO": (-11.0, -62.6),
        "RR": (2.2, -61.0), "RS": (-29.7, -53.3), "SC": (-27.2, -50.5),
        "SE": (-10.6, -37.5), "SP": (-22.3, -48.8), "TO": (-10.2, -48.3),
    }
    return centros.get(uf, (-15.0, -50.0))


def _label_html(texto: str, font_size: int) -> str:
    """Caixa branca retangular com borda azul. CSS inline para evitar shadow root."""
    return (
        f'<div style="background:#ffffff;border:1px solid #1f6feb;'
        f'color:#0b2545;font-family:Arial,sans-serif;font-weight:700;'
        f'font-size:{font_size}px;line-height:1;padding:2px 5px;'
        f'border-radius:2px;white-space:nowrap;text-align:center;'
        f'box-shadow:0 1px 2px rgba(11,37,69,0.15);'
        f'transform:translate(-50%,-50%)">{texto}</div>'
    )


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
            "Clone via `git clone https://github.com/tbrugz/geodata-br "
            "app-python/external/geodata-br`."
        )
        return

    n_munic = len(df.dropna(subset=["cd_ibge"]))
    c_a, c_b, _ = st.columns([1.3, 1, 3.7])
    so_top = c_a.toggle(
        "Mostrar só top 50",
        value=n_munic > 80,
        help="Rotula apenas os 50 municípios mais votados (recomendado em UFs grandes).",
    )
    font_size = c_b.slider("Tamanho do rótulo", 1, 14, 11)

    col_map, col_lista = st.columns([2, 1])

    with col_map:
        df_map = df.dropna(subset=["cd_ibge"]).copy()
        df_map["cd_ibge"] = df_map["cd_ibge"].astype(str).str[:7]
        df_map["votos"] = df_map["votos"].astype(int)

        m = folium.Map(
            location=_uf_center(ctx["uf"]),
            zoom_start=6,
            tiles="cartodbpositron",
            control_scale=False,
        )

        # Coroplético — colore por votos. Match por feature.properties.id (IBGE 7d).
        folium.Choropleth(
            geo_data=geo,
            data=df_map,
            columns=["cd_ibge", "votos"],
            key_on="feature.properties.id",
            fill_color="Blues",
            fill_opacity=0.85,
            line_opacity=0.4,
            nan_fill_color="#ffffff",
            legend_name="Votos do candidato",
        ).add_to(m)

        # Hover por município (tooltip).
        votos_por_cd = dict(zip(df_map["cd_ibge"], df_map["votos"]))
        nome_por_cd = dict(zip(df_map["cd_ibge"], df_map["nm"]))

        def _tooltip(feat):
            cd = str(feat["properties"].get("id", ""))[:7]
            nm = nome_por_cd.get(cd, feat["properties"].get("name", ""))
            v = votos_por_cd.get(cd, 0)
            return f"{nm}: {fmt_int(v)}"

        folium.GeoJson(
            geo,
            style_function=lambda x: {"fillOpacity": 0, "color": "#0b2545", "weight": 0.3},
            tooltip=folium.GeoJsonTooltip(
                fields=["name"],
                aliases=["Município"],
                sticky=True,
            ),
        ).add_to(m)

        # Labels — divs brancas quadradas com bordas, sempre visíveis.
        centroides = _centroides(ctx["uf"])
        df_lbl = df_map.copy()
        if so_top:
            df_lbl = df_lbl.head(50)

        # Estima largura/altura do DivIcon pelo nº de caracteres (evita corte do texto).
        for r in df_lbl.itertuples(index=False):
            c = centroides.get(r.cd_ibge)
            if not c:
                continue
            txt = fmt_int(r.votos)
            w = max(24, len(txt) * (font_size - 2) + 14)
            h = font_size + 8
            folium.Marker(
                location=list(c),
                icon=DivIcon(
                    icon_size=(w, h),
                    icon_anchor=(w // 2, h // 2),
                    html=_label_html(txt, font_size),
                ),
            ).add_to(m)

        st_folium(
            m,
            height=1000,
            use_container_width=True,
            returned_objects=[],
            key=f"mapa_{ctx['uf']}_{ctx['ano']}_{ctx['nr_votavel']}_{so_top}_{font_size}",
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
