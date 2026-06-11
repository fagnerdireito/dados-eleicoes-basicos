from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from components import fmt_int, section_title
from queries import votos_candidato_por_municipio

GEOJSON_DIR = Path(__file__).resolve().parent.parent / "assets" / "geojson_cache"


@st.cache_data(ttl=86400, show_spinner=False)
def _load_uf_municipios_geojson(uf: str):
    GEOJSON_DIR.mkdir(parents=True, exist_ok=True)
    cache = GEOJSON_DIR / f"{uf}_municipios.parquet"
    if cache.exists():
        return pd.read_parquet(cache)
    try:
        import geobr  # noqa
    except ImportError:
        return None
    try:
        gdf = geobr.read_municipality(code_muni=uf, year=2022, simplified=True)
    except Exception:
        return None
    df = pd.DataFrame({
        "code_muni": gdf["code_muni"].astype(str).str[:6],  # IBGE -> 6 dígitos (compat TSE)
        "name_muni": gdf["name_muni"],
        "geometry_wkt": gdf["geometry"].to_wkt(),
    })
    df.to_parquet(cache, index=False)
    return df


def _build_choropleth(df_votos, gdf_wkt, uf):
    """Renderiza mapa coroplético da UF colorindo municípios pelo total de votos."""
    import folium
    from streamlit_folium import st_folium
    from shapely import wkt

    feats = []
    cd_map = {row.code_muni[:5]: row for row in gdf_wkt.itertuples(index=False)}
    for _, vrow in df_votos.iterrows():
        cd = str(vrow["cd"])[:5]
        g = cd_map.get(cd)
        if g is None:
            continue
        geom = wkt.loads(g.geometry_wkt)
        feats.append({
            "type": "Feature",
            "properties": {"name": g.name_muni, "votos": int(vrow["votos"])},
            "geometry": geom.__geo_interface__,
        })
    geo = {"type": "FeatureCollection", "features": feats}

    if not feats:
        st.info("Sem geometria correspondente para os municípios da UF.")
        return

    # centro aproximado da UF — média dos centróides
    centro = [
        sum(f["geometry"]["coordinates"][0][0][1] for f in feats[:50]) / min(50, len(feats)),
        sum(f["geometry"]["coordinates"][0][0][0] for f in feats[:50]) / min(50, len(feats)),
    ]
    m = folium.Map(location=centro, zoom_start=6, tiles="cartodbpositron")
    folium.Choropleth(
        geo_data=geo,
        data=df_votos.assign(cd5=df_votos["cd"].astype(str).str[:5]),
        columns=["cd5", "votos"],
        key_on="feature.properties.name",  # fallback simples
        fill_color="Blues",
        fill_opacity=0.85,
        line_opacity=0.4,
        nan_fill_color="white",
        legend_name="Votos do candidato",
    ).add_to(m)

    folium.GeoJson(
        geo,
        tooltip=folium.GeoJsonTooltip(fields=["name", "votos"], aliases=["Município", "Votos"]),
        style_function=lambda x: {"color": "#1f6feb", "weight": 0.5, "fillOpacity": 0},
    ).add_to(m)

    st_folium(m, height=520, use_container_width=True, returned_objects=[])


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

    col_map, col_lista = st.columns([2, 1])

    with col_map:
        gdf = _load_uf_municipios_geojson(ctx["uf"])
        if gdf is None or gdf.empty:
            st.warning(
                "Não foi possível carregar o shapefile via `geobr`. "
                "Verifique a instalação ou a conexão de rede."
            )
        else:
            _build_choropleth(df, gdf, ctx["uf"])

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
