from __future__ import annotations

import streamlit as st

from components import fmt_int, section_title
from db import table_exists
from queries import votos_candidato_por_local


def render(ctx: dict) -> None:
    if not ctx["cd_municipio"]:
        st.info("Selecione um município para ver o mapa por local.")
        return

    section_title(
        "Onde estão os votos no município",
        f"Votação de {ctx['nm_candidato']} por local — {ctx['nm_municipio']}",
    )

    if not table_exists("local_votacao"):
        st.info("Dados não encontrados.")
        return

    df = votos_candidato_por_local(
        ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], ctx["nr_votavel"]
    )
    df = df.dropna(subset=["lat", "lng"])
    if df.empty:
        st.info("Sem coordenadas válidas para os locais deste município.")
        return

    import folium
    from streamlit_folium import st_folium

    centro = [df["lat"].mean(), df["lng"].mean()]
    m = folium.Map(location=centro, zoom_start=11, tiles="cartodbpositron")
    max_v = float(df["votos"].max() or 1)

    col_map, col_lista = st.columns([2, 1])
    with col_map:
        for r in df.itertuples(index=False):
            raio = 4 + (float(r.votos) / max_v) * 22
            folium.CircleMarker(
                location=[r.lat, r.lng],
                radius=raio,
                color="#1f6feb",
                fill=True,
                fill_color="#1f6feb",
                fill_opacity=0.55,
                tooltip=f"{r.nm_local} — {fmt_int(r.votos)} votos",
            ).add_to(m)
        st_folium(m, height=520, use_container_width=True, returned_objects=[])
        st.caption("Cada bolha é um local de votação (posição por GPS, tamanho conforme os votos).")

    with col_lista:
        st.markdown("#### Top locais (votos do candidato)")
        for i, r in enumerate(df.itertuples(index=False), start=1):
            st.markdown(
                f"<div style='display:flex;justify-content:space-between;"
                f"padding:0.25rem 0;border-bottom:1px solid #eef1f5'>"
                f"<div><span style='color:#5b6b80'>{i}.</span> {r.nm_local}</div>"
                f"<div style='font-weight:700'>{fmt_int(r.votos)}</div></div>",
                unsafe_allow_html=True,
            )
