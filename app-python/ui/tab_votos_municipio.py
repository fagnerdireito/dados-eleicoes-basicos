from __future__ import annotations

import streamlit as st

from components import fmt_int, section_title
from db import table_exists
from queries import votos_candidato_por_local


_PRINT_ZOOM_CSS = """
<style>
  @media print {
    @page {
      size: A4 portrait;
      margin: 0.35cm;
    }

    # .main .block-container {
    #   padding-left: 0.2rem !important;
    #   padding-right: 0.2rem !important;
    #   max-width: 100% !important;
    # }

    .st-key-votos-municipio-print
      [data-testid="stHorizontalBlock"]:has(iframe) {
      display: block !important;
      max-width: 100% !important;
      width: 100% !important;
      gap: 0.15rem !important;
      overflow: visible !important;
    }

    .st-key-votos-municipio-print
      [data-testid="stHorizontalBlock"]:has(iframe)
      > [data-testid="stColumn"] {
      max-width: 100% !important;
      width: 100% !important;
      min-width: 0 !important;
      overflow: visible !important;
    }

    .st-key-votos-municipio-print iframe,
    .st-key-votos-municipio-print [data-testid="stIFrame"] {
      max-width: 100% !important;
      width: 100% !important;
      box-sizing: border-box !important;
    }

    .st-key-votos-municipio-print .element-container {
      max-width: 100% !important;
    }
  }
</style>
"""


def render(ctx: dict) -> None:
    st.markdown(_PRINT_ZOOM_CSS, unsafe_allow_html=True)
    with st.container(key="votos-municipio-print"):
        _render(ctx)


def _render(ctx: dict) -> None:
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

    lat_min, lat_max = float(df["lat"].min()), float(df["lat"].max())
    lng_min, lng_max = float(df["lng"].min()), float(df["lng"].max())
    # Margem mínima para o caso de um único ponto (evita zoom máximo).
    if lat_min == lat_max and lng_min == lng_max:
        lat_min, lat_max = lat_min - 0.02, lat_max + 0.02
        lng_min, lng_max = lng_min - 0.02, lng_max + 0.02

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

        # Enquadra todas as bolhas em vez de centro+zoom fixos.
        m.fit_bounds([[lat_min, lng_min], [lat_max, lng_max]], padding=(25, 25))

        # Ao imprimir, o container do mapa muda de tamanho/proporção; sem
        # invalidateSize o Leaflet mantém o offset antigo e mostra outra área.
        reflow_js = f"""
        <script>
        (function() {{
          function reflow() {{
            try {{
              var map = {m.get_name()};
              map.invalidateSize(true);
              map.fitBounds(
                [[{lat_min}, {lng_min}], [{lat_max}, {lng_max}]],
                {{padding: [25, 25]}}
              );
            }} catch (e) {{}}
          }}
          window.addEventListener('beforeprint', function() {{ setTimeout(reflow, 50); }});
          window.addEventListener('afterprint', function() {{ setTimeout(reflow, 50); }});
          if (window.matchMedia) {{
            window.matchMedia('print').addEventListener('change', reflow);
          }}
        }})();
        </script>
        """
        m.get_root().html.add_child(folium.Element(reflow_js))

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
