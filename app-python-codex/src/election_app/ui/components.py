"""Componentes visuais compartilhados pelas nove abas."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from election_app.config import GEOJSON_DIR


UF_IBGE = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15", "AP": "16", "TO": "17",
    "MA": "21", "PI": "22", "CE": "23", "RN": "24", "PB": "25", "PE": "26", "AL": "27",
    "SE": "28", "BA": "29", "MG": "31", "ES": "32", "RJ": "33", "SP": "35", "PR": "41",
    "SC": "42", "RS": "43", "MS": "50", "MT": "51", "GO": "52", "DF": "53",
}


def fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def fmt_pct(value: Any) -> str:
    try:
        return f"{float(value or 0):.2f}%".replace(".", ",")
    except (TypeError, ValueError):
        return "0,00%"


def section_title(title: str, subtitle: str | None = None) -> None:
    safe_title = html.escape(title)
    safe_subtitle = html.escape(subtitle or "")
    st.markdown(
        f"""
        <div class="section-heading">
          <div class="section-bar"></div>
          <div>
            <h2>{safe_title}</h2>
            {f'<p>{safe_subtitle}</p>' if safe_subtitle else ''}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi(label: str, value: str, help_text: str = "", accent: bool = False) -> None:
    st.markdown(
        f"""
        <div class="metric-card {'metric-card-accent' if accent else ''}">
          <div class="metric-label">{html.escape(label)}</div>
          <div class="metric-value">{html.escape(value)}</div>
          <div class="metric-help">{html.escape(help_text)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def placeholder(title: str, message: str, source: str | None = None) -> None:
    source_html = f'<div class="placeholder-source">Fonte necessaria: {html.escape(source)}</div>' if source else ""
    st.markdown(
        f"""
        <div class="data-placeholder">
          <strong>{html.escape(title)}</strong>
          <p>{html.escape(message)}</p>
          {source_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def candidate_table(df: pd.DataFrame, focus_number: str | None = None) -> None:
    if df.empty:
        st.info("Nenhum candidato encontrado para este recorte.")
        return
    for position, row in enumerate(df.itertuples(index=False), start=1):
        number = str(getattr(row, "number"))
        focused = number == str(focus_number)
        st.markdown(
            f"""
            <div class="candidate-row {'candidate-focus' if focused else ''}">
              <div class="candidate-rank">{position}o</div>
              <div class="candidate-avatar">{html.escape(str(getattr(row, 'name', '?'))[:1])}</div>
              <div class="candidate-name">
                <strong>{html.escape(str(getattr(row, 'name', 'Nao informado')))}</strong>
                <span>{html.escape(str(getattr(row, 'party', '') or '-'))}</span>
              </div>
              <div class="candidate-votes">{fmt_int(getattr(row, 'votes', 0))}</div>
              <div class="candidate-pct">{fmt_pct(getattr(row, 'percentage', 0))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def state_choropleth(df: pd.DataFrame, uf: str) -> bool:
    state_code = UF_IBGE.get(uf)
    path = GEOJSON_DIR / f"geojs-{state_code}-mun.json" if state_code else Path()
    if not state_code or not path.exists() or df.empty or df["ibge_code"].isna().all():
        return False
    geojson = json.loads(path.read_text(encoding="utf-8"))
    plot_df = df.dropna(subset=["ibge_code"]).copy()
    plot_df["ibge_code"] = plot_df["ibge_code"].astype(str)
    fig = px.choropleth_map(
        plot_df,
        geojson=geojson,
        locations="ibge_code",
        featureidkey="properties.id",
        color="votes",
        hover_name="municipality",
        hover_data={"votes": ":,.0f", "ibge_code": False},
        color_continuous_scale=["#dbeafe", "#0b73c9", "#064b86"],
        map_style="carto-positron",
        opacity=0.78,
        zoom=5,
        height=610,
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, coloraxis_colorbar_title="Votos")
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    return True


def location_map(df: pd.DataFrame) -> bool:
    points = df.dropna(subset=["latitude", "longitude"]).copy()
    if points.empty:
        return False
    fig = px.scatter_map(
        points,
        lat="latitude",
        lon="longitude",
        size="votes",
        color="votes",
        hover_name="location",
        hover_data={"neighborhood": True, "votes": ":,.0f", "latitude": False, "longitude": False},
        color_continuous_scale=["#91c9ef", "#0b73c9", "#063b69"],
        size_max=42,
        zoom=10,
        map_style="carto-positron",
        height=610,
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    return True


def inject_css() -> None:
    st.markdown(
        """
        <style>
          :root { --navy:#10243e; --blue:#0b73c9; --muted:#64748b; --line:#dce5ef; --soft:#f3f7fb; }
          .stApp { background:#f7f9fc; }
          .block-container { max-width:1500px; padding-top:1.2rem; padding-bottom:3rem; }
          .app-header { display:flex; align-items:center; gap:1rem; padding:.35rem 0 1rem; }
          .app-header img { width:122px; }
          .app-header h1 { color:var(--navy); font-size:1.75rem; margin:0; }
          .app-header p { color:var(--muted); margin:.15rem 0 0; }
          .filter-panel { background:white; border:1px solid var(--line); border-radius:14px; padding:1rem; margin-bottom:1rem; }
          .scope-strip { background:#eaf4fc; color:#14324e; border-left:4px solid var(--blue); padding:.65rem .9rem; border-radius:6px; margin:.2rem 0 1rem; }
          .section-heading { display:flex; gap:.8rem; align-items:flex-start; margin:.2rem 0 1.2rem; }
          .section-bar { width:6px; min-height:54px; background:var(--blue); border-radius:6px; }
          .section-heading h2 { color:var(--navy); font-size:1.7rem; line-height:1.15; margin:0; }
          .section-heading p { color:#456078; margin:.25rem 0 0; }
          .metric-card { min-height:145px; background:#eef3f7; border:1px solid #e3eaf1; border-radius:10px; padding:1.2rem; }
          .metric-card-accent { background:#eaf4fc; border-color:#cce5f7; }
          .metric-label { color:#29455f; font-weight:700; }
          .metric-value { color:var(--navy); font-size:2.2rem; font-weight:750; margin:.35rem 0; }
          .metric-card-accent .metric-value { color:var(--blue); }
          .metric-help { color:#51677b; min-height:1.2rem; }
          .candidate-row { display:grid; grid-template-columns:48px 42px minmax(220px,1fr) 110px 85px; gap:.55rem; align-items:center; padding:.65rem .7rem; background:white; border-bottom:1px solid var(--line); }
          .candidate-focus { background:#eaf4fc; border-left:4px solid var(--blue); }
          .candidate-rank { color:var(--blue); font-weight:750; }
          .candidate-avatar { width:34px; height:34px; border-radius:50%; background:#dce9f4; color:var(--navy); display:grid; place-items:center; font-weight:800; }
          .candidate-name { display:flex; flex-direction:column; color:var(--navy); }
          .candidate-name span { color:var(--muted); font-size:.83rem; }
          .candidate-votes,.candidate-pct { text-align:right; color:var(--navy); font-weight:650; }
          .data-placeholder { border:1px dashed #8eb8d6; border-radius:10px; background:#f0f8fd; padding:1rem 1.1rem; color:#23445f; }
          .data-placeholder p { margin:.35rem 0; }
          .placeholder-source { color:#496b84; font-size:.88rem; }
          .dark-table { background:#172334; color:white; padding:1rem; border-radius:8px; }
          .dark-table table { color:white !important; }
          div[data-testid="stTabs"] [data-baseweb="tab-list"] {
            display:flex;
            flex-wrap:nowrap;
            gap:.35rem;
            background:#f0f3f7;
            border-bottom:1px solid #dce3eb;
            padding:.45rem .45rem 0;
            overflow-x:auto;
            -webkit-overflow-scrolling:touch;
          }
          div[data-testid="stTabs"] [data-baseweb="tab"] {
            background:white;
            border:1px solid #e2e8f0;
            border-bottom:none;
            border-radius:8px 8px 0 0;
            height:auto;
            margin-bottom:-1px;
          }
          div[data-testid="stTabs"] [data-baseweb="tab"] button {
            background:transparent;
            border:none;
            color:#4a5c6e;
            font-weight:500;
            font-size:.92rem;
            padding:.65rem .9rem;
            white-space:nowrap;
          }
          div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {
            border-bottom:2px solid var(--blue);
            z-index:1;
          }
          div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] button {
            color:var(--blue);
            font-weight:650;
          }
          div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
            background-color:transparent !important;
          }
          @media (max-width: 768px) {
            div[data-testid="stTabs"] [data-baseweb="tab-list"] {
              flex-wrap:wrap;
              overflow-x:visible;
              gap:.4rem;
              padding:.45rem;
              border-bottom:none;
            }
            div[data-testid="stTabs"] [data-baseweb="tab"] {
              border-radius:8px;
              border-bottom:1px solid #e2e8f0;
              margin-bottom:0;
              flex:0 1 auto;
            }
            div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {
              border-bottom:2px solid var(--blue);
            }
            div[data-testid="stTabs"] [data-baseweb="tab"] button {
              white-space:normal;
              line-height:1.25;
            }
          }
          @media (max-width: 800px) {
            .candidate-row { grid-template-columns:35px 35px 1fr 75px; }
            .candidate-pct { display:none; }
            .section-heading h2 { font-size:1.35rem; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
