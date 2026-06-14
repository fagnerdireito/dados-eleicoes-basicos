"""Aba 'Perfil do eleitorado (UF)'.

Layout inspirado em `app-python-codex/.../dashboard.render_state_profile`:

  [ Comparecimento  ] |  Eleitorado por faixa etária  |  Eleitorado por escolaridade
  [ Abstenção       ] |  (barras horizontais)         |  (barras horizontais)

Comparecimento/abstenção vêm de `boletim_de_urna` agregando QT_APTOS,
QT_COMPARECIMENTO e QT_ABSTENCOES por seção. Faixa etária e escolaridade
vêm da tabela `perfil_eleitorado` (importada pelo `10_import_perfil_eleitorado.go`).
"""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from components import fmt_int, fmt_pct, kpi, section_title
from db import table_exists
from queries import perfil_escolaridade, perfil_faixa_etaria, turnout_uf


# Só na impressão: escala para A4 e força 3 colunas lado a lado (grid),
# sem alterar o layout em tela.
_PRINT_ZOOM_CSS = """
<style>
  @media print {
    @page {
      size: A4 portrait;
      margin: 0.35cm;
    }

    .main .block-container {
      padding-left: 0.2rem !important;
      padding-right: 0.2rem !important;
      max-width: 100% !important;
    }

    .st-key-perfil-eleitorado-print {
      zoom: 0.42 !important;
      width: 100% !important;
      max-width: 100% !important;
      margin: 0 !important;
      box-sizing: border-box !important;
      overflow: visible !important;
    }

    /* Linha com cards + dois gráficos — grid evita quebra em A4 */
    .st-key-perfil-eleitorado-print
      [data-testid="stHorizontalBlock"]:has([data-testid="stPlotlyChart"]) {
      display: grid !important;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1.5fr) minmax(0, 1.5fr) !important;
      max-width: 100% !important;
      width: 100% !important;
      gap: 0.15rem !important;
      overflow: visible !important;
    }

    .st-key-perfil-eleitorado-print
      [data-testid="stHorizontalBlock"]:has([data-testid="stPlotlyChart"])
      > [data-testid="stColumn"] {
      max-width: 100% !important;
      width: 100% !important;
      min-width: 0 !important;
      overflow: visible !important;
    }

    /* Cards de comparecimento / abstenção */
    .st-key-perfil-eleitorado-print [data-testid="stVerticalBlockBorderWrapper"] {
      max-width: 100% !important;
      width: 100% !important;
      box-sizing: border-box !important;
    }

    /* Gráficos Plotly */
    .st-key-perfil-eleitorado-print [data-testid="stPlotlyChart"],
    .st-key-perfil-eleitorado-print [data-testid="stPlotlyChart"] > div,
    .st-key-perfil-eleitorado-print [data-testid="stPlotlyChart"] iframe {
      max-width: 100% !important;
      width: 100% !important;
      box-sizing: border-box !important;
    }

    .st-key-perfil-eleitorado-print .element-container {
      max-width: 100% !important;
    }
  }
</style>
"""


def render(ctx: dict) -> None:
    st.markdown(_PRINT_ZOOM_CSS, unsafe_allow_html=True)
    with st.container(key="perfil-eleitorado-print"):
        _render(ctx)


def _render(ctx: dict) -> None:
    cd_municipio = ctx.get("cd_municipio")
    nm_municipio = ctx.get("nm_municipio")
    recorte = f"{nm_municipio}/{ctx['uf']}" if cd_municipio else f"estado {ctx['uf']}"
    recorte_kpi = "município" if cd_municipio else "UF"

    section_title(
        f"Perfil do eleitorado ({recorte})",
        "Comparecimento do pleito e composição cadastral do eleitorado",
    )

    # 1) Cards laterais — comparecimento e abstenção
    turnout = turnout_uf(ctx["ano"], ctx["uf"], cd_municipio)

    left, center, right = st.columns([1, 1.5, 1.5])

    with left:
        kpi(
            f"Comparecimento ({recorte_kpi})",
            fmt_pct(turnout["pct_comparec"]),
            f"{fmt_int(turnout['comparec'])} eleitores",
        )
        st.write("")
        kpi(
            f"Abstenção ({recorte_kpi})",
            fmt_pct(turnout["pct_abstenc"]),
            f"{fmt_int(turnout['abstenc'])} eleitores",
        )

    # 2) Verifica se temos perfil_eleitorado para esse (ano, UF)
    if not table_exists("perfil_eleitorado"):
        with center:
            st.info(
                "Tabela `perfil_eleitorado` não encontrada no banco. "
                "Importe com `go run go_postgres/10_import_perfil_eleitorado.go`."
            )
        return

    ages = perfil_faixa_etaria(ctx["ano"], ctx["uf"], cd_municipio)
    education = perfil_escolaridade(ctx["ano"], ctx["uf"], cd_municipio)

    if ages.empty and education.empty:
        with center:
            st.info("Dados não encontrados")
        return

    # 3) Gráficos
    with center:
        st.markdown("#### Eleitorado por faixa etária")
        _render_barras(ages, altura=600)

    with right:
        st.markdown("#### Eleitorado por escolaridade")
        _render_barras(education, altura=460)

    st.info("Dados do perfil do eleitorado.")


def _render_barras(df, altura: int) -> None:
    if df.empty:
        st.caption("Sem dados para esse recorte.")
        return
    plot = df.copy()
    total = plot["eleitores"].sum()
    plot["pct"] = plot["eleitores"] / total * 100 if total else 0
    # Preserva ordem da query (idade asc / eleitores desc) no eixo Y do plotly.
    plot["label"] = plot["label"].astype(str)

    fig = px.bar(
        plot,
        x="pct",
        y="label",
        orientation="h",
        text="pct",
        custom_data=["eleitores"],
    )
    fig.update_traces(
        marker_color="#1f6feb",
        texttemplate="%{x:.1f}%",
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>%{customdata[0]:,} eleitores (%{x:.2f}%)<extra></extra>",
        cliponaxis=False,
    )
    fig.update_layout(
        height=altura,
        margin=dict(l=0, r=20, t=10, b=10),
        xaxis_visible=False,
        yaxis=dict(title=None, autorange="reversed", tickfont=dict(size=11)),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
