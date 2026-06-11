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


def render(ctx: dict) -> None:
    section_title(
        f"Perfil do eleitorado (estado {ctx['uf']})",
        "Comparecimento do pleito e composição cadastral do eleitorado",
    )

    # 1) Cards laterais — comparecimento e abstenção da UF
    turnout = turnout_uf(ctx["ano"], ctx["uf"])

    left, center, right = st.columns([1, 1.5, 1.5])

    with left:
        kpi(
            "Comparecimento (UF)",
            fmt_pct(turnout["pct_comparec"]),
            f"{fmt_int(turnout['comparec'])} eleitores",
        )
        st.write("")
        kpi(
            "Abstenção (UF)",
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

    ages = perfil_faixa_etaria(ctx["ano"], ctx["uf"])
    education = perfil_escolaridade(ctx["ano"], ctx["uf"])

    if ages.empty and education.empty:
        with center:
            st.info(
                f"Não há registros em `perfil_eleitorado` para {ctx['ano']}/{ctx['uf']}. "
                f"Verifique se o ZIP `perfil_eleitorado_{ctx['ano']}.zip` foi importado."
            )
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
