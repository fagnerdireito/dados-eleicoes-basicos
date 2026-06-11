from __future__ import annotations

import streamlit as st

from components import bar_row, section_title
from queries import ranking_municipio


def render(ctx: dict) -> None:
    if not ctx["cd_municipio"]:
        st.info("Selecione um município para ver o ranking.")
        return

    section_title("Ranking geral no município", f"Top 10 candidatos mais votados · {ctx['ds_cargo']}")

    ano_atual = ctx["ano"]
    ano_anterior = ano_atual - 4  # mesmo tipo de eleição (4 em 4 anos)

    c_left, c_right = st.columns(2)

    with c_left:
        st.markdown(f"#### {ano_anterior}")
        df_ant = ranking_municipio(ano_anterior, ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
        if df_ant.empty:
            st.caption(f"Sem dados de {ano_anterior} para esse cargo/município.")
        else:
            for i, r in enumerate(df_ant.itertuples(index=False), start=1):
                bar_row(r.nm, r.partido or "—", int(r.votos or 0), float(r.pct or 0), i)

    with c_right:
        st.markdown(f"#### {ano_atual}")
        df_at = ranking_municipio(ano_atual, ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
        if df_at.empty:
            st.caption("Sem dados para o filtro atual.")
        else:
            for i, r in enumerate(df_at.itertuples(index=False), start=1):
                cor = "#22c55e" if r.nr == ctx["nr_votavel"] else "#1f6feb"
                bar_row(r.nm, r.partido or "—", int(r.votos or 0), float(r.pct or 0), i, color=cor)
