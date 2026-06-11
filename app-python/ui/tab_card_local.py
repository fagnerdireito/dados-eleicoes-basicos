from __future__ import annotations

import streamlit as st

from components import bar_row, fmt_int, section_title
from queries import locais_do_municipio, nome_local, top_candidatos_no_local


def render(ctx: dict) -> None:
    if not ctx["cd_municipio"]:
        st.info("Selecione um município para ver os votos por local de votação.")
        return

    locais = locais_do_municipio(
        ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"]
    )
    if locais.empty:
        st.caption("Sem locais para o filtro.")
        return

    nome_cache = {
        nl: (nome_local(ctx["ano"], ctx["uf"], ctx["cd_municipio"], nl) or f"Local {nl}")
        for nl in locais["nr_local"]
    }
    opts = list(locais["nr_local"])
    sel = st.selectbox(
        "Local de votação",
        opts,
        format_func=lambda nl: f"{nome_cache[nl]} ({nl})",
    )

    nome = nome_cache[sel]
    section_title(nome, f"Top 10 no local · {ctx['ds_cargo']}")

    d = top_candidatos_no_local(
        ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], sel
    )
    st.caption(
        f"{fmt_int(d['validos'])} votos válidos · {fmt_int(d['brancos'])} em branco · "
        f"{fmt_int(d['nulos'])} nulos"
    )

    if d["ranking"].empty:
        st.info("Sem ranking para esse local.")
        return

    for i, r in enumerate(d["ranking"].itertuples(index=False), start=1):
        cor = "#22c55e" if r.nr == ctx["nr_votavel"] else "#1f6feb"
        bar_row(r.nm, r.partido or "—", int(r.votos or 0), float(r.pct or 0), i, color=cor)

    foco = d["ranking"][d["ranking"]["nr"] == ctx["nr_votavel"]]
    if not foco.empty:
        st.markdown(
            f"<div style='background:#eef6ff;border-radius:8px;padding:0.6rem 0.9rem;margin-top:0.8rem'>"
            f"Desempenho de <b>{ctx['nm_candidato']}</b>: "
            f"{fmt_int(int(foco['votos'].iloc[0]))} votos "
            f"({float(foco['pct'].iloc[0]):.2f}%) no local.</div>",
            unsafe_allow_html=True,
        )
