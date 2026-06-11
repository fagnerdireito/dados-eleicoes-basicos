from __future__ import annotations

import streamlit as st

from components import fmt_int, fmt_pct, kpi, section_title
from queries import resumo_candidato_municipio


def render(ctx: dict) -> None:
    if not ctx["cd_municipio"]:
        st.info("Selecione um município nos filtros globais para ver o resumo.")
        return

    section_title(
        f"{ctx['nm_municipio']} ({ctx['uf']})",
        f"Resumo de {ctx['nm_candidato']} no município · {ctx['ano']} · {ctx['ds_cargo']}",
    )

    d = resumo_candidato_municipio(
        ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], ctx["nr_votavel"]
    )

    c1, c2 = st.columns(2)
    with c1:
        kpi(
            "Votação do candidato",
            fmt_int(d["votos_cand"]),
            f"{fmt_pct(d['pct_validos'])} dos {fmt_int(d['validos'])} votos válidos do município.",
        )
    with c2:
        kpi(
            "Posição geral no município",
            f"{d['posicao'] or '—'}º" if d["posicao"] else "—",
            f"Classificação entre {fmt_int(d['total_cands'])} candidatos ao mesmo cargo.",
        )

    c3, c4 = st.columns(2)
    with c3:
        kpi(
            "Liderança nos locais",
            fmt_int(d["lideres"]),
            f"locais onde ficou em 1º, de {fmt_int(d['total_locais'])} analisados.",
        )
    with c4:
        kpi(
            "Locais de votação analisados",
            fmt_int(d["total_locais"]),
            f"Comparecimento de {fmt_pct(d['pct_comparec'])} ({fmt_int(d['comparec'])}/{fmt_int(d['aptos'])}).",
        )

    st.markdown("### Composição dos votos no município")
    cols = st.columns(5)
    cols[0].metric("Válidos (nominais + legenda)", fmt_int(d["validos"]))
    cols[1].metric("Brancos", fmt_int(d["brancos"]))
    cols[2].metric("Nulos", fmt_int(d["nulos"]))
    cols[3].metric("Abstenções", fmt_int(d["abstenc"]))
    cols[4].metric("Comparecimento", fmt_pct(d["pct_comparec"], casas=0))
