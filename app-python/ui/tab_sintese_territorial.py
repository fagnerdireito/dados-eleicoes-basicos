from __future__ import annotations

import streamlit as st

from components import fmt_int, fmt_pct, section_title
from queries import sintese_territorial, sintese_territorial_uf


def _render_locais(df, nr_foco: str) -> None:
    total = int(df["locais"].sum())
    st.caption(f"Total de **{fmt_int(total)} locais** com vencedor apurado.")

    for i, r in enumerate(df.itertuples(index=False), start=1):
        dest = " (foco)" if r.nr == nr_foco else ""
        st.markdown(
            f"<div style='display:flex;gap:0.8rem;padding:0.5rem 0;border-bottom:1px solid #eef1f5'>"
            f"<div style='width:32px;color:#5b6b80'>{i}</div>"
            f"<div style='flex:1'><div style='font-weight:600;color:#0b2545'>{r.nm}{dest}</div>"
            f"<div style='color:#5b6b80;font-size:0.85rem'>{r.partido or '—'}</div></div>"
            f"<div style='text-align:right;color:#0b2545;font-weight:700'>{fmt_int(r.locais)} "
            f"<span style='color:#5b6b80;font-weight:400;font-size:0.85rem'>locais</span></div>"
            f"</div>",
            unsafe_allow_html=True,
        )


def _render_votos_uf(df, nr_foco: str) -> None:
    total = int(df["votos"].sum())
    st.caption(f"Total de **{fmt_int(total)} votos válidos** na UF.")

    for i, r in enumerate(df.itertuples(index=False), start=1):
        dest = " (foco)" if r.nr == nr_foco else ""
        st.markdown(
            f"<div style='display:flex;gap:0.8rem;padding:0.5rem 0;border-bottom:1px solid #eef1f5'>"
            f"<div style='width:32px;color:#5b6b80'>{i}</div>"
            f"<div style='flex:1'><div style='font-weight:600;color:#0b2545'>{r.nm}{dest}</div>"
            f"<div style='color:#5b6b80;font-size:0.85rem'>{r.partido or '—'}</div></div>"
            f"<div style='text-align:right;color:#0b2545;font-weight:700'>{fmt_int(r.votos)} "
            f"<span style='color:#5b6b80;font-weight:400;font-size:0.85rem'>"
            f"({fmt_pct(r.pct)})</span></div>"
            f"</div>",
            unsafe_allow_html=True,
        )


def render(ctx: dict) -> None:
    cd_municipio = ctx.get("cd_municipio")

    if cd_municipio:
        section_title("Síntese territorial", "Locais liderados por cada candidato")
        df = sintese_territorial(ctx["ano"], ctx["uf"], cd_municipio, ctx["cd_cargo"])
        if df.empty:
            st.caption("Sem dados para o filtro.")
            return
        _render_locais(df, ctx["nr_votavel"])
        return

    section_title(
        "Síntese territorial",
        f"Votos por candidato na UF — {ctx['uf']} · {ctx['ds_cargo']}",
    )
    df = sintese_territorial_uf(ctx["ano"], ctx["uf"], ctx["cd_cargo"])
    if df.empty:
        st.caption("Sem dados para o filtro.")
        return
    _render_votos_uf(df, ctx["nr_votavel"])
