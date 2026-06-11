from __future__ import annotations

import streamlit as st

from components import fmt_int, section_title
from queries import sintese_territorial


def render(ctx: dict) -> None:
    if not ctx["cd_municipio"]:
        st.info("Selecione um município para ver a síntese territorial.")
        return

    section_title("Síntese territorial", "Locais liderados por cada candidato")
    df = sintese_territorial(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
    if df.empty:
        st.caption("Sem dados para o filtro.")
        return

    total = int(df["locais"].sum())
    st.caption(f"Total de **{fmt_int(total)} locais** com vencedor apurado.")

    for i, r in enumerate(df.itertuples(index=False), start=1):
        dest = " (foco)" if r.nr == ctx["nr_votavel"] else ""
        st.markdown(
            f"<div style='display:flex;gap:0.8rem;padding:0.5rem 0;border-bottom:1px solid #eef1f5'>"
            f"<div style='width:32px;color:#5b6b80'>{i}</div>"
            f"<div style='flex:1'><div style='font-weight:600;color:#0b2545'>{r.nm}{dest}</div>"
            f"<div style='color:#5b6b80;font-size:0.85rem'>{r.partido or '—'}</div></div>"
            f"<div style='text-align:right;color:#0b2545;font-weight:700'>{fmt_int(r.locais)} <span style='color:#5b6b80;font-weight:400;font-size:0.85rem'>locais</span></div>"
            f"</div>",
            unsafe_allow_html=True,
        )
