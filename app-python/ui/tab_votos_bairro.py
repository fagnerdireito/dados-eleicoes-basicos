from __future__ import annotations

import streamlit as st

from components import fmt_int, section_title
from db import table_exists
from queries import votos_candidato_por_municipio, votos_por_bairro, votos_por_local_candidato


def render(ctx: dict) -> None:
    section_title("Votos por bairro", f"Agregação por bairro/local — {ctx['nm_candidato']}")

    if not table_exists("local_votacao"):
        st.info("Dados não encontrados.")
        return

    if not ctx["cd_municipio"]:
        st.info(
            "Selecione um município nos filtros (ou deixe a UF para o comparativo geral abaixo)."
        )

    df_mun = votos_candidato_por_municipio(
        ctx["ano"],
        ctx["uf"],
        ctx["cd_cargo"],
        ctx["nr_votavel"],
        ctx.get("cd_municipio"),
    )
    if not df_mun.empty:
        total = int(df_mun["votos"].sum())
        st.markdown("##### Município")
        st.dataframe(
            df_mun.assign(Anos=ctx["ano"], Votos=df_mun["votos"]).rename(columns={"nm": "Município"})[
                ["Município", "Anos", "Votos"]
            ],
            hide_index=True,
            use_container_width=True,
        )
        st.caption(f"Total: {fmt_int(total)}")

    if ctx["cd_municipio"]:
        st.markdown("##### Bairro")
        df_b = votos_por_bairro(
            ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], ctx["nr_votavel"]
        )
        if df_b.empty:
            st.caption("Sem votos por bairro para o filtro.")
        else:
            df_show = df_b.assign(Anos=ctx["ano"]).rename(
                columns={"bairro": "Bairro", "votos": "Votos", "nm_votavel": "Candidato"}
            )[["Bairro", "Anos", "Votos", "Candidato"]]
            st.dataframe(df_show, hide_index=True, use_container_width=True)
            st.caption(f"Total: {fmt_int(int(df_b['votos'].sum()))}")

        st.markdown("##### Local")
        df_l = votos_por_local_candidato(
            ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], ctx["nr_votavel"]
        )
        if df_l.empty:
            st.caption("Sem votos por local para o filtro.")
        else:
            df_show = df_l.assign(Anos=ctx["ano"]).rename(
                columns={"local": "Local", "votos": "Votos", "nm_votavel": "Candidato"}
            )[["Local", "Anos", "Votos", "Candidato"]]
            st.dataframe(df_show, hide_index=True, use_container_width=True, height="content")
            st.caption(f"Total: {fmt_int(int(df_l['votos'].sum()))}")
