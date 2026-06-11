from __future__ import annotations

import streamlit as st

from components import section_title


def render(ctx: dict) -> None:
    section_title("Sumário", "Índice das seções/abas do dossiê")
    cidade = ctx.get("nm_municipio") or "estado inteiro"
    st.caption(
        f"Eleição **{ctx['ano']}** · UF **{ctx['uf']}** · Cidade **{cidade}** · "
        f"Cargo **{ctx['ds_cargo']}** · Candidato foco **{ctx['nm_candidato']}**"
    )

    itens = [
        ("Resumo no município", "KPIs do candidato no município, posição geral, liderança em locais e composição dos votos."),
        ("Perfil do eleitorado (UF)", "Comparecimento e abstenção por faixa etária e escolaridade — recorte por UF."),
        ("Onde estão os votos no estado", "Mapa coroplético dos votos do candidato por município da UF."),
        ("Onde estão os votos no município", "Mapa de bolhas por local de votação (precisa de coordenadas; só 2024)."),
        ("Ranking geral no município", "Top 10 candidatos no município comparando com a eleição anterior."),
        ("Síntese territorial", "Quantos locais cada candidato lidera dentro do município."),
        ("Votos por local de votação", "Top 10 candidatos por local + totais (válidos, brancos e nulos)."),
        ("Votos por bairro", "Agregação por bairro/local via JOIN com local_votacao (só 2024)."),
    ]
    for i, (titulo, descricao) in enumerate(itens, start=2):
        st.markdown(
            f"<div style='display:flex;gap:0.8rem;padding:0.5rem 0;border-bottom:1px solid #eef1f5'>"
            f"<div style='color:#5b6b80;width:32px'>{i:02d}</div>"
            f"<div><div style='font-weight:600;color:#0b2545'>{titulo}</div>"
            f"<div style='color:#5b6b80'>{descricao}</div></div></div>",
            unsafe_allow_html=True,
        )

    st.markdown("---")
    st.caption(
        "Use as abas no topo para navegar. Os filtros globais (acima das abas) "
        "se aplicam a todo o dossiê."
    )
