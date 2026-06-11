"""Tab 1: Sumário - Índice do relatório."""
from __future__ import annotations

import streamlit as st

from .components import section_title


class TabSumario:
    """Índice/navegação do relatório com 9 seções."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o sumário."""
        section_title("Sumário", "Índice de páginas — 9 seções")
        
        secoes = [
            ("2", "Resumo no município", "📊"),
            ("3", "Perfil do eleitorado (estado)", "👥"),
            ("4", "Onde estão os votos no estado", "🗺️"),
            ("5", "Onde estão os votos no município", "📍"),
            ("6", "Ranking geral no município", "🏆"),
            ("7", "Síntese territorial", "🗂"),
            ("8", "Card local de votação", "🏫"),
            ("9", "Votos por bairro", "🏘"),
        ]
        
        col1, col2 = st.columns(2)
        
        with col1:
            for i, (pagina, titulo, emoji) in enumerate(secoes[:4]):
                st.markdown(
                    f"""
                    <div style='display:flex;align-items:center;padding:0.5rem 0;
                               border-bottom:1px solid #eef1f5'>
                        <div style='width:40px;color:#1f6feb;font-weight:700;font-size:1.1rem'>{pagina}</div>
                        <div style='font-size:1.2rem;margin-right:0.5rem'>{emoji}</div>
                        <div style='color:#0b2545;font-weight:500'>{titulo}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        
        with col2:
            for i, (pagina, titulo, emoji) in enumerate(secoes[4:]):
                st.markdown(
                    f"""
                    <div style='display:flex;align-items:center;padding:0.5rem 0;
                               border-bottom:1px solid #eef1f5'>
                        <div style='width:40px;color:#1f6feb;font-weight:700;font-size:1.1rem'>{pagina}</div>
                        <div style='font-size:1.2rem;margin-right:0.5rem'>{emoji}</div>
                        <div style='color:#0b2545;font-weight:500'>{titulo}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        
        # Info do contexto atual
        st.divider()
        st.caption(
            f"📌 Contexto atual: {ctx['nm_municipio'] or ctx['uf']} — "
            f"{ctx['nm_candidato']} ({ctx['ds_cargo']}) — {ctx['ano']}"
        )