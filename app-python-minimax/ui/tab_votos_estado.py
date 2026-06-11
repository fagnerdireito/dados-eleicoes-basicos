"""Tab 4: Votos no estado - Mapa coroplético + ranking."""
from __future__ import annotations

import streamlit as st

from .components import section_title, fmt_int
from queries import votos_candidato_por_municipio


class TabVotosEstado:
    """Mapa de votação por município no estado."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o mapa de votos no estado."""
        section_title(
            "Onde estão os votos no estado",
            f"Votação de {ctx['nm_candidato']} por município – {ctx['uf']}"
        )
        
        df = votos_candidato_por_municipio(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_cargo=ctx['cd_cargo'],
            nr_votavel=ctx['nr_votavel']
        )
        
        if df.empty:
            st.info("Nenhum dado de votação encontrado para os filtros selecionados.")
            return
        
        col_mapa, col_ranking = st.columns([1.5, 1])
        
        with col_mapa:
            st.markdown("### 🗺️ Mapa do estado")
            
            # PLACEHOLDER: Mapa coroplético requer geobr
            # Implementar com: geobr::read_municipality() + plotly
            st.info(
                "🗺️ **Mapa interativo**\n\n"
                "Para ativar o mapa coroplético:\n"
                f"1. Instale `geobr`: `pip install geobr`\n"
                f"2. Implemente com `geobr::read_municipality(state='{ctx['uf']}')`\n"
                "3. Use plotly para criar o mapa colorido por intensidade de votos"
            )
            
            # Fallback: tabela de municípios
            st.markdown("**Distribuição por município:**")
            df_display = df.head(15).copy()
            df_display['votos_fmt'] = df_display['votos'].apply(fmt_int)
            st.dataframe(
                df_display[['nm', 'votos_fmt']].rename(columns={'nm': 'Município', 'votos_fmt': 'Votos'}),
                hide_index=True
            )
        
        with col_ranking:
            st.markdown("### 🏆 Top municípios")
            
            for i, row in df.head(20).iterrows():
                is_selected = row['cd'] == ctx['cd_municipio']
                bg = "#e8f4fd" if is_selected else "transparent"
                border = "3px solid #1f6feb" if is_selected else "none"
                
                st.markdown(
                    f"""
                    <div style='display:flex;justify-content:space-between;padding:0.4rem;
                               background:{bg};border-left:{border};margin-bottom:0.2rem;border-radius:4px'>
                        <div style='color:#5b6b80;width:25px'>{i+1}º</div>
                        <div style='flex:1;color:#0b2545;font-weight:500'>{row['nm']}</div>
                        <div style='color:#1f6feb;font-weight:700'>{fmt_int(row['votos'])}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )