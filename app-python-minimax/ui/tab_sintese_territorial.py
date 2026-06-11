"""Tab 7: Síntese territorial - Locais liderados por candidato."""
from __future__ import annotations

import streamlit as st

from .components import section_title, fmt_int
from queries import sintese_territorial


class TabSinteseTerritorial:
    """Ranking de candidatos por quantidade de locais liderados."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza a síntese territorial."""
        df = sintese_territorial(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo']
        )
        
        total_locais = df['locais'].sum() if not df.empty else 0
        
        section_title(
            "Síntese territorial",
            f"Locais liderados por candidato · {total_locais} locais"
        )
        
        if df.empty:
            st.info("Nenhum dado disponível para síntese territorial.")
            return
        
        # Leaderboard
        for i, row in df.iterrows():
            is_foco = row['nr'] == ctx['nr_votavel']
            bg = "#e8f4fd" if is_foco else "white"
            border = "4px solid #1f6feb" if is_foco else "none"
            
            st.markdown(
                f"""
                <div style='display:flex;align-items:center;padding:1rem;margin-bottom:0.5rem;
                           background:{bg};border:{border};border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.1)'>
                    <div style='width:50px;color:#1f6feb;font-size:1.5rem;font-weight:700;text-align:center'>
                        {i+1}º
                    </div>
                    <div style='flex:1'>
                        <div style='font-weight:600;color:#0b2545;font-size:1.1rem'>
                            {row['nm']}
                        </div>
                        <div style='color:#5b6b80;font-size:0.85rem'>{row['partido'] or '—'}</div>
                    </div>
                    <div style='text-align:right'>
                        <div style='font-size:1.8rem;font-weight:700;color:#1f6feb'>
                            {int(row['locais'])}
                        </div>
                        <div style='color:#5b6b80;font-size:0.85rem'>locais</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )