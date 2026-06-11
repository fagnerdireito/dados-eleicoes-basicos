"""Tab 6: Ranking geral no município - Comparativo entre anos."""
from __future__ import annotations

import streamlit as st

from .components import section_title, fmt_int, fmt_pct, candidato_row
from queries import ranking_municipio


class TabRankingMunicipio:
    """Ranking comparativo de candidatos no município."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o ranking do município."""
        section_title(
            "Ranking geral no município",
            f"Top 10 candidatos mais votados · {ctx['nm_municipio']}"
        )
        
        # Ano atual
        df_atual = ranking_municipio(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo']
        )
        
        # Ano anterior
        ano_anterior = ctx['ano'] - 4
        df_anterior = ranking_municipio(
            ano=ano_anterior,
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo']
        )
        
        col_atual, col_anterior = st.columns(2)
        
        with col_atual:
            st.markdown(f"### 📅 {ctx['ano']}")
            
            if df_atual.empty:
                st.info(f"Sem dados para {ctx['ano']}")
            else:
                for i, row in df_atual.iterrows():
                    is_foco = row['nr'] == ctx['nr_votavel']
                    candidato_row(
                        name=row['nm'],
                        party=row['partido'] or "—",
                        votos=int(row['votos']),
                        pct=float(row['pct']) if pd.notna(row['pct']) else 0,
                        rank=i + 1,
                        is_foco=is_foco
                    )
        
        with col_anterior:
            st.markdown(f"### 📅 {ano_anterior}")
            
            if df_anterior.empty:
                st.info(f"Sem dados para {ano_anterior}")
            else:
                for i, row in df_anterior.iterrows():
                    is_foco = False  # Não há candidato foco no ano anterior
                    candidato_row(
                        name=row['nm'],
                        party=row['partido'] or "—",
                        votos=int(row['votos']),
                        pct=float(row['pct']) if pd.notna(row['pct']) else 0,
                        rank=i + 1,
                        is_foco=is_foco
                    )