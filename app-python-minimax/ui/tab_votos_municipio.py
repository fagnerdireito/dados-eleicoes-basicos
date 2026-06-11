"""Tab 5: Votos no município - Mapa de bolhas por local."""
from __future__ import annotations

import streamlit as st

from .components import section_title, fmt_int
from queries import votos_candidato_por_local


class TabVotosMunicipio:
    """Mapa de bolhas com votação por local de votação."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o mapa de votos no município."""
        section_title(
            "Onde estão os votos no município",
            f"Votação de {ctx['nm_candidato']} por local — {ctx['nm_municipio']}"
        )
        
        df = votos_candidato_por_local(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo'],
            nr_votavel=ctx['nr_votavel']
        )
        
        col_mapa, col_ranking = st.columns([1.5, 1])
        
        with col_mapa:
            st.markdown("### 📍 Mapa de locais de votação")
            
            if df.empty or (df['lat'].isna().all() if 'lat' in df.columns else True):
                st.warning(
                    "⚠️ **Dados de localização não disponíveis**\n\n"
                    "O mapa de bolhas requer:\n"
                    "1. Tabela `local_votacao` importada com coordenadas GPS\n"
                    "2. Campos `NR_LATITUDE` e `NR_LONGITUDE` preenchidos\n\n"
                    "📥 **Como obter:**\n"
                    "Baixe em [dadosabertos.tse.jus.br](https://dadosabertos.tse.jus.br/dataset/local-votacao) "
                    "o arquivo `localvotacao_{ano}.csv` e importe usando scripts de importação"
                )
            else:
                # PLACEHOLDER: Implementar mapa com plotly/scatter_mapbox
                st.info("🗺️ Mapa interativo em desenvolvimento")
                
                # Mostrar estatísticas do mapa
                total_votos = df['votos'].sum()
                max_votos = df['votos'].max()
                st.metric("Total de votos no mapa", fmt_int(total_votos))
                st.metric("Maior concentração", fmt_int(max_votos))
        
        with col_ranking:
            st.markdown("### 🏆 Top locais")
            
            if df.empty:
                st.info("Nenhum local encontrado.")
            else:
                for i, (_, row) in enumerate(df.iterrows()):
                    bar_width = (row['votos'] / df['votos'].max() * 100) if df['votos'].max() > 0 else 0
                    
                    st.markdown(
                        f"""
                        <div style='margin-bottom:0.8rem'>
                            <div style='display:flex;justify-content:space-between'>
                                <span style='color:#5b6b80'>{i+1}º</span>
                                <span style='color:#0b2545;font-weight:500'>{row['nm_local']}</span>
                                <span style='color:#1f6feb;font-weight:700'>{fmt_int(row['votos'])}</span>
                            </div>
                            <div style='background:#eef1f5;border-radius:4px;height:4px;margin-top:2px'>
                                <div style='background:#1f6feb;width:{bar_width:.1f}%;height:100%;border-radius:4px'></div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
        
        # Legenda
        st.caption("📍 Cada bolha é um local de votação (posição por GPS, tamanho conforme os votos)")