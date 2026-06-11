"""Tab 8: Card local de votação - Ranking em local específico."""
from __future__ import annotations

import streamlit as st

from .components import section_title, fmt_int, fmt_pct, kpi, candidato_row
from queries import locales_do_municipio, nome_local, top_candidatos_no_local


class TabCardLocal:
    """Ranking de candidatos em um local específico de votação."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o card do local de votação."""
        section_title(
            "Card local de votação",
            f"Análise por local · {ctx['nm_municipio']}"
        )
        
        # Listar locais
        df_locais = locales_do_municipio(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo']
        )
        
        if df_locais.empty:
            st.info("Nenhum local de votação encontrado.")
            return
        
        # Seletor de local
        locais_options = df_locais['nr_local'].tolist()
        
        # Mostrar nomes dos locais
        locais_com_nomes = []
        for nr in locais_options:
            nome = nome_local(ctx['ano'], ctx['uf'], ctx['cd_municipio'], nr)
            locais_com_nomes.append((nr, nome or f"Local {nr}"))
        
        selected = st.selectbox(
            "Selecione o local de votação:",
            options=[l[0] for l in locais_com_nomes],
            format_func=lambda x: next((l[1] for l in locais_com_nomes if l[0] == x), x)
        )
        
        # Dados do local selecionado
        data = top_candidatos_no_local(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo'],
            nr_local=selected
        )
        
        nome_do_local = nome_local(ctx['ano'], ctx['uf'], ctx['cd_municipio'], selected) or f"Local {selected}"
        
        st.markdown(f"#### 🏫 {nome_do_local}")
        
        # Estatísticas do local
        col1, col2, col3 = st.columns(3)
        with col1:
            kpi("Votos Válidos", fmt_int(data['validos']))
        with col2:
            kpi("Brancos", fmt_int(data['brancos']))
        with col3:
            kpi("Nulos", fmt_int(data['nulos']))
        
        st.divider()
        
        # Ranking no local
        st.markdown("### 🏆 Top 10 no local")
        
        if data['ranking'].empty:
            st.info("Nenhum dado de ranking para este local.")
        else:
            for i, row in data['ranking'].iterrows():
                is_foco = row['nr'] == ctx['nr_votavel']
                candidato_row(
                    name=row['nm'],
                    party=row['partido'] or "—",
                    votos=int(row['votos']),
                    pct=float(row['pct']) if pd.notna(row['pct']) else 0,
                    rank=i + 1,
                    is_foco=is_foco
                )
        
        # Footer com destaque do candidato foco
        st.divider()
        
        # Encontrar posição do candidato foco neste local
        ranking_df = data['ranking']
        foco_row = ranking_df[ranking_df['nr'] == ctx['nr_votavel']]
        
        if not foco_row.empty:
            posicao = foco_row.index[0] + 1
            votos = int(foco_row['votos'].iloc[0])
            pct = float(foco_row['pct'].iloc[0]) if pd.notna(foco_row['pct'].iloc[0]) else 0
            
            st.markdown(
                f"""
                <div style='background:#e8f4fd;padding:1rem;border-radius:8px;border-left:4px solid #1f6feb'>
                    <div style='font-weight:600;color:#0b2545'>Desempenho de {ctx['nm_candidato']}</div>
                    <div style='margin-top:0.5rem'>
                        <span style='font-size:1.5rem;font-weight:700;color:#1f6feb'>{posicao}º</span>
                        <span style='margin-left:1rem;color:#0b2545'>{fmt_int(votos)} votos</span>
                        <span style='margin-left:0.5rem;color:#5b6b80'>{fmt_pct(pct)}</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )