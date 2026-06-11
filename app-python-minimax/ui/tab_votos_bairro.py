"""Tab 9: Votos por bairro - Drill-down territorial."""
from __future__ import annotations

import streamlit as st

from .components import section_title, fmt_int
from queries import votos_por_bairro


class TabVotosBairro:
    """Drill-down de votação por bairro e local de votação."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o drill-down por bairro."""
        section_title(
            "Votos por bairro",
            f"Detalhamento territorial · {ctx['nm_municipio']}"
        )
        
        df = votos_por_bairro(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo'],
            nr_votavel=ctx['nr_votavel']
        )
        
        if df.empty:
            st.warning(
                "⚠️ **Dados de bairro não disponíveis**\n\n"
                "O drill-down por bairro requer:\n"
                "1. Tabela `local_votacao` importada com campo `NM_BAIRRO`\n\n"
                "📥 **Como obter:**\n"
                "Importe o arquivo `localvotacao_{ano}.csv` do TSE para ter acesso aos dados de bairro."
            )
            return
        
        # 1. Resumo por Município
        st.markdown("### 📊 Resumo por Município")
        total_votos = df['votos'].sum()
        
        st.markdown(
            f"""
            <div style='display:flex;gap:2rem;padding:1rem;background:#f8f9fa;border-radius:8px'>
                <div>
                    <div style='color:#5b6b80;font-size:0.85rem'>Município</div>
                    <div style='font-weight:600;color:#0b2545'>{ctx['nm_municipio']}</div>
                </div>
                <div>
                    <div style='color:#5b6b80;font-size:0.85rem'>Ano</div>
                    <div style='font-weight:600;color:#0b2545'>{ctx['ano']}</div>
                </div>
                <div>
                    <div style='color:#5b6b80;font-size:0.85rem'>Votos de {ctx['nm_candidato']}</div>
                    <div style='font-weight:700;color:#1f6feb;font-size:1.5rem'>{fmt_int(total_votos)}</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        st.divider()
        
        # 2. Detalhamento por Bairro
        st.markdown("### 🏘️ Detalhamento por Bairro")
        
        # Agrupar por bairro
        df_bairro = df.groupby('bairro', as_index=False).agg({
            'votos': 'sum',
            'nm_votavel': 'first'
        }).sort_values('votos', ascending=False)
        
        total_bairros = len(df_bairro)
        
        for i, row in df_bairro.iterrows():
            bar_width = (row['votos'] / total_votos * 100) if total_votos > 0 else 0
            
            st.markdown(
                f"""
                <div style='margin-bottom:0.5rem'>
                    <div style='display:flex;justify-content:space-between'>
                        <span style='color:#0b2545;font-weight:500'>{row['bairro']}</span>
                        <span style='color:#1f6feb;font-weight:700'>{fmt_int(row['votos'])}</span>
                    </div>
                    <div style='background:#eef1f5;border-radius:4px;height:6px;margin-top:2px'>
                        <div style='background:#1f6feb;width:{bar_width:.1f}%;height:100%;border-radius:4px'></div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        st.caption(f"Total: {total_bairros} bairros/distritos")
        
        st.divider()
        
        # 3. Detalhamento por Local de Votação (via bairro)
        st.markdown("### 🏫 Detalhamento por Local de Votação")
        
        # Mostrar os locais com seus votos (ordenados)
        for _, row in df.iterrows():
            st.markdown(
                f"""
                <div style='display:flex;justify-content:space-between;padding:0.4rem;
                           border-bottom:1px solid #eef1f5'>
                    <div style='color:#5b6b80'>{row['bairro']}</div>
                    <div style='color:#0b2545'>{row['nm_votavel']}</div>
                    <div style='color:#1f6feb;font-weight:600'>{fmt_int(row['votos'])}</div>
                </div>
                """,
                unsafe_allow_html=True
            )