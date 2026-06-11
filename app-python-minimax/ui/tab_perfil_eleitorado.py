"""Tab 3: Perfil do eleitorado - Abstenção por idade e escolaridade."""
from __future__ import annotations

import streamlit as st

from .components import section_title, kpi
from .utils import render_placeholder


class TabPerfilEleitorado:
    """Dashboard de perfil do eleitorado (placeholder)."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o perfil do eleitorado."""
        section_title(
            f"Perfil do eleitorado ({ctx['uf']})",
            "Comparecimento e abstenção"
        )
        
        # PLACEHOLDER: Dados de perfil não existem no banco atual
        # Para implementar, seria necessário importar do TSE:
        # https://dadosabertos.tse.jus.br/dataset/perfil-eleitorado
        
        st.warning(
            "⚠️ **Dados de perfil do eleitorado não disponíveis**\n\n"
            "Os dados de abstenção por idade e escolaridade não estão no banco.\n\n"
            "**Para obter:**\n"
            "1. Baixe o arquivo de perfil do eleitorado em [dadosabertos.tse.jus.br](https://dadosabertos.tse.jus.br/dataset/perfil-eleitorado)\n"
            "2. Importe para uma tabela `perfil_eleitorado` no banco\n"
            "3. Crie novas queries em queries/abas.py para buscar esses dados"
        )
        
        # Layout placeholder com estrutura esperada
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### 📊 KPIs")
            kpi("Comparecimento (UF)", "—", "aguardando dados")
            kpi("Abstenção (UF)", "—", "aguardando dados")
        
        with col2:
            st.markdown("### 📅 Abstenção por faixa etária")
            st.info("Gráfico aguardando importação de dados")
        
        with col3:
            st.markdown("### 🎓 Abstenção por escolaridade")
            st.info("Gráfico aguardando importação de dados")