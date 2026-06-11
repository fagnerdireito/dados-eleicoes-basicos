from __future__ import annotations

import streamlit as st

from components import section_title
from db import table_exists


def render(ctx: dict) -> None:
    section_title(
        f"Perfil do eleitorado (estado {ctx['uf']})",
        "Comparecimento e abstenção — recorte por UF.",
    )

    st.info(
        "O TSE só publica este recorte por UF, não por município ou bairro. "
        "Esta aba requer a tabela `perfil_comparecimento_abstencao`, "
        "que ainda não está carregada no banco."
    )

    if table_exists("perfil_comparecimento_abstencao"):
        st.success("Tabela detectada — atualize esta aba com as consultas reais quando precisar.")
        return

    st.markdown(
        f"""
        **Como habilitar esta aba**

        1. Baixe o ZIP do TSE:
        ```bash
        curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_comparecimento_abstencao/perfil_comparecimento_abstencao_{ctx['ano']}.zip
        ```
        2. Importe para o banco `eleicoes` em uma tabela chamada `perfil_comparecimento_abstencao`
           (pode reaproveitar o padrão dos scripts em `go_postgres/`).
        3. Recarregue esta página — a aba detectará a tabela automaticamente e poderá ser
           expandida com gráficos de barra por faixa etária e escolaridade.
        """
    )
