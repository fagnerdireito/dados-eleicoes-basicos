"""Tab 2: Resumo no município - KPIs do candidato."""
from __future__ import annotations

import streamlit as st

from .components import section_title, kpi, fmt_int, fmt_pct, render_composicao_votos
from queries import resumo_candidato_municipio


class TabResumoMunicipio:
    """Métricas principais para o card de resumo."""
    
    def render(self, ctx: dict) -> None:
        """Renderiza o resumo do candidato no município."""
        data = resumo_candidato_municipio(
            ano=ctx['ano'],
            uf=ctx['uf'],
            cd_municipio=ctx['cd_municipio'],
            cd_cargo=ctx['cd_cargo'],
            nr_votavel=ctx['nr_votavel']
        )
        
        section_title(
            f"Resumo em {ctx['nm_municipio']} ({ctx['uf']})",
            f"Desempenho de {data['nm_candidato']} · {ctx['ano']}"
        )
        
        # Cards de métricas principais
        cols = st.columns([1, 1, 1, 1])
        
        with cols[0]:
            kpi(
                "Votação do candidato",
                fmt_int(data['votos_cand']),
                f"{fmt_pct(data['pct_validos'])} dos {fmt_int(data['validos'])} votos válidos"
            )
        
        with cols[1]:
            pos_text = f"{data['posicao']}º" if data['posicao'] else "—"
            kpi(
                "Posição geral",
                pos_text,
                f"de {data['total_cands']} candidatos"
            )
        
        with cols[2]:
            kpi(
                "Liderança nos locais",
                fmt_int(data['lideres']),
                f"de {data['total_locais']} locais analisados"
            )
        
        with cols[3]:
            kpi(
                "Locais analisados",
                fmt_int(data['total_locais']),
                f"no município"
            )
        
        st.divider()
        
        # Composição dos votos
        section_title("Composição dos votos no município")
        
        render_composicao_votos(
            validos=data['validos'],
            brancos=data['brancos'],
            nulos=data['nulos'],
            comparec=data['comparec'],
            aptos=data['aptos']
        )