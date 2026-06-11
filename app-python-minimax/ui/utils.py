"""Funções utilitárias para a UI."""
from __future__ import annotations

import streamlit as st


def render_placeholder(message: str, suggestion: str | None = None) -> None:
    """Renderiza mensagem de dados não disponíveis."""
    st.info(
        f"📋 **{message}**"
        + (f"\n\n💡 *Sugestão: {suggestion}*" if suggestion else "")
    )


def render_warning(message: str) -> None:
    """Renderiza aviso/warning."""
    st.warning(message)


def render_error(message: str) -> None:
    """Renderiza mensagem de erro."""
    st.error(message)


def check_data_availability(tab_name: str) -> dict:
    """Verifica se dados existem para a aba específica.
    
    Returns:
        dict com 'available' (bool) e 'message' (str)
    """
    from db import run_df
    
    checks = {
        'tab_votos_municipio': {
            'check': lambda: run_df('SELECT COUNT(*) FROM local_votacao').iloc[0, 0] > 0,
            'message': 'Local de votação não importado. Importe os dados de local_votacao do TSE.'
        },
        'tab_votos_bairro': {
            'check': lambda: run_df('SELECT COUNT(*) FROM local_votacao').iloc[0, 0] > 0,
            'message': 'Dados de bairro não disponíveis. Importe local_votacao para ver distribuição por bairro.'
        },
        'tab_perfil_eleitorado': {
            'check': lambda: False,  # Sempre retorna placeholder
            'message': 'Dados de perfil do eleitorado não disponíveis no banco. Importe do TSE.'
        }
    }
    
    if tab_name in checks:
        try:
            available = checks[tab_name]['check']()
            return {
                'available': available,
                'message': checks[tab_name]['message'],
                'show_placeholder': not available
            }
        except Exception:
            return {
                'available': False,
                'message': checks[tab_name]['message'],
                'show_placeholder': True
            }
    
    return {'available': True, 'message': '', 'show_placeholder': False}


def nome_eleicao(ano: int, municipal: bool) -> str:
    """Retorna nome da eleição baseado no ano."""
    if municipal:
        return f"Municipal {ano}"
    return f"Geral {ano}"


def badge(text: str, color: str = "#1f6feb") -> str:
    """Cria badge/styled text."""
    return f"<span style='background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.8rem'>{text}</span>"