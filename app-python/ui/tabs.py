"""Navegação por abas — renderiza só a seção ativa (lazy).

Usa ``st.segmented_control`` ou ``st.radio`` em vez de ``st.tabs``,
que executaria o conteúdo de todas as abas a cada rerun.
"""
from __future__ import annotations

from typing import Any, Protocol

import streamlit as st

from . import (
    tab_card_local,
    tab_comparativo,
    tab_perfil_eleitorado,
    tab_ranking_municipio,
    tab_resumo_municipio,
    tab_sintese_territorial,
    tab_sumario,
    tab_votos_bairro,
    tab_votos_estado,
    tab_votos_municipio,
)


class TabModule(Protocol):
    def render(self, ctx: dict[str, Any]) -> None: ...


TAB_RENDERERS: list[tuple[str, TabModule]] = [
    ("1. Sumário", tab_sumario),
    ("2. Resumo município", tab_resumo_municipio),
    ("3. Perfil eleitorado (UF)", tab_perfil_eleitorado),
    ("4. Votos no estado", tab_votos_estado),
    ("5. Votos no município", tab_votos_municipio),
    ("6. Ranking município", tab_ranking_municipio),
    ("7. Síntese territorial", tab_sintese_territorial),
    ("8. Votos por local de votação", tab_card_local),
    ("9. Comparativo candidatos", tab_comparativo),
    ("10. Votos por bairro", tab_votos_bairro),
]

_TAB_LABELS = [label for label, _ in TAB_RENDERERS]


def _tab_id(label: str) -> str:
    return label.split(".", 1)[0].strip()


def _tab_from_id(tab_id: str | None, labels: list[str]) -> str | None:
    if not tab_id:
        return None
    for label in labels:
        if _tab_id(label) == tab_id:
            return label
    return None


def _sync_tab_query_param(tab_label: str) -> None:
    tab_id = _tab_id(tab_label)
    if st.query_params.get("tab", "") == tab_id:
        return
    qp = dict(st.query_params.to_dict())
    qp["tab"] = tab_id
    st.query_params.from_dict(qp)


def _init_session_tab() -> None:
    if "app_tab" not in st.session_state:
        qp_tab = _tab_from_id(st.query_params.get("tab"), _TAB_LABELS)
        st.session_state.app_tab = qp_tab or _TAB_LABELS[0]
    elif st.session_state.app_tab not in _TAB_LABELS:
        st.session_state.app_tab = _TAB_LABELS[0]


def _render_nav() -> str:
    with st.container(key="app-tab-nav"):
        if hasattr(st, "segmented_control"):
            return st.segmented_control(
                "Seção",
                _TAB_LABELS,
                key="app_tab",
                label_visibility="collapsed",
            )
        return st.radio(
            "Seção",
            _TAB_LABELS,
            horizontal=True,
            key="app_tab",
            label_visibility="collapsed",
        )


def render(ctx: dict[str, Any]) -> None:
    """Exibe a navegação e o conteúdo da aba selecionada."""
    _init_session_tab()
    selected = _render_nav()
    _sync_tab_query_param(selected)

    for label, module in TAB_RENDERERS:
        if label == selected:
            module.render(ctx)
            break
