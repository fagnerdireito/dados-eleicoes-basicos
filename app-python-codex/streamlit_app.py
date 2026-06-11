"""Entrypoint do Dossie Eleitoral Codex."""

from __future__ import annotations

import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent
SRC_DIR = APP_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import streamlit as st

from election_app.config import ASSETS_DIR
from election_app.database import check_connection, clear_query_cache
from election_app.repository import data_capabilities
from election_app.ui.components import inject_css
from election_app.ui.filters import render_filters
from election_app.ui.tabs.dashboard import render_all_tabs


st.set_page_config(
    page_title="Dossie Eleitoral",
    page_icon=str(ASSETS_DIR / "logo_elegis.png"),
    layout="wide",
    initial_sidebar_state="collapsed",
)
inject_css()

logo_path = ASSETS_DIR / "logo_elegis.png"
header_logo = ""
if logo_path.exists():
    import base64

    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    header_logo = f'<img src="data:image/png;base64,{encoded}" alt="Elegis">'

st.markdown(
    f"""
    <div class="app-header">
      {header_logo}
      <div><h1>Dossie Eleitoral</h1><p>Resultados, perfil e distribuicao territorial com dados abertos do TSE.</p></div>
    </div>
    """,
    unsafe_allow_html=True,
)

connected, connection_detail = check_connection()
if not connected:
    st.error("Nao foi possivel conectar ao PostgreSQL configurado no .env.")
    st.code(connection_detail)
    st.stop()

with st.sidebar:
    st.markdown("### Diagnostico")
    st.success(f"PostgreSQL conectado: {connection_detail}")
    if st.button("Limpar cache de consultas", width="stretch"):
        clear_query_cache()
        st.rerun()
    st.caption("A conexao e configurada como somente leitura. O app nao executa DDL, INSERT, UPDATE ou DELETE.")

try:
    scope = render_filters()
except Exception as exc:
    st.error("Falha ao carregar os catalogos eleitorais.")
    st.exception(exc)
    st.stop()

if scope is None:
    st.stop()

st.markdown(
    f"""
    <div class="scope-strip">
      <strong>{scope.election.label}</strong> | {scope.uf} | {scope.municipality_name or 'Todos os municipios'} |
      {scope.office_name} | {scope.candidate_name} ({scope.candidate_party})
    </div>
    """,
    unsafe_allow_html=True,
)

try:
    capabilities = data_capabilities(scope)
    with st.sidebar:
        st.markdown("### Cobertura do recorte")
        st.write(f"Perfil do eleitorado: {'disponivel' if capabilities['electorate_profile'] else 'ausente'}")
        st.write(f"Locais de votacao: {'disponivel' if capabilities['voting_locations'] else 'ausente'}")
        st.write(f"Coordenadas: {'disponiveis' if capabilities['coordinates'] else 'ausentes'}")
        st.write(f"Cadastro de candidatos: {'disponivel' if capabilities['candidate_registry'] else 'ausente'}")
    render_all_tabs(scope, capabilities)
except Exception as exc:
    st.error("Uma consulta do dashboard falhou. Nenhuma alteracao foi feita no banco.")
    st.exception(exc)
