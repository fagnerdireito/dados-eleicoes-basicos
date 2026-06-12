"""Streamlit app — Dados Eleitorais.

Filtros globais (Eleição/Ano + UF + Cidade + Cargo + Candidato) ficam acima
das 9 abas. Cada aba é um módulo em ui/ que recebe um dict ``ctx`` com o
contexto selecionado pelo usuário.
"""
from __future__ import annotations

import base64
import html
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from db import is_municipal
from queries import _usa_catalogo_filtros
from queries import (
    listar_anos,
    listar_cargos,
    listar_candidatos,
    listar_municipios,
    listar_ufs,
)
from ui import (
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

LOGO_PATH = Path(__file__).resolve().parent.parent / "imagens" / "logo-elegis-light.png"
FAVICON_PATH = Path(__file__).resolve().parent.parent / "imagens" / "favicon.png"

st.set_page_config(
    page_title="Dados Eleitorais",
    page_icon=str(FAVICON_PATH) if FAVICON_PATH.is_file() else "🗳️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      /* Streamlit fixa a barra superior (Deploy/menu); padding baixo cortava o título. */
      .block-container {
        padding-top: 4.5rem;
        padding-bottom: 100px;
      }
      h1, h2, h3 { color: #0b2545; }
      [data-testid="stMetricLabel"] { color: #5b6b80; }

      .app-header {
        display: flex;
        align-items: center;
        gap: 1rem;
        flex-wrap: wrap;
      }
      .app-header-logo {
        width: 150px;
        max-width: 42vw;
        height: auto;
        flex-shrink: 0;
      }
      .app-header-text {
        border-left: 4px solid #1f6feb;
        padding-left: 0.6rem;
        flex: 1 1 240px;
        min-width: 0;
      }
      .app-header-title {
        font-size: clamp(1.25rem, 4vw, 1.6rem);
        font-weight: 800;
        color: #0b2545;
        line-height: 1.2;
      }
      .app-header-subtitle {
        color: #5b6b80;
        margin-top: 0.2rem;
        font-size: clamp(0.85rem, 2.5vw, 0.95rem);
        line-height: 1.35;
      }

      .print-toolbar {
        display: flex;
        justify-content: flex-end;
        margin-bottom: 0.5rem;
      }
      .print-page-btn {
        background: #1f6feb;
        color: #fff;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-size: 0.9rem;
        font-weight: 600;
        cursor: pointer;
        line-height: 1.2;
      }
      .print-page-btn:hover { background: #1758c7; }

      /* Abas — cartões brancos, ativa em azul; no mobile quebram linha e ficam arredondadas */
      div[data-testid="stTabs"] [data-baseweb="tab-list"] {
        display: flex;
        flex-wrap: nowrap;
        gap: 0.35rem;
        background: #f0f3f7;
        border-bottom: 1px solid #dce3eb;
        padding: 0.45rem 0.45rem 0;
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
      }
      div[data-testid="stTabs"] [data-baseweb="tab"] {
        background: white;
        border: 1px solid #e2e8f0;
        border-bottom: none;
        border-radius: 8px 8px 0 0;
        height: auto;
        margin-bottom: -1px;
        padding: 10px 20px;
      }
      div[data-testid="stTabs"] [data-baseweb="tab"] button {
        background: transparent;
        border: none;
        color: #4a5c6e;
        font-weight: 500;
        font-size: 0.92rem;
        padding: 0.65rem 0.9rem;
        white-space: nowrap;
      }
      div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {
        border-bottom: 2px solid #1f6feb;
        z-index: 1;
      }
      div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] button {
        color: #1f6feb;
        font-weight: 650;
      }
        div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
        background-color: transparent !important;
      }

      /* Navegação lazy (radio / segmented_control) — visual de abas */
      .st-key-app-tab-nav [data-testid="stRadio"],
      .st-key-app-tab-nav [data-testid="stSegmentedControl"] {
        background: #f0f3f7;
        border: 1px solid #dce3eb;
        border-radius: 8px;
        padding: 0.45rem;
        margin-bottom: 0.75rem;
      }
      .st-key-app-tab-nav [data-testid="stRadio"] > div {
        flex-wrap: wrap;
        gap: 0.35rem;
      }
      .st-key-app-tab-nav [data-testid="stRadio"] label {
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 0.55rem 0.85rem;
        margin: 0 !important;
        font-size: 0.92rem;
        color: #4a5c6e;
      }
      .st-key-app-tab-nav [data-testid="stRadio"] label:has(input:checked) {
        border-color: #1f6feb;
        color: #1f6feb;
        font-weight: 650;
        box-shadow: inset 0 -2px 0 #1f6feb;
      }
      .st-key-app-tab-nav [data-testid="stRadio"] label input {
        display: none;
      }

      .print-filter-summary { display: none; }
      .element-container:has(.print-filter-summary) { display: none !important; }
      #global-filters-marker,
      .global-filter-marker { display: none; }
      .element-container:has(.global-filter-marker) {
        height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: hidden !important;
      }
      @media print {
        /* Esconde o botão Imprimir — o markup está num iframe do
           components.html, então o seletor precisa pegar o wrapper
           gerado pelo st.container(key="print-toolbar"). */
        .st-key-print-toolbar,
        .element-container:has(.st-key-print-toolbar),
        .print-toolbar,
        .print-page-btn,
        .element-container:has(.print-toolbar),
        [data-testid="stHtml"]:has(.print-toolbar) {
          display: none !important;
          height: 0 !important;
          margin: 0 !important;
          padding: 0 !important;
          overflow: hidden !important;
        }
        header[data-testid="stHeader"],
        section[data-testid="stSidebar"],
        footer { display: none !important; }
        .block-container { padding-top: 0.5rem !important; }
        .element-container:has(.global-filter-marker),
        .element-container:has(.global-filter-marker) + .element-container {
          display: none !important;
        }
        .element-container:has(.print-filter-summary) {
          display: block !important;
        }
        .print-filter-summary {
          display: block !important;
          color: #0b2545;
          font-size: 0.95rem;
          margin: 0 0 0.75rem;
          padding: 0 0 0.65rem;
          border-bottom: 1px solid #dce3eb;
        }
        .element-container:has([data-testid="stDivider"]),
        .element-container:has(.print-filter-summary) + .element-container:has(hr) {
          display: none !important;
          height: 0 !important;
          margin: 0 !important;
          padding: 0 !important;
        }
        div[data-testid="stTabs"] [data-baseweb="tab-list"] { display: none !important; }
      }

      @media (max-width: 768px) {
        .app-header {
          flex-direction: column;
          align-items: flex-start;
          gap: 0.6rem;
        }
        .app-header-logo {
          max-width: 130px;
        }
        .app-header-text {
          border-left: none;
          padding-left: 0;
          width: 100%;
          flex: 0 0 auto;
        }
        .element-container:has(.app-header) {
          margin-bottom: 0.35rem !important;
        }
        [data-testid="stHorizontalBlock"] {
          flex-wrap: wrap !important;
          gap: 0.25rem !important;
        }
        [data-testid="stHorizontalBlock"] > [data-testid="column"] {
          flex: 1 1 100% !important;
          min-width: 100% !important;
          width: 100% !important;
        }
        div[data-testid="stTabs"] [data-baseweb="tab-list"] {
          flex-wrap: wrap;
          overflow-x: visible;
          gap: 0.4rem;
          padding: 0.45rem;
          border-bottom: none;
        }
        div[data-testid="stTabs"] [data-baseweb="tab"] {
          border-radius: 8px;
          border-bottom: 1px solid #e2e8f0;
          margin-bottom: 0;
          flex: 0 1 auto;
        }
        div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {
          border-bottom: 2px solid #1f6feb;
        }
        div[data-testid="stTabs"] [data-baseweb="tab"] button {
          white-space: normal;
          line-height: 1.25;
        }
      }
    </style>
    """,
    unsafe_allow_html=True,
)

logo_html = ""
if LOGO_PATH.is_file():
    encoded = base64.b64encode(LOGO_PATH.read_bytes()).decode("ascii")
    logo_html = (
        f'<img class="app-header-logo" src="data:image/png;base64,{encoded}" alt="Elegis">'
    )

# Botão "Imprimir" — usa components.html porque st.html() do Streamlit
# remove tags <script> por segurança, impedindo o listener de funcionar.
# components.html renderiza num iframe que executa JS e pode chamar
# window.top.print() (a janela do navegador onde o usuário está).
# Envolvido em um container com key="print-toolbar" para que o CSS
# `@media print { .st-key-print-toolbar { display:none } }` esconda o
# wrapper (incluindo o iframe) na hora de imprimir.
with st.container(key="print-toolbar"):
    components.html(
        """
        <style>
          :root { color-scheme: light; }
          html, body { margin: 0; background: transparent; }
          .print-toolbar {
            display: flex;
            justify-content: flex-end;
            margin: 0;
            padding: 0 4px;
          }
          .print-page-btn {
            background: #1f6feb;
            color: #fff;
            border: none;
            border-radius: 8px;
            padding: 0.5rem 1rem;
            font-size: 0.9rem;
            font-weight: 600;
            cursor: pointer;
            line-height: 1.2;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                         Helvetica, Arial, sans-serif;
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
          }
          .print-page-btn:hover { background: #1758c7; }
          .print-page-btn:active { background: #154ba8; }
          .print-page-btn svg { width: 1rem; height: 1rem; }
          /* Quando a impressão for disparada a partir do iframe, escondemos
             o próprio botão para não vazar no preview. */
          @media print {
            html, body, .print-toolbar, .print-page-btn { display: none !important; }
          }
        </style>
        <div class="print-toolbar">
          <button type="button" class="print-page-btn" id="dados-eleitorais-print-btn">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
                 stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <polyline points="6 9 6 2 18 2 18 9"></polyline>
              <path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"></path>
              <rect x="6" y="14" width="12" height="8"></rect>
            </svg>
            Imprimir
          </button>
        </div>
        <script>
          (function () {
            const btn = document.getElementById("dados-eleitorais-print-btn");
            if (!btn) return;
            btn.addEventListener("click", function () {
              // O componente roda dentro de um iframe; subimos até o topo da
              // janela (window.top) para acionar a impressão da página inteira.
              try {
                const w = window.top || window.parent || window;
                w.focus();
                w.print();
              } catch (err) {
                // Se o navegador bloquear cross-origin, imprime o próprio iframe.
                window.print();
              }
            });
          })();
        </script>
        """,
        height=56,
    )

st.markdown(
    f"""
    <div class="app-header">
      <span id="global-filters-marker" aria-hidden="true"></span>
      {logo_html}
      <div class="app-header-text">
        <div class="app-header-title">Dados Eleitorais</div>
        <div class="app-header-subtitle">
          Resultados consolidados a partir do boletim de urna do TSE.
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Filtros globais
# ---------------------------------------------------------------------------
anos = listar_anos()
if not anos:
    st.error("Nenhuma eleição encontrada em `boletim_de_urna`. Importe os dados antes.")
    st.stop()

_FILTER_MARKER = '<span class="global-filter-marker" aria-hidden="true"></span>'
c1, c2, c3, c4, c5 = st.columns([1, 1, 1.4, 1.4, 1.6])
with c1:
    st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
    ano = st.selectbox("Eleição/Ano", anos, index=len(anos) - 1)
municipal = is_municipal(ano)

ufs = listar_ufs(ano)
with c2:
    st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
    uf = st.selectbox("UF", ufs, index=0 if ufs else None)

with c3:
    municipios = listar_municipios(ano, uf)
    st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
    if municipal:
        if municipios.empty:
            st.warning("Sem municípios para o filtro.")
            st.stop()
        idx = 0
        municipio = st.selectbox(
            "Cidade (obrigatório)",
            options=municipios.index,
            format_func=lambda i: f"{municipios.loc[i, 'nm']}",
            index=idx,
        )
        cd_municipio = municipios.loc[municipio, "cd"]
        nm_municipio = municipios.loc[municipio, "nm"]
    else:
        opts = [(None, "— (eleição geral)")] + [
            (row.cd, row.nm) for row in municipios.itertuples(index=False)
        ]
        sel = st.selectbox(
            "Cidade (opcional em geral)",
            opts,
            format_func=lambda x: x[1],
            index=0,
        )
        cd_municipio = sel[0]
        nm_municipio = sel[1] if cd_municipio else None

cargos = listar_cargos(ano, uf, cd_municipio)
with c4:
    st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
    if cargos.empty:
        st.warning("Sem cargos para o filtro.")
        st.stop()
    sel_cargo = st.selectbox(
        "Cargo",
        cargos.index,
        format_func=lambda i: cargos.loc[i, "ds"].title(),
    )
    cd_cargo = cargos.loc[sel_cargo, "cd"]
    ds_cargo = cargos.loc[sel_cargo, "ds"].title()

cands = listar_candidatos(ano, uf, cd_municipio, cd_cargo)
with c5:
    st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
    if cands.empty:
        st.warning("Sem candidatos para o filtro.")
        st.stop()
    sel_cand = st.selectbox(
        "Candidato foco",
        cands.index,
        format_func=lambda i: f"{cands.loc[i, 'nm']} ({cands.loc[i, 'sg_partido'] or '—'})",
    )
    nr_votavel = cands.loc[sel_cand, "nr"]
    nm_candidato = cands.loc[sel_cand, "nm"]
    sg_partido = cands.loc[sel_cand, "sg_partido"] or "—"

cidade_impressao = nm_municipio if nm_municipio else "— (eleição geral)"
candidato_impressao = f"{nm_candidato} ({sg_partido})"
st.markdown(
    '<div class="print-filter-summary">'
    f"{html.escape(str(ano))} | {html.escape(uf)} | "
    f"{html.escape(cidade_impressao)} | {html.escape(ds_cargo)} | "
    f"{html.escape(candidato_impressao)}"
    "</div>",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("### Cache")
    if st.button("Limpar cache", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache limpo. Recarregando…")
        st.rerun()
    if _usa_catalogo_filtros():
        st.caption("Filtros acelerados via `catalogo_boletim`.")
    else:
        st.caption(
            "Para filtros mais rápidos, rode "
            "`go run go_postgres/13_build_catalogo_filtros.go` após importar o boletim."
        )
    st.markdown("---")
    # st.caption("Banco: `eleicoes` em PostgreSQL local.")
    # st.caption("Tabela base: `boletim_de_urna` (granular por seção).")

ctx = {
    "ano": int(ano),
    "uf": uf,
    "cd_municipio": cd_municipio,
    "nm_municipio": nm_municipio,
    "cd_cargo": cd_cargo,
    "ds_cargo": ds_cargo,
    "nr_votavel": nr_votavel,
    "nm_candidato": nm_candidato,
    "municipal": municipal,
}

st.divider()

# ---------------------------------------------------------------------------
# Abas — renderiza só a seção ativa (st.tabs executaria todas de uma vez)
# ---------------------------------------------------------------------------
_TAB_RENDERERS: list[tuple[str, object]] = [
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
_tab_labels = [label for label, _ in _TAB_RENDERERS]
if "app_tab" not in st.session_state or st.session_state.app_tab not in _tab_labels:
    st.session_state.app_tab = _tab_labels[0]

with st.container(key="app-tab-nav"):
    if hasattr(st, "segmented_control"):
        _sel_tab = st.segmented_control(
            "Seção",
            _tab_labels,
            key="app_tab",
            label_visibility="collapsed",
        )
    else:
        _sel_tab = st.radio(
            "Seção",
            _tab_labels,
            horizontal=True,
            key="app_tab",
            label_visibility="collapsed",
        )

for _label, _module in _TAB_RENDERERS:
    if _label == _sel_tab:
        _module.render(ctx)
        break
