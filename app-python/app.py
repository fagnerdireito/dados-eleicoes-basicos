"""Streamlit app — Dados Eleitorais.

Filtros globais (Eleição/Ano + UF + Cidade + Cargo + Candidato) ficam acima
das 9 abas. Cada aba é um módulo em ui/ que recebe um dict ``ctx`` com o
contexto selecionado pelo usuário.
"""
from __future__ import annotations

import base64
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from ui import filtros, tabs

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
      .st-key-app-tab-nav [data-testid="stSegmentedControl"],
      .st-key-app-tab-nav [data-testid="stButtonGroup"] {
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
        .st-key-app-tab-nav,
        .element-container:has(.st-key-app-tab-nav) {
          display: none !important;
          height: 0 !important;
          margin: 0 !important;
          padding: 0 !important;
          overflow: hidden !important;
        }
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

        /* Abas independentes no mobile — cartões separados, não colados */
        .st-key-app-tab-nav [data-testid="stRadio"],
        .st-key-app-tab-nav [data-testid="stSegmentedControl"],
        .st-key-app-tab-nav [data-testid="stButtonGroup"] {
          background: transparent;
          border: none;
          padding: 0;
        }
        .st-key-app-tab-nav [data-testid="stRadio"] > div,
        .st-key-app-tab-nav [data-testid="stButtonGroup"] {
          flex-wrap: wrap;
          gap: 0.5rem;
        }
        .st-key-app-tab-nav [data-testid="stRadio"] label,
        .st-key-app-tab-nav [data-testid="stBaseButton-segmented_control"],
        .st-key-app-tab-nav [data-testid="stBaseButton-segmented_controlActive"] {
          flex: 1 1 calc(50% - 0.25rem);
          min-width: calc(50% - 0.25rem);
          margin: 0 !important;
          background: white;
          border-radius: 8px !important;
          border: 1px solid #e2e8f0 !important;
          box-shadow: none;
          white-space: normal;
          line-height: 1.25;
          text-align: center;
        }
        .st-key-app-tab-nav [data-testid="stRadio"] label:has(input:checked),
        .st-key-app-tab-nav [data-testid="stBaseButton-segmented_controlActive"] {
          border-color: #1f6feb !important;
          box-shadow: inset 0 -2px 0 #1f6feb;
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


ctx = filtros.render()

st.divider()

tabs.render(ctx)
