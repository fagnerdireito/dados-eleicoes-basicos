"""Streamlit app — Dossiê eleitoral.

Filtros globais (Eleição/Ano + UF + Cidade + Cargo + Candidato) ficam acima
das 9 abas. Cada aba é um módulo em ui/ que recebe um dict ``ctx`` com o
contexto selecionado pelo usuário.
"""
from __future__ import annotations

import streamlit as st

from db import is_municipal
from queries import (
    listar_anos,
    listar_cargos,
    listar_candidatos,
    listar_municipios,
    listar_ufs,
)
from ui import (
    tab_card_local,
    tab_perfil_eleitorado,
    tab_ranking_municipio,
    tab_resumo_municipio,
    tab_sintese_territorial,
    tab_sumario,
    tab_votos_bairro,
    tab_votos_estado,
    tab_votos_municipio,
)

st.set_page_config(
    page_title="Dossiê eleitoral",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.2rem; }
      h1, h2, h3 { color: #0b2545; }
      [data-testid="stMetricLabel"] { color: #5b6b80; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    "<div style='display:flex;align-items:center;gap:0.6rem;margin-bottom:0.6rem'>"
    "<div style='font-size:1.8rem'>🗳️</div>"
    "<div><div style='font-size:1.6rem;font-weight:800;color:#0b2545'>Dossiê eleitoral</div>"
    "<div style='color:#5b6b80'>Resultados consolidados a partir do boletim de urna do TSE.</div>"
    "</div></div>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Filtros globais
# ---------------------------------------------------------------------------
anos = listar_anos()
if not anos:
    st.error("Nenhuma eleição encontrada em `boletim_de_urna`. Importe os dados antes.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns([1, 1, 1.4, 1.4, 1.6])
with c1:
    ano = st.selectbox("Eleição/Ano", anos, index=len(anos) - 1)
municipal = is_municipal(ano)

ufs = listar_ufs(ano)
with c2:
    uf = st.selectbox("UF", ufs, index=0 if ufs else None)

with c3:
    municipios = listar_municipios(ano, uf)
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

with st.sidebar:
    st.markdown("### Cache")
    if st.button("Limpar cache", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache limpo. Recarregando…")
        st.rerun()
    st.markdown("---")
    st.caption("Banco: `eleicoes` em PostgreSQL local.")
    st.caption("Tabela base: `boletim_de_urna` (granular por seção).")

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
# Tabs (uma por imagem de referência)
# ---------------------------------------------------------------------------
labels = [
    "1 · Sumário",
    "2 · Resumo município",
    "3 · Perfil eleitorado (UF)",
    "4 · Votos no estado",
    "5 · Votos no município",
    "6 · Ranking município",
    "7 · Síntese territorial",
    "8 · Card local",
    "9 · Votos por bairro",
]
tabs = st.tabs(labels)
with tabs[0]: tab_sumario.render(ctx)
with tabs[1]: tab_resumo_municipio.render(ctx)
with tabs[2]: tab_perfil_eleitorado.render(ctx)
with tabs[3]: tab_votos_estado.render(ctx)
with tabs[4]: tab_votos_municipio.render(ctx)
with tabs[5]: tab_ranking_municipio.render(ctx)
with tabs[6]: tab_sintese_territorial.render(ctx)
with tabs[7]: tab_card_local.render(ctx)
with tabs[8]: tab_votos_bairro.render(ctx)
