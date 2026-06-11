from __future__ import annotations

import streamlit as st

from app.core import table_exists
from app.queries import (
    is_municipal,
    list_candidatos,
    list_cargos,
    list_municipios,
    list_ufs,
    list_years,
)
from app.ui import (
    hero,
    inject_theme,
    tab_bairros_locais,
    tab_card_local,
    tab_perfil_eleitorado,
    tab_ranking_municipio,
    tab_resumo_municipio,
    tab_sintese_territorial,
    tab_sumario,
    tab_votos_estado,
    tab_votos_municipio,
)

st.set_page_config(
    page_title="App Python Trae",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
inject_theme()
hero()

if not table_exists("boletim_de_urna"):
    st.error("A tabela `boletim_de_urna` nao foi encontrada no banco configurado.")
    st.stop()

anos = list_years()
if not anos:
    st.error("Nenhuma eleicao encontrada em `boletim_de_urna`.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns([1.2, 1, 1.4, 1.4, 1.8])
with c1:
    ano = st.selectbox("Eleicao/Ano", anos, index=len(anos) - 1)

municipal = is_municipal(ano)
ufs = list_ufs(ano)
with c2:
    uf = st.selectbox("Estado", ufs, index=0 if ufs else None)

municipios = list_municipios(ano, uf) if uf else None
with c3:
    if municipios is None or municipios.empty:
        st.selectbox("Cidade", ["Sem cidades disponiveis"], disabled=True)
        cd_municipio = None
        nm_municipio = None
    elif municipal:
        idx_municipio = st.selectbox(
            "Cidade",
            options=municipios.index,
            format_func=lambda i: municipios.loc[i, "nm"],
        )
        cd_municipio = str(municipios.loc[idx_municipio, "cd"])
        nm_municipio = str(municipios.loc[idx_municipio, "nm"])
    else:
        opcoes = [("__nenhuma__", "Todas as cidades / opcional")] + [
            (str(row.cd), str(row.nm)) for row in municipios.itertuples(index=False)
        ]
        escolha = st.selectbox("Cidade", opcoes, format_func=lambda item: item[1], index=0)
        cd_municipio = None if escolha[0] == "__nenhuma__" else escolha[0]
        nm_municipio = None if escolha[0] == "__nenhuma__" else escolha[1]

cargos = list_cargos(ano, uf, cd_municipio)
with c4:
    if cargos.empty:
        st.warning("Sem cargos para os filtros atuais.")
        st.stop()
    idx_cargo = st.selectbox("Cargo", options=cargos.index, format_func=lambda i: cargos.loc[i, "ds"])
    cd_cargo = str(cargos.loc[idx_cargo, "cd"])
    ds_cargo = str(cargos.loc[idx_cargo, "ds"])

candidatos = list_candidatos(ano, uf, cd_municipio, cd_cargo)
with c5:
    if candidatos.empty:
        st.warning("Sem candidatos para os filtros atuais.")
        st.stop()
    idx_candidato = st.selectbox(
        "Candidato foco",
        options=candidatos.index,
        format_func=lambda i: f"{candidatos.loc[i, 'nm']} ({candidatos.loc[i, 'partido'] or '-'})",
    )
    nr_votavel = str(candidatos.loc[idx_candidato, "nr"])
    nm_candidato = str(candidatos.loc[idx_candidato, "nm"])

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

with st.sidebar:
    st.markdown("### Contexto")
    st.caption(f"Ano: `{ano}`")
    st.caption(f"UF: `{uf}`")
    st.caption(f"Cidade: `{nm_municipio or 'todas / nao definida'}`")
    st.caption(f"Cargo: `{ds_cargo}`")
    st.caption(f"Candidato: `{nm_candidato}`")
    st.markdown("---")
    st.caption("Banco padrao: `postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes`")
    if st.button("Limpar cache", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

st.divider()
tabs = st.tabs(
    [
        "1. Sumario",
        "2. Resumo municipio",
        "3. Perfil eleitorado",
        "4. Votos no estado",
        "5. Votos no municipio",
        "6. Ranking municipio",
        "7. Sintese territorial",
        "8. Card local",
        "9. Bairros e locais",
    ]
)

with tabs[0]:
    tab_sumario(ctx)
with tabs[1]:
    tab_resumo_municipio(ctx)
with tabs[2]:
    tab_perfil_eleitorado(ctx)
with tabs[3]:
    tab_votos_estado(ctx)
with tabs[4]:
    tab_votos_municipio(ctx)
with tabs[5]:
    tab_ranking_municipio(ctx)
with tabs[6]:
    tab_sintese_territorial(ctx)
with tabs[7]:
    tab_card_local(ctx)
with tabs[8]:
    tab_bairros_locais(ctx)
