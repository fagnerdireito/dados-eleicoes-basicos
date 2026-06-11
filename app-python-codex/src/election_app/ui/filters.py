"""Filtros encadeados que bloqueiam a exibicao ate o usuario aplicar o recorte."""

from __future__ import annotations

import streamlit as st

from election_app.models import ElectionOption, ElectionScope
from election_app.repository import (
    list_candidates,
    list_elections,
    list_municipalities,
    list_offices,
    list_ufs,
)


def render_filters() -> ElectionScope | None:
    elections_df = list_elections()
    if elections_df.empty:
        st.error("A tabela boletim_de_urna nao possui eleicoes carregadas.")
        return None

    elections = [ElectionOption.from_mapping(row) for row in elections_df.to_dict("records")]
    by_key = {item.key: item for item in elections}

    st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
    row1 = st.columns([2.4, 1, 1.8])
    with row1[0]:
        election_key = st.selectbox(
            "Eleicao / ano / turno",
            options=list(by_key),
            format_func=lambda key: by_key[key].label,
            key="filter_election",
        )
    election = by_key[election_key]
    ufs = list_ufs(election.year, election.election_code, election.round_number)
    with row1[1]:
        uf = st.selectbox("Estado", options=ufs, key="filter_uf")

    municipalities = list_municipalities(election.year, election.election_code, election.round_number, uf)
    municipality_options: list[str | None] = [None] + municipalities["code"].astype(str).tolist()
    municipality_names = dict(zip(municipalities["code"].astype(str), municipalities["name"]))
    with row1[2]:
        municipality_code = st.selectbox(
            "Cidade (obrigatoria em eleicao municipal)" if election.is_municipal else "Cidade (opcional)",
            options=municipality_options,
            format_func=lambda code: "Todos os municipios" if code is None else municipality_names.get(code, code),
            key="filter_municipality",
        )

    if election.is_municipal and municipality_code is None:
        st.warning(
            "Selecione uma cidade para identificar corretamente candidatos municipais. "
            "Numeros de prefeito e vereador se repetem em municipios diferentes."
        )
        st.markdown("</div>", unsafe_allow_html=True)
        return None

    offices = list_offices(
        election.year,
        election.election_code,
        election.round_number,
        uf,
        municipality_code,
    )
    if offices.empty:
        st.warning("Nenhum cargo encontrado para os filtros selecionados.")
        st.markdown("</div>", unsafe_allow_html=True)
        return None
    office_names = dict(zip(offices["code"].astype(str), offices["name"]))
    row2 = st.columns([1.5, 2.2, 1])
    with row2[0]:
        office_code = st.selectbox(
            "Cargo",
            options=offices["code"].astype(str).tolist(),
            format_func=lambda code: str(office_names[code]).title(),
            key="filter_office",
        )

    candidates = list_candidates(
        election.year,
        election.election_code,
        election.round_number,
        uf,
        municipality_code,
        office_code,
    )
    if candidates.empty:
        st.warning("Nenhum voto nominal encontrado para este cargo.")
        st.markdown("</div>", unsafe_allow_html=True)
        return None
    candidate_names = {
        str(row.number): f"{row.name} ({row.party or '-'})"
        for row in candidates.itertuples(index=False)
    }
    with row2[1]:
        candidate_number = st.selectbox(
            "Candidato em foco",
            options=candidates["number"].astype(str).tolist(),
            format_func=lambda number: candidate_names[number],
            key="filter_candidate",
        )
    with row2[2]:
        st.write("")
        applied = st.button("Exibir dados", type="primary", width="stretch")
    st.markdown("</div>", unsafe_allow_html=True)

    selected_candidate = candidates[candidates["number"].astype(str) == candidate_number].iloc[0]
    pending_scope = ElectionScope(
        election=election,
        uf=uf,
        municipality_code=municipality_code,
        municipality_name=municipality_names.get(municipality_code) if municipality_code else None,
        office_code=office_code,
        office_name=str(office_names[office_code]),
        candidate_number=candidate_number,
        candidate_name=str(selected_candidate["name"]),
        candidate_party=str(selected_candidate["party"] or "-"),
    )

    if applied:
        st.session_state["active_scope"] = pending_scope

    active_scope = st.session_state.get("active_scope")
    if active_scope is None:
        st.info("Defina o recorte eleitoral e clique em Exibir dados antes de abrir as analises.")
        return None
    if active_scope != pending_scope:
        st.warning("Os filtros foram alterados. Clique em Exibir dados para aplicar o novo recorte.")
    return active_scope
