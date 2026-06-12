"""Filtros globais da aplicação (Eleição, UF, Cidade, Cargo, Candidato).

Os valores selecionados são espelhados na query string do navegador
(`?ano=…&uf=…&municipio=…&cargo=…&candidato=…`; a aba ativa usa `tab=…`
em ``ui/tabs.py``) para links compartilháveis.
"""
from __future__ import annotations

import html
from typing import Any

import streamlit as st

from components import SELECT_PLACEHOLDER
from db import is_municipal
from queries import _usa_catalogo_filtros
from queries import (
    listar_anos,
    listar_cargos,
    listar_candidatos,
    listar_municipios,
    listar_ufs,
)

_FILTER_MARKER = '<span class="global-filter-marker" aria-hidden="true"></span>'
_QP_KEYS = ("ano", "uf", "municipio", "cargo", "candidato")


def _qp_get(name: str) -> str | None:
    value = st.query_params.get(name)
    if value is None or value == "":
        return None
    return value


def _index_for(options: list[Any], value: Any | None, default: int = 0) -> int:
    if not options:
        return 0
    if value is None:
        return min(default, len(options) - 1)
    for i, opt in enumerate(options):
        if opt is None and value in (None, "", "null"):
            return i
        if str(opt) == str(value):
            return i
    return min(default, len(options) - 1)


def _build_filter_qp(
    ano: int,
    uf: str,
    cd_municipio: str | None,
    cd_cargo: str,
    nr_votavel: str,
) -> dict[str, str]:
    qp: dict[str, str] = {
        "ano": str(ano),
        "uf": uf,
        "cargo": str(cd_cargo),
        "candidato": str(nr_votavel),
    }
    if cd_municipio:
        qp["municipio"] = str(cd_municipio)
    return qp


def _sync_filter_query_params(
    ano: int,
    uf: str,
    cd_municipio: str | None,
    cd_cargo: str,
    nr_votavel: str,
) -> None:
    desired = _build_filter_qp(ano, uf, cd_municipio, cd_cargo, nr_votavel)
    current = {key: _qp_get(key) or "" for key in _QP_KEYS}
    desired_full = {key: desired.get(key, "") for key in _QP_KEYS}
    if current == desired_full:
        return

    preserved = {
        k: v
        for k, v in st.query_params.to_dict().items()
        if k not in _QP_KEYS
    }
    preserved.update(desired)
    st.query_params.from_dict(preserved)


def render() -> dict[str, Any]:
    """Renderiza filtros globais e retorna o contexto ``ctx`` para as abas."""
    anos = listar_anos()
    if not anos:
        st.error("Nenhuma eleição encontrada em `boletim_de_urna`. Importe os dados antes.")
        st.stop()

    qp_ano = _qp_get("ano")
    qp_uf = _qp_get("uf")
    qp_municipio = _qp_get("municipio")
    qp_cargo = _qp_get("cargo")
    qp_candidato = _qp_get("candidato")

    c1, c2, c3, c4, c5 = st.columns([1, 1, 1.4, 1.4, 1.6])
    with c1:
        st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
        ano = st.selectbox(
            "Eleição/Ano",
            anos,
            index=_index_for(anos, qp_ano, default=len(anos) - 1),
            placeholder=SELECT_PLACEHOLDER,
        )
    municipal = is_municipal(ano)

    ufs = listar_ufs(ano)
    with c2:
        st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
        uf = st.selectbox(
            "UF",
            ufs,
            index=_index_for(ufs, qp_uf, default=0),
            placeholder=SELECT_PLACEHOLDER,
        )

    with c3:
        municipios = listar_municipios(ano, uf)
        st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
        if municipal:
            if municipios.empty:
                st.warning("Sem municípios para o filtro.")
                st.stop()
            muni_codes = municipios["cd"].astype(str).tolist()
            muni_names = dict(zip(muni_codes, municipios["nm"], strict=False))
            cd_municipio = st.selectbox(
                "Cidade (obrigatório)",
                muni_codes,
                index=_index_for(muni_codes, qp_municipio, default=0),
                format_func=lambda cd: muni_names[cd],
                placeholder=SELECT_PLACEHOLDER,
            )
            nm_municipio = muni_names[cd_municipio]
        else:
            muni_codes = [None] + municipios["cd"].astype(str).tolist()
            muni_names = {None: "— (eleição geral)"}
            muni_names.update(
                dict(zip(municipios["cd"].astype(str), municipios["nm"], strict=False))
            )
            cd_municipio = st.selectbox(
                "Cidade (opcional em geral)",
                muni_codes,
                index=_index_for(muni_codes, qp_municipio, default=0),
                format_func=lambda cd: muni_names[cd],
                placeholder=SELECT_PLACEHOLDER,
            )
            nm_municipio = muni_names[cd_municipio] if cd_municipio else None

    cargos = listar_cargos(ano, uf, cd_municipio)
    with c4:
        st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
        if cargos.empty:
            st.warning("Sem cargos para o filtro.")
            st.stop()
        cargo_codes = cargos["cd"].astype(str).tolist()
        cargo_names = dict(zip(cargo_codes, cargos["ds"], strict=False))
        cd_cargo = st.selectbox(
            "Cargo",
            cargo_codes,
            index=_index_for(cargo_codes, qp_cargo, default=0),
            format_func=lambda cd: cargo_names[cd].title(),
            placeholder=SELECT_PLACEHOLDER,
        )
        ds_cargo = cargo_names[cd_cargo].title()

    cands = listar_candidatos(ano, uf, cd_municipio, cd_cargo)
    with c5:
        st.markdown(_FILTER_MARKER, unsafe_allow_html=True)
        if cands.empty:
            st.warning("Sem candidatos para o filtro.")
            st.stop()
        cand_numbers = cands["nr"].astype(str).tolist()
        cand_names = dict(zip(cand_numbers, cands["nm"], strict=False))
        cand_parties = dict(zip(cand_numbers, cands["sg_partido"], strict=False))
        nr_votavel = st.selectbox(
            "Candidato foco",
            cand_numbers,
            index=_index_for(cand_numbers, qp_candidato, default=0),
            format_func=lambda nr: f"{cand_names[nr]} ({cand_parties.get(nr) or '—'})",
            placeholder=SELECT_PLACEHOLDER,
        )
        nm_candidato = cand_names[nr_votavel]
        sg_partido = cand_parties.get(nr_votavel) or "—"

    _sync_filter_query_params(ano, uf, cd_municipio, cd_cargo, nr_votavel)

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

    return {
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
