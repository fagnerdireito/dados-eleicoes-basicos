from __future__ import annotations

import streamlit as st

from components import fmt_int, fmt_pct, kpi, section_title
from queries import resumo_candidato_municipio, votos_candidato_por_municipio

_COMPOSICAO_TITLE = "1.215rem"    # ~1.5rem −19%
_COMPOSICAO_LABEL = "0.70875rem"  # ~0.875rem −19%
_COMPOSICAO_VALUE = "1.8225rem"   # ~2.25rem −19%


def _composicao_votos(d: dict) -> None:
    st.markdown(
        f"<div style='font-size:{_COMPOSICAO_TITLE};font-weight:700;color:#0b2545;"
        "margin:0.4rem 0 0.75rem'>Composição dos votos no município</div>",
        unsafe_allow_html=True,
    )
    items = [
        ("Válidos (nominais + legenda)", fmt_int(d["validos"])),
        ("Brancos", fmt_int(d["brancos"])),
        ("Nulos", fmt_int(d["nulos"])),
        ("Abstenções", fmt_int(d["abstenc"])),
        ("Comparecimento", fmt_pct(d["pct_comparec"], casas=0)),
    ]
    cols = st.columns(5)
    for col, (label, value) in zip(cols, items, strict=True):
        col.markdown(
            f"<div style='color:#5b6b80;font-size:{_COMPOSICAO_LABEL}'>{label}</div>"
            f"<div style='color:#0b2545;font-size:{_COMPOSICAO_VALUE};"
            f"font-weight:600;line-height:1.2'>{value}</div>",
            unsafe_allow_html=True,
        )


def _render_municipio(
    nm_municipio: str,
    uf: str,
    ano: int,
    ds_cargo: str,
    nm_candidato: str,
    cd_municipio: str,
    cd_cargo: str,
    nr_votavel: str,
) -> None:
    section_title(
        f"{nm_municipio} ({uf})",
        f"Resumo de {nm_candidato} no município · {ano} · {ds_cargo}",
    )

    d = resumo_candidato_municipio(ano, uf, cd_municipio, cd_cargo, nr_votavel)

    c1, c2 = st.columns(2)
    with c1:
        kpi(
            "Votação do candidato",
            fmt_int(d["votos_cand"]),
            f"{fmt_pct(d['pct_validos'])} dos {fmt_int(d['validos'])} votos válidos do município.",
        )
    with c2:
        kpi(
            "Posição geral no município",
            f"{d['posicao'] or '—'}º" if d["posicao"] else "—",
            f"Classificação entre {fmt_int(d['total_cands'])} candidatos ao mesmo cargo.",
        )

    c3, c4 = st.columns(2)
    with c3:
        kpi(
            "Liderança nos locais",
            fmt_int(d["lideres"]),
            f"locais onde ficou em 1º, de {fmt_int(d['total_locais'])} analisados.",
        )
    with c4:
        kpi(
            "Locais de votação analisados",
            fmt_int(d["total_locais"]),
            f"Comparecimento de {fmt_pct(d['pct_comparec'])} ({fmt_int(d['comparec'])}/{fmt_int(d['aptos'])}).",
        )

    _composicao_votos(d)


def render(ctx: dict) -> None:
    municipios = votos_candidato_por_municipio(
        ctx["ano"],
        ctx["uf"],
        ctx["cd_cargo"],
        ctx["nr_votavel"],
    )
    municipios = municipios[municipios["votos"] > 0]
    if municipios.empty:
        st.info("Nenhum município com votação do candidato na UF selecionada.")
        return

    section_title(
        "Resumo por município",
        f"Municípios com votação na UF · {ctx['nm_candidato']} · {ctx['ano']} · {ctx['ds_cargo']}",
    )

    for i, row in enumerate(municipios.itertuples(index=False)):
        if i > 0:
            st.divider()
        _render_municipio(
            row.nm,
            ctx["uf"],
            ctx["ano"],
            ctx["ds_cargo"],
            ctx["nm_candidato"],
            row.cd,
            ctx["cd_cargo"],
            ctx["nr_votavel"],
        )
