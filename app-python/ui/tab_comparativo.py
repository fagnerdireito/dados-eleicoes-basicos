"""Aba comparativo — até 4 candidatos lado a lado por zona, bairro ou seção."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from components import fmt_int, fmt_pct, section_title
from db import table_exists
from queries import comparativo_votos_territorio, listar_candidatos

_CAND_COLORS = ("#1f6feb", "#ef4444", "#22c55e", "#f59e0b")
_DIM_LABELS = {
    "zona": "Zona",
    "bairro": "Bairro",
    "secao": "Seção",
    "local": "Local de votação",
}
# Dimensões que dependem de `local_votacao` (só 2024 hoje).
_DIM_PRECISA_LOCAL_VOTACAO = {"bairro", "local"}


def _cand_labels(cands: pd.DataFrame) -> dict[str, str]:
    return {
        str(row.nr): f"{row.nm} ({row.sg_partido or '—'})"
        for row in cands.itertuples(index=False)
    }


def _short_name(label: str, max_len: int = 22) -> str:
    return label if len(label) <= max_len else label[: max_len - 1] + "…"


def _pivot_comparativo(df: pd.DataFrame, nrs: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows: list[dict] = []
    for territorio, grp in df.groupby("territorio", sort=False):
        total = int(grp["votos"].sum())
        row: dict = {"territorio": territorio, "_total": total}
        for nr in nrs:
            match = grp[grp["nr"].astype(str) == str(nr)]
            votos = int(match["votos"].iloc[0]) if not match.empty else 0
            row[f"{nr}_votos"] = votos
            row[f"{nr}_pct"] = (votos / total * 100) if total else 0.0
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    sort_col = f"{nrs[0]}_votos"
    return out.sort_values(sort_col, ascending=False).drop(columns="_total")


def _render_cards(nrs: list[str], cands: pd.DataFrame, labels: dict[str, str]) -> None:
    cols = st.columns(len(nrs))
    for i, nr in enumerate(nrs):
        info = cands[cands["nr"].astype(str) == str(nr)]
        if info.empty:
            continue
        row = info.iloc[0]
        color = _CAND_COLORS[i % len(_CAND_COLORS)]
        with cols[i]:
            st.markdown(
                f"<div style='border:2px solid {color};border-radius:10px;padding:0.75rem;"
                f"background:#fafbfd;text-align:center'>"
                f"<div style='font-weight:700;color:#0b2545;font-size:0.95rem'>"
                f"{row['nm']}</div>"
                f"<div style='color:#5b6b80;font-size:0.8rem;margin:0.15rem 0'>"
                f"{row['sg_partido'] or '—'}</div>"
                f"<div style='font-size:1.5rem;font-weight:800;color:{color}'>"
                f"{fmt_int(int(row['votos']))}</div>"
                f"<div style='color:#5b6b80;font-size:0.75rem'>votos no município</div>"
                f"</div>",
                unsafe_allow_html=True,
            )


def _render_tabela(
    df_long: pd.DataFrame,
    nrs: list[str],
    labels: dict[str, str],
    dim_label: str,
) -> None:
    wide = _pivot_comparativo(df_long, nrs)
    if wide.empty:
        st.info(f"Sem dados por {dim_label.lower()} para os candidatos selecionados.")
        return

    head = (
        "<tr style='background:#f6f8fa;border-bottom:2px solid #eef1f5'>"
        f"<th style='text-align:left;padding:0.55rem 0.75rem;color:#5b6b80;font-size:0.8rem'>"
        f"{dim_label.upper()}</th>"
    )
    for i, nr in enumerate(nrs):
        color = _CAND_COLORS[i % len(_CAND_COLORS)]
        head += (
            f"<th style='text-align:right;padding:0.55rem 0.75rem;color:{color};"
            f"font-size:0.8rem'>{_short_name(labels.get(nr, nr))}</th>"
        )
    head += "</tr>"

    body = ""
    for r in wide.itertuples(index=False):
        body += (
            "<tr style='border-bottom:1px solid #eef1f5'>"
            f"<td style='padding:0.55rem 0.75rem;font-weight:600;color:#0b2545'>"
            f"{r.territorio}</td>"
        )
        for i, nr in enumerate(nrs):
            color = _CAND_COLORS[i % len(_CAND_COLORS)]
            votos = getattr(r, f"{nr}_votos", 0)
            pct = getattr(r, f"{nr}_pct", 0.0)
            body += (
                f"<td style='padding:0.55rem 0.75rem;text-align:right'>"
                f"<div style='font-weight:700;color:{color}'>{fmt_int(votos)}</div>"
                f"<div style='font-size:0.8rem;color:#5b6b80'>{fmt_pct(pct)}</div>"
                f"</td>"
            )
        body += "</tr>"

    st.markdown(
        f"<div style='overflow-x:auto;border:1px solid #eef1f5;border-radius:8px'>"
        f"<table style='width:100%;border-collapse:collapse;font-size:0.9rem'>"
        f"<thead>{head}</thead><tbody>{body}</tbody></table></div>",
        unsafe_allow_html=True,
    )


def _render_dimensao(
    ctx: dict,
    nrs: tuple[str, ...],
    labels: dict[str, str],
    dimensao: str,
) -> None:
    dim_label = _DIM_LABELS[dimensao]
    if dimensao in _DIM_PRECISA_LOCAL_VOTACAO and (
        ctx["ano"] != 2024 or not table_exists("local_votacao")
    ):
        st.warning(
            f"Dados de {dim_label.lower()} exigem a tabela `local_votacao` "
            "(hoje carregada para **2024**). Importe "
            "`eleitorado_local_votacao_<ANO>.zip` do TSE para outros anos."
        )
        return

    df = comparativo_votos_territorio(
        ctx["ano"],
        ctx["uf"],
        ctx["cd_municipio"],
        ctx["cd_cargo"],
        nrs,
        dimensao,
    )
    _render_tabela(df, list(nrs), labels, dim_label)


def render(ctx: dict) -> None:
    if not ctx["cd_municipio"]:
        st.info("Selecione um município nos filtros globais para comparar candidatos.")
        return

    section_title(
        "Resultado comparativo",
        f"{ctx['nm_municipio']} · {ctx['ds_cargo']} · {ctx['ano']}",
    )

    cands = listar_candidatos(
        ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], limit=500
    )
    if cands.empty:
        st.info("Sem candidatos para o filtro atual.")
        return

    labels = _cand_labels(cands)
    options = cands["nr"].astype(str).tolist()
    default_nr = str(ctx["nr_votavel"])
    default = [default_nr] if default_nr in options else []

    with st.container(border=True):
        st.markdown(
            "<div style='font-size:0.75rem;font-weight:700;color:#5b6b80;"
            "letter-spacing:0.04em;margin-bottom:0.35rem'>RESULTADO COMPARATIVO</div>",
            unsafe_allow_html=True,
        )
        selecionados = st.multiselect(
            "Candidatos (até 4)",
            options=options,
            default=default,
            format_func=lambda nr: labels.get(str(nr), str(nr)),
            placeholder="Escolha de 1 a 4 candidatos…",
        )
        if len(selecionados) > 4:
            st.warning("Selecione no máximo 4 candidatos.")
            selecionados = selecionados[:4]

    if not selecionados:
        st.caption("Escolha ao menos um candidato para ver a comparação.")
        return

    nrs = tuple(str(nr) for nr in selecionados)
    _render_cards(list(nrs), cands, labels)

    st.markdown("")
    tab_zona, tab_bairro, tab_local, tab_secao = st.tabs(
        ["Zona", "Bairro", "Locais de votação", "Seção"]
    )
    with tab_zona:
        _render_dimensao(ctx, nrs, labels, "zona")
    with tab_bairro:
        _render_dimensao(ctx, nrs, labels, "bairro")
    with tab_local:
        _render_dimensao(ctx, nrs, labels, "local")
    with tab_secao:
        _render_dimensao(ctx, nrs, labels, "secao")
