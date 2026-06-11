from __future__ import annotations

import pandas as pd
import streamlit as st

from app.core import REFERENCE_IMAGES, SOURCE_IMAGES_DIR, fmt_int, fmt_pct
from app.queries import (
    candidate_breakdowns,
    list_locais,
    local_card,
    previous_cycle_year,
    profile_breakdowns_available,
    ranking_for_year,
    summary_context,
    territorial_synthesis,
    turnout_by_uf,
    votes_by_local,
    votes_by_municipio,
)


def inject_theme() -> None:
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.1rem; padding-bottom: 2rem; }
        .trae-hero { padding: 1rem 1.2rem; border: 1px solid #dbe7f3; border-radius: 16px; background: linear-gradient(180deg, #f9fbff 0%, #f2f7ff 100%); }
        .trae-muted { color: #5b6b80; }
        .trae-card { border: 1px solid #e5edf6; border-radius: 16px; padding: 1rem; background: #ffffff; min-height: 120px; }
        .trae-card-label { color: #5b6b80; font-size: 0.95rem; margin-bottom: 0.4rem; }
        .trae-card-value { font-size: 2rem; font-weight: 800; color: #0b2545; }
        .trae-card-caption { color: #425466; margin-top: 0.45rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def hero() -> None:
    st.markdown(
        """
        <div class='trae-hero'>
          <div style='font-size:1.85rem;font-weight:800;color:#0b2545'>App Python Trae</div>
          <div class='trae-muted' style='margin-top:0.25rem'>
            Dossie eleitoral em Streamlit, desacoplado, consumindo PostgreSQL e organizado em 9 abas conforme as imagens de referencia.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(title: str, subtitle: str | None = None) -> None:
    st.markdown(f"### {title}")
    if subtitle:
        st.caption(subtitle)


def metric_card(label: str, value: str, caption: str | None = None) -> None:
    html = [
        "<div class='trae-card'>",
        f"<div class='trae-card-label'>{label}</div>",
        f"<div class='trae-card-value'>{value}</div>",
    ]
    if caption:
        html.append(f"<div class='trae-card-caption'>{caption}</div>")
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def show_reference(index: int) -> None:
    path = SOURCE_IMAGES_DIR / REFERENCE_IMAGES[index]
    with st.expander("Referencia visual", expanded=False):
        if path.exists():
            st.image(str(path), use_container_width=True)
        else:
            st.info(f"Imagem nao encontrada em {path}")


def data_gap(message: str, items: list[str] | None = None) -> None:
    st.warning(message)
    if items:
        for item in items:
            st.markdown(f"- {item}")


def render_table(df: pd.DataFrame, rename: dict[str, str] | None = None) -> None:
    out = df.copy()
    if rename:
        out = out.rename(columns=rename)
    st.dataframe(out, use_container_width=True, hide_index=True)


def render_top_list(df: pd.DataFrame, label_col: str, value_col: str, title: str, limit: int = 10) -> None:
    st.markdown(f"#### {title}")
    if df.empty:
        st.info("Sem dados para exibir.")
        return
    for idx, row in enumerate(df.head(limit).itertuples(index=False), start=1):
        label = getattr(row, label_col)
        value = getattr(row, value_col)
        st.markdown(
            "<div style='display:flex;justify-content:space-between;padding:0.28rem 0;border-bottom:1px solid #eef1f5'>"
            f"<div><span style='color:#5b6b80'>{idx}.</span> {label}</div>"
            f"<div style='font-weight:700'>{fmt_int(value)}</div></div>",
            unsafe_allow_html=True,
        )


def tab_sumario(ctx: dict) -> None:
    section_title("Sumario", "Indice dinamico das 9 abas a partir dos filtros selecionados.")
    show_reference(0)

    items = [
        "Resumo no municipio",
        "Perfil do eleitorado",
        "Onde estao os votos no estado",
        "Onde estao os votos no municipio",
        "Ranking geral no municipio",
        "Sintese territorial",
        "Card do local de votacao",
        "Bairros e locais do candidato",
    ]
    left, right = st.columns(2)
    with left:
        for pos, item in enumerate(items[:4], start=2):
            st.markdown(f"**{pos}. {item}**")
    with right:
        for pos, item in enumerate(items[4:], start=6):
            st.markdown(f"**{pos}. {item}**")

    st.divider()
    st.markdown("#### Locais em destaque")
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para destacar os locais municipais.")
        return
    locais = list_locais(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
    if locais.empty:
        st.info("Sem locais para o recorte atual.")
        return
    for _, row in locais.head(6).iterrows():
        st.markdown(f"- {row['nm_local']}")


def tab_resumo_municipio(ctx: dict) -> None:
    section_title("Resumo no municipio", "Cards principais do candidato no municipio selecionado.")
    show_reference(1)
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para abrir o resumo municipal.")
        return

    resumo = summary_context(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], ctx["nr_votavel"])
    c1, c2 = st.columns(2)
    with c1:
        metric_card("Votacao do candidato", fmt_int(resumo["votos_candidato"]), f"{fmt_pct(resumo['pct_validos'])} dos votos validos.")
    with c2:
        posicao = "-" if resumo["posicao"] is None else f"{resumo['posicao']}o"
        metric_card("Posicao geral no municipio", posicao, f"Entre {fmt_int(resumo['total_candidatos'])} candidatos.")

    c3, c4 = st.columns(2)
    with c3:
        metric_card("Lideranca nos locais", fmt_int(resumo["liderancas"]), f"Ficou em 1o em {fmt_int(resumo['locais'])} locais.")
    with c4:
        metric_card("Locais analisados", fmt_int(resumo["locais"]), f"Cidade: {ctx['nm_municipio']}")

    st.divider()
    c5, c6, c7, c8, c9 = st.columns(5)
    with c5:
        metric_card("Validos", fmt_int(resumo["validos"]))
    with c6:
        metric_card("Brancos", fmt_int(resumo["brancos"]))
    with c7:
        metric_card("Nulos", fmt_int(resumo["nulos"]))
    with c8:
        metric_card("Abstencoes", fmt_int(resumo["abstencoes"]))
    with c9:
        metric_card("Comparecimento", fmt_pct(resumo["pct_comparecimento"], 0), fmt_int(resumo["comparecimento"]))


def tab_perfil_eleitorado(ctx: dict) -> None:
    section_title("Perfil do eleitorado", "Comparecimento e abstencao na UF; recortes detalhados entram como placeholder se faltarem bases.")
    show_reference(2)

    resumo = turnout_by_uf(ctx["ano"], ctx["uf"])
    c1, c2 = st.columns(2)
    with c1:
        metric_card("Comparecimento (UF)", fmt_pct(resumo["pct_comparecimento"]), f"{fmt_int(resumo['comparecimento'])} eleitores.")
    with c2:
        metric_card("Abstencao (UF)", fmt_pct(resumo["pct_abstencao"]), f"{fmt_int(resumo['abstencoes'])} eleitores.")

    st.divider()
    st.markdown("#### Faixa etaria e escolaridade")
    if not profile_breakdowns_available():
        data_gap(
            "Nao ha base importada no banco para reproduzir faixa etaria e escolaridade nesta tela.",
            [
                "Eleitorado - <ANO>",
                "Perfil do eleitorado por secao eleitoral - <ANO>",
                "Comparecimento e Abstencao - <ANO>",
            ],
        )
    placeholder_age = pd.DataFrame(
        [
            {"Faixa etaria": "16 anos", "Status": "placeholder ate importar base do TSE"},
            {"Faixa etaria": "17 anos", "Status": "placeholder ate importar base do TSE"},
            {"Faixa etaria": "18 anos", "Status": "placeholder ate importar base do TSE"},
        ]
    )
    placeholder_school = pd.DataFrame(
        [
            {"Escolaridade": "Analfabeto", "Status": "placeholder ate importar base do TSE"},
            {"Escolaridade": "Le e escreve", "Status": "placeholder ate importar base do TSE"},
            {"Escolaridade": "Ensino medio completo", "Status": "placeholder ate importar base do TSE"},
        ]
    )
    left, right = st.columns(2)
    with left:
        render_table(placeholder_age)
    with right:
        render_table(placeholder_school)


def tab_votos_estado(ctx: dict) -> None:
    section_title("Onde estao os votos no estado", "Ranking por municipio na UF selecionada.")
    show_reference(3)
    df = votes_by_municipio(ctx["ano"], ctx["uf"], ctx["cd_cargo"], ctx["nr_votavel"])
    if df.empty:
        st.info("Sem votos do candidato no recorte atual.")
        return
    left, right = st.columns([1.6, 1])
    with left:
        data_gap(
            "O ranking por municipio esta funcional, mas o mapa coropletico depende de uma malha geografica municipal dentro de `app-python-trae/app/assets`."
        )
        render_table(df.head(20), {"nm": "Municipio", "votos": "Votos"})
    with right:
        render_top_list(df, "nm", "votos", "Top municipios", limit=30)


def tab_votos_municipio(ctx: dict) -> None:
    section_title("Onde estao os votos no municipio", "Mapa de pontos por local de votacao quando houver coordenadas.")
    show_reference(4)
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para abrir o mapa municipal.")
        return
    df = votes_by_local(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], ctx["nr_votavel"])
    if df.empty:
        data_gap("Nao ha base de locais georreferenciados suficiente para esta combinacao de filtros.")
        return
    df_map = df.dropna(subset=["lat", "lng"]).copy()
    left, right = st.columns([1.5, 1])
    with left:
        if df_map.empty:
            st.info("Sem latitude/longitude valida para montar o mapa.")
        else:
            st.map(df_map.rename(columns={"lat": "latitude", "lng": "longitude"}), size="votos", color="#1f6feb", use_container_width=True)
            st.caption("Cada ponto representa um local de votacao com tamanho proporcional aos votos.")
    with right:
        render_top_list(df, "nm_local", "votos", "Top locais", limit=20)


def tab_ranking_municipio(ctx: dict) -> None:
    section_title("Ranking geral no municipio", "Comparativo do top 10 entre o ano filtrado e o ciclo anterior.")
    show_reference(5)
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para comparar o ranking municipal.")
        return
    prev_year = previous_cycle_year(ctx["ano"])
    current = ranking_for_year(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
    previous = (
        ranking_for_year(prev_year, ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
        if prev_year
        else pd.DataFrame()
    )
    left, right = st.columns(2)
    with left:
        st.markdown(f"#### {prev_year or 'Sem ano anterior'}")
        if previous.empty:
            st.info("Sem dados para o ciclo anterior.")
        else:
            previous["pct"] = previous["pct"].apply(fmt_pct)
            render_table(previous, {"nm": "Candidato", "partido": "Partido", "votos": "Votos", "pct": "%"})
    with right:
        st.markdown(f"#### {ctx['ano']}")
        if current.empty:
            st.info("Sem ranking para o ano atual.")
        else:
            current["pct"] = current["pct"].apply(fmt_pct)
            render_table(current, {"nm": "Candidato", "partido": "Partido", "votos": "Votos", "pct": "%"})


def tab_sintese_territorial(ctx: dict) -> None:
    section_title("Sintese territorial", "Quantos locais cada candidato lidera no municipio selecionado.")
    show_reference(6)
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para abrir a sintese territorial.")
        return
    df = territorial_synthesis(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
    if df.empty:
        st.info("Sem sintese territorial para o filtro atual.")
        return
    render_table(df.head(20), {"nm": "Candidato", "partido": "Partido", "locais": "Locais liderados"})


def tab_card_local(ctx: dict) -> None:
    section_title("Card do local de votacao", "Top 10 do local escolhido dentro da cidade filtrada.")
    show_reference(7)
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para abrir o card do local.")
        return
    locais = list_locais(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"])
    if locais.empty:
        st.info("Nao foram encontrados locais para este municipio.")
        return
    idx = st.selectbox(
        "Local de votacao",
        options=locais.index,
        format_func=lambda i: f"{locais.loc[i, 'nm_local']} ({locais.loc[i, 'nr_local']})",
    )
    nr_local = str(locais.loc[idx, "nr_local"])
    nm_local = str(locais.loc[idx, "nm_local"])
    dados = local_card(ctx["ano"], ctx["uf"], ctx["cd_municipio"], ctx["cd_cargo"], nr_local, ctx["nr_votavel"])
    st.markdown(f"#### {nm_local}")
    st.caption(
        f"Top 10 no local. {fmt_int(dados['validos'])} votos validos, {fmt_int(dados['brancos'])} em branco, {fmt_int(dados['nulos'])} nulos."
    )
    ranking = dados["ranking"].copy()
    if ranking.empty:
        st.info("Sem ranking para o local selecionado.")
        return
    ranking["pct"] = ranking["pct"].apply(fmt_pct)
    render_table(ranking, {"nm": "Candidato", "partido": "Partido", "votos": "Votos", "pct": "%", "pos": "Posicao"})
    destaque = dados["destaque"]
    if destaque:
        st.success(
            f"Desempenho de {ctx['nm_candidato']}: {fmt_int(destaque['votos'])} votos, posicao {int(destaque['pos'])} e {fmt_pct(destaque['pct'])} dos validos do local."
        )


def tab_bairros_locais(ctx: dict) -> None:
    section_title("Bairros e locais do candidato", "Quebra do candidato por municipio, bairro e local de votacao.")
    show_reference(8)
    if not ctx["cd_municipio"]:
        st.info("Selecione uma cidade para abrir a quebra por bairro e local.")
        return
    dados = candidate_breakdowns(
        ctx["ano"],
        ctx["uf"],
        ctx["cd_municipio"],
        ctx["cd_cargo"],
        ctx["nr_votavel"],
        ctx["nm_municipio"],
    )
    st.markdown("#### Municipio")
    render_table(dados["municipio"], {"municipio": "Municipio", "ano": "Ano", "votos": "Votos"})
    left, right = st.columns(2)
    with left:
        st.markdown("#### Bairro")
        if dados["bairro"].empty:
            data_gap("Nao ha vinculacao de bairro para este recorte.")
        else:
            render_table(dados["bairro"], {"bairro": "Bairro", "ano": "Ano", "votos": "Votos", "nm_votavel": "Candidato"})
    with right:
        st.markdown("#### Local")
        if dados["local"].empty:
            data_gap("Nao ha nomes de local de votacao suficientes para esta consulta.")
        else:
            render_table(dados["local"], {"local": "Local", "ano": "Ano", "votos": "Votos", "nm_votavel": "Candidato"})
