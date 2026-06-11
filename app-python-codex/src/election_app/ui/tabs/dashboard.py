"""Nove abas que reproduzem as referencias fornecidas pelo usuario."""

from __future__ import annotations

import html

import pandas as pd
import plotly.express as px
import streamlit as st

from election_app.models import ElectionScope
from election_app.repository import (
    electorate_profile,
    geographic_detail,
    location_options,
    location_result,
    municipal_summary,
    previous_cycle_year,
    ranking,
    state_turnout,
    territorial_leaders,
    votes_by_location,
    votes_by_municipality,
)
from election_app.ui.components import (
    candidate_table,
    fmt_int,
    fmt_pct,
    kpi,
    location_map,
    placeholder,
    section_title,
    state_choropleth,
)


TAB_LABELS = [
    "1. Sumario",
    "2. Visao municipal",
    "3. Perfil da UF",
    "4. Distribuicao estadual",
    "5. Distribuicao por local",
    "6. Ranking historico",
    "7. Lideranca territorial",
    "8. Resultado do local",
    "9. Detalhamento geografico",
]


def _requires_municipality(scope: ElectionScope) -> bool:
    if scope.has_municipality:
        return False
    placeholder(
        "Selecione uma cidade",
        "Esta aba tem granularidade municipal. Volte aos filtros, escolha uma cidade e aplique o recorte.",
    )
    return True


def render_sumario(scope: ElectionScope) -> None:
    section_title("Sumario", "Indice das 9 secoes do dossie eleitoral")
    items = [
        ("01", "Sumario", "Navegacao e cobertura do relatorio"),
        ("02", "Visao municipal", "Desempenho consolidado do candidato"),
        ("03", "Perfil da UF", "Comparecimento e composicao do eleitorado"),
        ("04", "Distribuicao estadual", "Votos por municipio no estado"),
        ("05", "Distribuicao por local", "Votos nos locais de votacao"),
        ("06", "Ranking historico", "Top 10 e comparacao com ciclo anterior"),
        ("07", "Lideranca territorial", "Locais liderados por candidato"),
        ("08", "Resultado do local", "Top 10 em um local de votacao"),
        ("09", "Detalhamento geografico", "Municipio, bairro e local"),
    ]
    left, right = st.columns(2)
    for index, (number, title, description) in enumerate(items):
        column = left if index % 2 == 0 else right
        with column:
            st.markdown(
                f"""
                <div class="candidate-row">
                  <div class="candidate-rank">{number}</div>
                  <div></div>
                  <div class="candidate-name"><strong>{html.escape(title)}</strong><span>{html.escape(description)}</span></div>
                  <div></div><div></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.caption(
        f"Recorte aplicado: {scope.election.label}; {scope.uf}; "
        f"{scope.municipality_name or 'todos os municipios'}; {scope.office_name}; {scope.candidate_name}."
    )


def render_municipal_overview(scope: ElectionScope) -> None:
    if _requires_municipality(scope):
        return
    section_title(
        f"{scope.municipality_name} ({scope.uf})",
        f"Resumo de {scope.candidate_name} no municipio - {scope.election.year}",
    )
    data = municipal_summary(scope)
    if not data:
        placeholder("Sem resultado municipal", "Nao foram encontrados votos para o recorte selecionado.")
        return
    row1 = st.columns(2)
    with row1[0]:
        kpi(
            "Votacao do candidato",
            fmt_int(data["candidate_votes"]),
            f"{fmt_pct(data['vote_share'])} dos {fmt_int(data['valid_votes'])} votos validos.",
            accent=True,
        )
    with row1[1]:
        kpi("Posicao geral no municipio", f"{int(data['position'])}o", "Classificacao entre os candidatos nominais.")
    row2 = st.columns(2)
    with row2[0]:
        kpi(
            "Lideranca nos locais",
            fmt_int(data["led_locations"]),
            f"Locais em que ficou em primeiro, de {fmt_int(data['total_locations'])} analisados.",
        )
    with row2[1]:
        kpi("Locais analisados", fmt_int(data["total_locations"]), "Empates sao resolvidos pelo menor numero do candidato.")
    st.markdown("#### Composicao dos votos no municipio")
    stats = st.columns(5)
    values = [
        ("Validos", data["valid_votes"]),
        ("Brancos", data["blank_votes"]),
        ("Nulos", data["null_votes"]),
        ("Abstencoes", data["abstentions"]),
        ("Comparecimento", fmt_pct(data["attendance_rate"])),
    ]
    for column, (label, value) in zip(stats, values):
        with column:
            st.metric(label, value if isinstance(value, str) else fmt_int(value))


def render_state_profile(scope: ElectionScope, capabilities: dict[str, bool]) -> None:
    section_title(
        f"Perfil do eleitorado (estado {scope.uf})",
        "Comparecimento do pleito e composicao cadastral do eleitorado",
    )
    turnout = state_turnout(scope)
    left, center, right = st.columns([1, 1.5, 1.5])
    with left:
        kpi("Comparecimento (UF)", fmt_pct(turnout["attendance_rate"]), f"{fmt_int(turnout['attendance'])} eleitores", accent=True)
        st.write("")
        kpi("Abstencao (UF)", fmt_pct(turnout["abstention_rate"]), f"{fmt_int(turnout['abstentions'])} eleitores")
    if not capabilities.get("electorate_profile"):
        with center:
            placeholder(
                "Perfil demografico indisponivel",
                "O banco nao possui perfil_eleitorado para este ano e UF.",
                f"perfil_eleitorado_{scope.election.year}.csv",
            )
        return
    ages, education = electorate_profile(scope)
    with center:
        st.markdown("#### Eleitorado por faixa etaria")
        age_plot = ages.copy()
        total_age = age_plot["voters"].sum()
        age_plot["percentage"] = age_plot["voters"] / total_age * 100 if total_age else 0
        fig = px.bar(age_plot, x="percentage", y="label", orientation="h", text_auto=".1f")
        fig.update_traces(marker_color="#0b73c9", texttemplate="%{x:.1f}%")
        fig.update_layout(height=600, margin=dict(l=0, r=10, t=10, b=10), xaxis_visible=False, yaxis_title=None)
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    with right:
        st.markdown("#### Eleitorado por escolaridade")
        edu_plot = education.copy()
        total_education = edu_plot["voters"].sum()
        edu_plot["percentage"] = edu_plot["voters"] / total_education * 100 if total_education else 0
        fig = px.bar(edu_plot, x="percentage", y="label", orientation="h", text_auto=".1f")
        fig.update_traces(marker_color="#0b73c9", texttemplate="%{x:.1f}%")
        fig.update_layout(height=460, margin=dict(l=0, r=10, t=10, b=10), xaxis_visible=False, yaxis_title=None)
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    st.info(
        "O banco atual permite calcular comparecimento total por UF. As barras demograficas mostram a composicao do cadastro eleitoral, "
        "nao a abstencao por idade/escolaridade. Para esse recorte e necessario importar Comparecimento e Abstencao por perfil do TSE."
    )


def render_state_distribution(scope: ElectionScope) -> None:
    section_title(
        "Onde estao os votos no estado",
        f"Votacao de {scope.candidate_name} por municipio - {scope.uf}",
    )
    if scope.election.is_municipal:
        placeholder(
            "Distribuicao estadual nao se aplica ao candidato municipal",
            "Prefeito e vereador pertencem a um unico municipio e seus numeros se repetem no estado. "
            "Esta aba e preenchida para cargos estaduais ou federais.",
        )
        return
    df = votes_by_municipality(scope)
    if df.empty:
        placeholder("Sem distribuicao estadual", "Nao foram encontrados votos do candidato no estado.")
        return
    map_column, ranking_column = st.columns([1.65, 1])
    with map_column:
        if not state_choropleth(df, scope.uf):
            placeholder(
                "Mapa ainda nao disponivel para esta UF",
                "Os dados tabulares estao prontos, mas o GeoJSON municipal nao esta empacotado no app.",
                f"GeoJSON de municipios da UF {scope.uf}",
            )
    with ranking_column:
        st.markdown("#### Top municipios (votos do candidato)")
        top = df.head(30).copy()
        top.index = range(1, len(top) + 1)
        top["Votos"] = top["votes"].map(fmt_int)
        st.dataframe(top[["municipality", "Votos"]].rename(columns={"municipality": "Municipio"}), width="stretch")


def render_location_distribution(scope: ElectionScope, capabilities: dict[str, bool]) -> None:
    if _requires_municipality(scope):
        return
    section_title(
        "Onde estao os votos no municipio",
        f"Votacao de {scope.candidate_name} por local - {scope.municipality_name}",
    )
    df = votes_by_location(scope)
    map_column, ranking_column = st.columns([1.65, 1])
    with map_column:
        if not capabilities.get("voting_locations"):
            placeholder(
                "Locais de votacao indisponiveis",
                "O ano selecionado nao possui local_votacao carregado no banco.",
                f"localvotacao_{scope.election.year}.csv",
            )
        elif not location_map(df):
            placeholder(
                "Coordenadas ausentes",
                "Ha votos por numero do local, mas faltam latitude e longitude validas para desenhar as bolhas.",
                f"localvotacao_{scope.election.year}.csv com coordenadas",
            )
    with ranking_column:
        st.markdown("#### Top locais (votos do candidato)")
        if df.empty:
            st.info("Nenhum local encontrado.")
        else:
            display = df[["location", "votes"]].head(30).copy()
            display["Votos"] = display["votes"].map(fmt_int)
            st.dataframe(display[["location", "Votos"]].rename(columns={"location": "Local"}), width="stretch")


def render_historical_ranking(scope: ElectionScope) -> None:
    if _requires_municipality(scope):
        return
    section_title("Ranking geral no municipio", "Top 10 candidatos mais votados")
    previous_year = previous_cycle_year(scope)
    current_col, previous_col = st.columns(2)
    with current_col:
        st.markdown(f"### {scope.election.year}")
        candidate_table(ranking(scope), scope.candidate_number)
    with previous_col:
        if previous_year is None:
            st.markdown("### Ciclo anterior")
            placeholder(
                "Historico indisponivel",
                "Nao ha eleicao anterior para o mesmo cargo, UF e municipio no banco atual.",
                "Boletim de urna do ciclo eleitoral anterior",
            )
        else:
            st.markdown(f"### {previous_year}")
            candidate_table(ranking(scope, previous_year))


def render_territorial_leadership(scope: ElectionScope) -> None:
    if _requires_municipality(scope):
        return
    df = territorial_leaders(scope)
    total_locations = int(df["locations"].sum()) if not df.empty else 0
    section_title("Sintese territorial", f"Locais liderados por candidato - {total_locations} locais")
    if df.empty:
        placeholder("Sem sintese territorial", "Nao foi possivel calcular a lideranca por local.")
        return
    max_locations = max(int(df["locations"].max()), 1)
    for position, row in enumerate(df.itertuples(index=False), start=1):
        focused = str(row.number) == scope.candidate_number
        width = int(row.locations) / max_locations * 100
        st.markdown(
            f"""
            <div class="candidate-row {'candidate-focus' if focused else ''}">
              <div class="candidate-rank">{position}</div><div class="candidate-avatar">{html.escape(str(row.name)[:1])}</div>
              <div class="candidate-name"><strong>{html.escape(str(row.name))}</strong><span>{html.escape(str(row.party or '-'))}</span>
                <div style="height:5px;background:#dce5ef;margin-top:5px"><div style="height:5px;width:{width:.1f}%;background:#0b73c9"></div></div>
              </div>
              <div class="candidate-votes">{fmt_int(row.locations)}</div><div class="candidate-pct">locais</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.caption("Em caso de empate no local, o menor numero de candidato e usado como criterio deterministico.")


def render_location_result(scope: ElectionScope, capabilities: dict[str, bool]) -> None:
    if _requires_municipality(scope):
        return
    if not capabilities.get("voting_locations"):
        section_title("Resultado do local", "Top 10 em um local de votacao")
        placeholder(
            "Nomes dos locais indisponiveis",
            "O boletim possui o numero do local, mas a tabela local_votacao nao cobre este ano.",
            f"localvotacao_{scope.election.year}.csv",
        )
        return
    options = location_options(scope)
    if options.empty:
        placeholder("Sem locais", "Nao foram localizados locais de votacao para este recorte.")
        return
    names = dict(zip(options["number"].astype(str), options["name"]))
    selected = st.selectbox(
        "Local de votacao",
        options=options["number"].astype(str).tolist(),
        format_func=lambda number: names[number],
        key=f"location_{scope.election.key}_{scope.uf}_{scope.municipality_code}_{scope.office_code}",
    )
    rank_df, composition = location_result(scope, selected)
    section_title(
        str(names[selected]),
        f"Top 10 no local - {fmt_int(composition['valid_votes'])} validos - "
        f"{fmt_int(composition['blank_votes'])} brancos - {fmt_int(composition['null_votes'])} nulos",
    )
    candidate_table(rank_df, scope.candidate_number)
    focus = rank_df[rank_df["number"].astype(str) == scope.candidate_number]
    if focus.empty:
        placeholder("Candidato fora do Top 10", "O candidato em foco nao aparece entre os dez mais votados deste local.")
    else:
        row = focus.iloc[0]
        position = int(focus.index[0]) + 1
        kpi(
            f"Desempenho de {scope.candidate_name}",
            f"{position}o - {fmt_int(row['votes'])} votos",
            fmt_pct(row["percentage"]),
            accent=True,
        )


def render_geographic_detail(scope: ElectionScope, capabilities: dict[str, bool]) -> None:
    if _requires_municipality(scope):
        return
    section_title(
        "Detalhamento geografico",
        f"Votos de {scope.candidate_name} por municipio, bairro e local",
    )
    if not capabilities.get("voting_locations"):
        placeholder(
            "Bairros e nomes dos locais indisponiveis",
            "A tabela local_votacao nao cobre o ano selecionado. Os votos permanecem disponiveis por secao no boletim.",
            f"localvotacao_{scope.election.year}.csv",
        )
        return
    neighborhoods, locations = geographic_detail(scope)
    if locations.empty:
        placeholder("Sem detalhamento", "Nao ha associacao entre o boletim e os locais para o recorte atual.")
        return
    total_votes = int(locations["votes"].sum())
    municipality = pd.DataFrame(
        [{"Municipio": scope.municipality_name, "Ano": scope.election.year, "Votos": total_votes}]
    )
    st.markdown("#### Municipio")
    st.dataframe(municipality, hide_index=True, width="stretch")
    st.markdown("#### Bairro")
    neighborhood_display = neighborhoods.rename(columns={"neighborhood": "Bairro", "votes": "Votos"}).copy()
    neighborhood_display["Ano"] = scope.election.year
    neighborhood_display["Candidato"] = scope.candidate_name
    st.dataframe(neighborhood_display[["Bairro", "Ano", "Votos", "Candidato"]], hide_index=True, width="stretch")
    st.markdown("#### Local")
    location_display = locations.rename(columns={"location": "Local", "votes": "Votos"}).copy()
    location_display["Ano"] = scope.election.year
    location_display["Candidato"] = scope.candidate_name
    st.dataframe(location_display[["Local", "Ano", "Votos", "Candidato"]], hide_index=True, width="stretch")


def render_all_tabs(scope: ElectionScope, capabilities: dict[str, bool]) -> None:
    tabs = st.tabs(TAB_LABELS)
    renderers = [
        lambda: render_sumario(scope),
        lambda: render_municipal_overview(scope),
        lambda: render_state_profile(scope, capabilities),
        lambda: render_state_distribution(scope),
        lambda: render_location_distribution(scope, capabilities),
        lambda: render_historical_ranking(scope),
        lambda: render_territorial_leadership(scope),
        lambda: render_location_result(scope, capabilities),
        lambda: render_geographic_detail(scope, capabilities),
    ]
    for tab, renderer in zip(tabs, renderers):
        with tab:
            renderer()
