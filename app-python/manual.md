# Manual — Dossiê eleitoral (Streamlit)

Este documento explica **o que foi entregue**, **o que ainda falta** e **como prosseguir** com o app em `app-python/`.

---

## 1. Visão geral

App **Python + Streamlit** que consome o PostgreSQL local (`eleicoes`) e reproduz as 9 telas do dossiê eleitoral indicadas pelas imagens em `.vscode/imagens-sistema/`.

- Cada imagem virou **uma aba** (`st.tabs`).
- Filtros globais (Eleição/Ano + UF + Cidade + Cargo + Candidato) ficam **acima das abas** e valem para todas elas.
- Conexão padrão: `postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes` (sobrescrevível via `DATABASE_URL`).

---

## 2. O que foi feito

### 2.1 Estrutura de arquivos

```
app-python/
├── app.py                        # entrypoint + filtros globais + st.tabs
├── db.py                         # engine SQLAlchemy + table_exists + run_df
├── queries.py                    # funções SQL cacheadas (uma por necessidade)
├── components.py                 # KPI cards, barras, formatadores pt-BR
├── ui/
│   ├── __init__.py
│   ├── tab_sumario.py
│   ├── tab_resumo_municipio.py
│   ├── tab_perfil_eleitorado.py
│   ├── tab_votos_estado.py
│   ├── tab_votos_municipio.py
│   ├── tab_ranking_municipio.py
│   ├── tab_sintese_territorial.py
│   ├── tab_card_local.py
│   └── tab_votos_bairro.py
├── assets/geojson_cache/         # cache parquet dos shapes da geobr
├── .streamlit/config.toml        # tema azul (#1f6feb)
├── requirements.txt
├── run.sh                        # cria .venv, instala, sobe streamlit
├── README.md
└── manual.md                     # este arquivo
```

### 2.2 Stack

- `streamlit>=1.39`, `pandas`, `pyarrow`
- `sqlalchemy>=2` + `psycopg2-binary` (engine via `@st.cache_resource`)
- `plotly` (preparado para gráficos extras)
- `geobr` + `folium` + `streamlit-folium` (mapas)
- `python-dotenv`, `babel`

### 2.3 Filtros globais

Renderizados no topo de `app.py`, persistidos em `ctx` e passados para cada aba:

1. **Eleição/Ano** — populado por `SELECT DISTINCT "ANO_ELEICAO" FROM boletim_de_urna`.
2. **UF** — depende do ano.
3. **Cidade** — obrigatória em 2020/2024, opcional em 2022.
4. **Cargo** — dinâmico conforme escopo (Presidente, Governador, Senador, Deputados, Prefeito, Vereador).
5. **Candidato foco** — top N por votos no escopo.

### 2.4 Abas implementadas

| # | Aba | Estado | Observação |
|---|---|---|---|
| 1 | **Sumário** | ✅ | Índice navegável com as 8 outras abas. |
| 2 | **Resumo no município** | ✅ | KPIs: votos, posição (RANK), liderança em locais, composição (válidos/brancos/nulos/abstenções/comparecimento). Validado: Caiado/Goiânia/2022 → 332.769 votos, 1º lugar, 87/88 locais, 43,6% válidos. |
| 3 | **Perfil eleitorado (UF)** | ⚠️ placeholder | Tabela `perfil_comparecimento_abstencao` não existe ainda. Mostra mensagem com instruções de carga. |
| 4 | **Votos no estado (mapa)** | ✅ | Coroplético com `geobr.read_municipality(uf)` cacheado em parquet local + lista top 30 municípios. |
| 5 | **Votos no município (mapa)** | ✅ só em 2024 | Bolhas por local de votação via `JOIN local_votacao` (lat/lng). Demais anos: aviso explicando lacuna. |
| 6 | **Ranking município** | ✅ | Top 10 candidatos lado a lado, comparando ano selecionado vs ano−4 (mesmo tipo de eleição). |
| 7 | **Síntese territorial** | ✅ | `RANK() OVER (PARTITION BY NR_LOCAL_VOTACAO)` agregando "quantos locais cada candidato lidera". |
| 8 | **Votos por local de votação** | ✅ | Seletor de local + top 10 candidatos + totais (válidos/brancos/nulos) + destaque do candidato foco. |
| 9 | **Votos por bairro** | ✅ só em 2024 | Tabelas Município/Bairro/Local com JOIN dinâmico em `local_votacao`. |

### 2.5 Cache e performance

- `db.get_engine()` → `@st.cache_resource` (singleton).
- Funções em `queries.py` → `@st.cache_data(ttl=3600)` com chave automática por parâmetros.
- `assets/geojson_cache/<uf>_municipios.parquet` grava shape no primeiro hit (TTL 24h em runtime, persistente no disco).
- Botão "Limpar cache" no sidebar.

### 2.6 Como rodar

```bash
cd /Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/app-python
./run.sh
# Abre em http://localhost:8501
```

`run.sh` cria `.venv`, instala `requirements.txt` e sobe o Streamlit. Para reiniciar com cache limpo, pare o processo e rode novamente.

---

## 3. O que ainda falta (e por quê)

### 3.1 Limitações de dados

| Lacuna | Impacto | Tabela/origem necessária |
|---|---|---|
| Sem boletim de Rondônia | As imagens originais usam RO — hoje só temos AC e GO. | `bweb_<turno>t_RO_*.zip` |
| `local_votacao` só tem 2024 | Mapa município + bairro em 2020/2022 ficam indisponíveis. | `eleitorado_local_votacao_2018.zip`, `_2020.zip`, `_2022.zip` |
| Sem `perfil_comparecimento_abstencao` | Aba 3 fica em placeholder. | `perfil_comparecimento_abstencao_<ANO>.zip` |
| Sem fotos de candidatos | Cards de ranking sem avatares (imagens originais têm). | `foto_cand<ANO>_<UF>_div.zip` |
| Sem `consulta_cand` 2018/2020/2022 | Atributos como gênero/escolaridade do candidato não consultáveis. | `consulta_cand_<ANO>.zip` |

### 3.2 Funcionalidades não implementadas

- **Aba 3 (Perfil do eleitorado)**: hoje só mostra placeholder. Falta:
  - Cards "Comparecimento (UF)" e "Abstenção (UF)" com totais.
  - Gráfico de barras horizontal "Abstenção por faixa etária".
  - Gráfico de barras horizontal "Abstenção por escolaridade".
- **Avatares de candidato** nos cards (Ranking, Votos por local de votação, Síntese).
- **Filtro por turno** (`NR_TURNO`): hoje agrega 1º e 2º turnos juntos. Necessário em eleições com 2º turno (Governador/Presidente).
- **Filtro por situação** (`DS_SIT_TOT_TURNO`): hoje ranking ignora eleito/não eleito.
- **Persistir favoritos** (estrela de município, como na tela "Busca e Seleção de Municípios" do `sistema.md`).
- **Exportar dados** (CSV/Excel) por aba.
- **Comparativo multi-candidato** na mesma tela.
- **Testes automatizados** (`pytest` cobrindo `queries.py` com fixtures de dados).

### 3.3 Débitos técnicos

- `tab_votos_estado.py` usa um centróide aproximado (primeiros 50 polígonos). Para UFs com município muito longe (ex: Trindade-PE numa coroplético do PE), pode descentralizar. **Como melhorar**: calcular centróide real via Shapely (`geom.centroid`).
- `Choropleth` está com `key_on="feature.properties.name"` (match por nome). Pode falhar em municípios com acentuação diferente entre TSE e IBGE. **Como melhorar**: usar `code_muni` do IBGE mapeando do TSE via tabela `municipio_tse_ibge` já existente em `dados/`.
- Deprecation warnings: `use_container_width=True` precisa virar `width='stretch'` antes de 2025-12-31.
- A tabela `votos_bairro` planejada no `memory-bank/votos_bairro.md` ainda não existe. Hoje o app faz JOIN dinâmico — funciona, mas é mais lento. **Como melhorar**: rodar `go run go_postgres/9_create_table_votos_bairro.go` e trocar as queries da aba 9 para usar a tabela materializada.

---

## 4. Como continuar — passo a passo sugerido

### 4.1 Importar dados faltantes (alta prioridade)

#### a) Boletim de urna de Rondônia (para reproduzir as imagens)

```bash
# 2022
curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_secao/bweb_1t_RO_<TIMESTAMP>.zip
# 2024 (turno 1 e 2)
curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_secao/bweb_1t_RO_<TIMESTAMP>.zip
curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_secao/bweb_2t_RO_<TIMESTAMP>.zip
```

Aproveite o pipeline existente: `go run 1_import_boletim_urna.go` (em `go_postgres/`) — ele já é idempotente (índice `idx_unique_bu`).

#### b) Locais de votação para 2018/2020/2022

```bash
curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao_2022.zip
```

Depois rode `go run 8_import_local_votacao.go`. Isso destrava automaticamente:
- Aba 5 (mapa por local) em 2020/2022.
- Aba 9 (bairro) em 2020/2022.

#### c) Perfil do eleitorado

```bash
curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_comparecimento_abstencao/perfil_comparecimento_abstencao_2022.zip
```

Você precisará criar um importador (copie o padrão de `import_boletim_urna.py` / `1_import_boletim_urna.go`). A tabela deve ficar com nome `perfil_comparecimento_abstencao`. Depois de importada, implemente a aba 3:

```python
# ui/tab_perfil_eleitorado.py (esboço)
df_idade = run_df("""
    SELECT DS_FAIXA_ETARIA AS faixa,
           SUM(QT_ABSTENCAO)::float / NULLIF(SUM(QT_APTOS), 0) * 100 AS pct
    FROM perfil_comparecimento_abstencao
    WHERE ANO_ELEICAO=:ano AND SG_UF=:uf
    GROUP BY 1 ORDER BY 1
""", {"ano": ano, "uf": uf})
st.bar_chart(df_idade.set_index("faixa"))
```

### 4.2 Materializar `votos_bairro`

```bash
cd go_postgres
go run 9_create_table_votos_bairro.go
```

Depois substitua, em `queries.py`, as funções `votos_por_bairro` e `votos_por_local_candidato` para ler de `votos_bairro` (mais rápido, sem JOIN em runtime).

### 4.3 Adicionar avatares de candidato

1. Baixar `foto_cand<ANO>_<UF>_div.zip` do TSE.
2. Extrair em `app-python/assets/fotos_candidatos/<ANO>/<SQ_CANDIDATO>.jpg`.
3. Em `components.py::bar_row`, aceitar `foto_url` e renderizar `<img>`:

```python
def bar_row(name, party, votos, pct, rank, color="#1f6feb", foto=None):
    avatar = f"<img src='{foto}' style='width:36px;height:36px;border-radius:50%;object-fit:cover'/>" if foto else ""
    # ... compor avatar no início da linha
```

4. Em `queries.py`, fazer JOIN com `consulta_cand` para obter `SQ_CANDIDATO` por `NR_VOTAVEL`.

### 4.4 Filtro por turno

Em `app.py`, adicionar um `selectbox` com `NR_TURNO` ao lado do Cargo (default = 1). Propagar via `ctx["nr_turno"]` e incluir `AND "NR_TURNO" = :turno` em todas as queries de `queries.py`. Sem isso, segundos turnos contaminam os totais de 1º turno.

### 4.5 Polimento UX

- Trocar `use_container_width=True` por `width='stretch'` em todas as ocorrências.
- Mapear `CD_MUNICIPIO` (TSE) → `code_muni` (IBGE) via `dados/municipio_tse_ibge/` para o coroplético da aba 4.
- Permitir busca textual no selectbox de candidato (já é nativo do Streamlit) e de município.

### 4.6 Testes

```bash
pip install pytest
mkdir tests
```

Cobrir queries críticas com fixtures de banco (transação rollback ao fim):

```python
# tests/test_queries.py
from queries import resumo_candidato_municipio
def test_resumo_caiado_goiania_2022():
    d = resumo_candidato_municipio(2022, "GO", "93734", "3", "44")
    assert d["posicao"] == 1
    assert d["votos_cand"] == 332769
```

---

## 5. Mapeamentos úteis

### Códigos de cargo (`CD_CARGO_PERGUNTA`)

| Código | Cargo | Eleição |
|---|---|---|
| 1 | Presidente | Geral (2022) |
| 3 | Governador | Geral (2022) |
| 5 | Senador | Geral (2022) |
| 6 | Deputado Federal | Geral (2022) |
| 7 | Deputado Estadual | Geral (2022) |
| 8 | Deputado Distrital | Geral (2022, DF) |
| 11 | Prefeito | Municipal (2020/2024) |
| 13 | Vereador | Municipal (2020/2024) |

### `DS_TIPO_VOTAVEL`

`Nominal`, `Legenda`, `Branco`, `Nulo`. O app exclui Branco/Nulo das agregações de "votos válidos".

### Códigos de município

O `CD_MUNICIPIO` no `boletim_de_urna` é o **código TSE** (ex: Goiânia = `93734`), **não** o código IBGE (Goiânia = `5208707`). Use `dados/municipio_tse_ibge/` para converter ao integrar com bases IBGE.

---

## 6. Verificação rápida (smoke test)

```bash
cd app-python
./run.sh
# em outro terminal:
curl -sS http://127.0.0.1:8501/_stcore/health
# esperado: ok
```

No browser (`http://localhost:8501`), valide:

- Fluxo **GO/2022/sem cidade/Governador** → aba 4 deve mostrar coroplético + Caiado liderando.
- Fluxo **GO/2022/Goiânia/Governador/Caiado** → aba 2 deve mostrar 332.769 votos, 1º lugar, 87/88 locais.
- Fluxo **GO/2024/Goiânia/Prefeito/<top>** → abas 5 e 9 funcionando com mapa de bolhas e bairros.
- Fluxo **AC/2020/Rio Branco/Prefeito/<top>** → abas 5 e 9 mostram aviso "só 2024" se `local_votacao` 2020 não estiver carregado.

---

## 7. Decisões fechadas durante o desenvolvimento

1. **Cobertura**: app construído só com AC + GO. RO fica para quando os ZIPs forem importados.
2. **Abas sem dado**: mostram placeholder com instrução de carga, em vez de desaparecer.
3. **Mapas**: `geobr` + `folium` com cache parquet local (não estamos versionando GeoJSONs).
4. **Bairro**: JOIN dinâmico em `local_votacao` no lugar de criar `votos_bairro` agora — funcional, otimizar depois.
5. **Banco**: PostgreSQL local, sem proxy nem API intermediária. App lê direto via SQLAlchemy.
