# Planejamento do app-python-codex

## Objetivo

Construir um dashboard eleitoral em Python e Streamlit, isolado em `app-python-codex`, usando o PostgreSQL existente sem alterar suas tabelas.

## Levantamento executado

- 9 imagens de referencia analisadas e mapeadas para 9 abas.
- PostgreSQL inspecionado em uma sessao somente leitura.
- Prototipos `app-python`, `app-python-minimax` e `app-python-trae` avaliados para reaproveitamento conceitual.
- Documentacao atual de Streamlit e Plotly consultada via Context7.
- Tres subagentes usados em paralelo para banco, referencias visuais e arquitetura.

## Fases

### 1. Inventario

- Identificar tabelas, colunas, anos, turnos, UFs e municipios.
- Identificar dados necessarios para cada imagem.
- Registrar lacunas de cobertura.

Status: concluido.

### 2. Arquitetura

- Configuracao independente via `.env`.
- Conexao PostgreSQL somente leitura.
- `ElectionScope` tipado com ano, eleicao, turno, UF, municipio, cargo e candidato.
- SQL centralizado no repositorio.
- Componentes e abas separados da infraestrutura.

Status: concluido.

### 3. Interface

- Bloquear analises ate o usuario aplicar os filtros.
- Reproduzir a linguagem visual: fundo claro, azul, cards, rankings e mapas.
- Criar uma aba para cada imagem.
- Exibir placeholders orientativos onde o banco nao cobre o recorte.

Status: concluido.

### 4. Dados e desempenho

- Filtrar sempre por `CD_ELEICAO` e `NR_TURNO`.
- Usar apenas `DS_TIPO_VOTAVEL = Nominal` em rankings de candidatos.
- Incluir turno nos joins com `local_votacao`.
- Usar cache de consulta com TTL de 15 minutos.
- Manter indices opcionais apenas como documentacao.

Status: concluido.

### 5. Validacao

- Compilar todos os modulos Python.
- Executar testes unitarios.
- Fazer consultas de fumaca contra o PostgreSQL real.
- Subir o Streamlit e validar a pagina inicial.

Status: executado durante a entrega; resultados descritos na resposta final.

## Decisoes de produto

- Cidade e opcional em eleicoes gerais. Em eleicoes municipais ela e obrigatoria, pois numeros de prefeito e vereador se repetem entre municipios.
- O perfil demografico disponivel no banco descreve o cadastro eleitoral, nao a abstencao por grupo. A interface deixa essa diferenca explicita.
- Empates de lideranca por local usam o menor numero de candidato como criterio deterministico.
- GeoJSON municipal foi empacotado para AC e GO, UFs atualmente cobertas pelo boletim local.
