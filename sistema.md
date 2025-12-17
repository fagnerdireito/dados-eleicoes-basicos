# preparacao de dados para sistema de consulta de resultados de eleicoes

## objetivo
- primeiro objetivo é preparar os dados de boletim de urna para consulta em um sistema web.
- o grande problema é que os dados de boletim de urna são big data, ou seja, dados muito grandes com grande volume de registros em csv.
- para isso primeiro é importante fazer um processo de consolidação e transformação dos dados, para que fiquem em um formato mais adequado para consulta.
- para que fiquem mais rápido e organizado um script em python devera ser desenvolvido para fazer essa consolidação e transformação, contagem, soma, agregação, agrupamento e ordenação dos dados.
- por exemplo:
* uma lista de estados
* uma lista de cidades por estado
* uma lista de candidatos por município
* uma lista de votos por candidato por município
* uma lista de partidos em geral agrupados por partido
* uma lista de votos por partido por município ou por estado
- Sugestões de formato poderia ser em um banco de dados relacional, como o MySQL.
- os dados serao consumidos por uma aplicacao laravel ao final.

## script python
o script python deve utilizar as boas práticas de ciência de dados, como:
- leitura de csv
- utilizar bibliotecas como pandas, numpy, matplotlib, seaborn, scikit-learn, entre outras
- utilizar funções de agregação, contagem, soma, média, mediana, desvio padrão, variância, entre outras
- utilizar funções de ordenação, filtragem, seleção, entre outras
- reconhecer o padrao anual de dados, por exemplo ler os arquivos de acordo com o ano eleitoral, estado, cidade, candidato, partido, entre outros.
- para cada pasta e para cada arquivo verifica-se o padrao de acordo com a nomenclatura de cada arquivo, por exemplo:
- Lista de diretórios:
* bweb_1t_AC_051020221321 (ano eleitoral 2022, estado AC, cidade AC, turno 1)
* bweb_1t_AC_091020241636 (ano eleitoral 2024, estado AC, cidade AC, turno 1)
* bweb_1t_AL_051020221321 (ano eleitoral 2022, estado AL, cidade AL, turno 1)
* bweb_2t_AC_281020241046 (ano eleitoral 2024, estado AC, cidade AC, turno 2)
* bweb_2t_AC_311020221535 (ano eleitoral 2022, estado AC, cidade AC, turno 2)
* bweb_2t_AL_281020241046 (ano eleitoral 2024, estado AL, cidade AL, turno 2)
* bweb_2t_AL_311020221535 (ano eleitoral 2022, estado AL, cidade AL, turno 2)
- para cada pasta e para cada arquivo verifica-se o padrao de acordo com a nomenclatura de faz a compatibiidade de cada arquivo
- dentro de cada arquivo tem um arquivo pdf com instrucoes de como ler o arquivo csv:
bweb_1t_AC_051020221321/leiame-boletimurnaweb.pdf

- por fim dentro de cada pasta tem o seu arquivo csv:
* bweb_1t_AC_051020221321/bweb_1t_AC_051020221321.csv

- se novas pastas forem adicionadas, o script python devera ser capaz de detectar e processar os novos arquivos e pastas sem duplicar os registros já processados ou adicionados no banco de dados.

## formato dos arquivos
os detalhes da estrutura dos arquivos csv podem ser encontrados no arquivo descricao_arquivos.md

## pipeline de processamento python
# Estrutura sugerida do projeto
election_data_pipeline/
├── config/
│   └── settings.py          # Configurações (paths, encoding, etc)
├── src/
│   ├── extractors/
│   │   └── csv_extractor.py  # Leitura dos CSVs agrupados por ano detectado dinamicamente agrupando nomes de pastas
│   ├── transformers/
│   │   ├── cleaner.py        # Limpeza de dados
│   │   ├── aggregator.py     # Agregações
│   │   └── normalizer.py     # Normalização para BD
│   ├── loaders/
│   │   └── mysql_loader.py   # Carga no MySQL
│   └── utils/
│       └── file_parser.py    # Parser de nomenclatura
├── scripts/
│   └── run_pipeline.py
└── requirements.txt

## sh file
- o script run_pipeline.sh deve ser executado para rodar o pipeline de processamento python.
- o script run_pipeline.sh deve ser configurado com as seguintes variáveis:
- ANO_ELEITORAL=2022
- TIPO_ELEICAO=1
- UF=AC
- CIDADE=AC
- TURNOS=(1 2)
- o script run_pipeline.sh deve ser executado para cada ano eleitoral, tipo eleitoral, estado, cidade e turno.

## Estrutura de Banco de Dados
- eleicoes (id, ano, turno, tipo_eleicao, dt_pleito)
- estados (id, sigla, nome)
- municipios (id, estado_id, codigo_tse, nome)
- zonas (id, municipio_id, nr_zona)
- secoes (id, zona_id, nr_secao, nr_local_votacao, qt_aptos)
- cargos (id, codigo, descricao)
- partidos (id, numero, sigla, nome)
- candidatos (id, partido_id, cargo_id, nr_votavel, nome, eleicao_id)
- votos_consolidados (id, eleicao_id, municipio_id, cargo_id, candidato_id, total_votos, total_validos, total_brancos, total_nulos)
- votos_por_secao (id, secao_id, candidato_id, qt_votos) -- opcional para drill-down

## Otimizações para Performance
- Usar chunks para leitura de arquivos grandes
- Processar em paralelo com multiprocessing

## conexao mysql
DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=eleicoes
DB_USERNAME=root
DB_PASSWORD=


# Especificação de Telas — Sistema de Resultados Eleitorais Municipais

Este documento descreve duas telas principais de um sistema de visualização de resultados eleitorais, com foco em **reprodução fiel da experiência**, clareza de dados e boa usabilidade. O público-alvo é um **engenheiro de software** responsável por implementar o frontend e integrar com backend/dados eleitorais.

---

## 1. Tela de Resultados por Município

### Objetivo
Exibir o **resultado consolidado de uma eleição majoritária (ex: Prefeito, Vereador, Governador, Deputado Federal, Deputado Estadual e Deputado Distrital e Senador)** para um município ou uf específico, com ranking de candidatos, percentual de votos, votos absolutos e distribuição visual.

---

### Estrutura Geral

- **Cabeçalho**
  - Filtro de Ano Eleitoral (com opções pré-definidas)
  - Filtro de Turno (1 ou 2)
  - Filtro de UF (estado)
  - Filtro de Cidade
  - Nome do município (opcional em eleicoes de Gerais (Governador, Deputado Federal, Deputado Estadual e Deputado Distrital e Senador))
  - UF (sigla do estado)
  - Exemplo: `Rio Verde - GO`

- **Lista de Candidatos (ordenada por votos válidos – decrescente)**

Cada candidato é apresentado como um **card linear** contendo:

#### Elementos por candidato
- Foto/avatar do candidato (circular)
- Nome completo
- Partido (sigla ou nome)
- Percentual de votos válidos (em destaque, alinhado à direita)
- Total de votos absolutos
- Barra horizontal de progresso proporcional ao percentual de votos

#### Regras visuais
- A barra de progresso ocupa 100% da largura disponível
- A cor da barra identifica o candidato (ou partido)
- Apenas a parte correspondente ao percentual é colorida; o restante é cinza claro
- O candidato vencedor aparece no topo

---

### Exemplo de Dados por Candidato
- Nome: Wellington Carrijo  
- Partido: MDB  
- Percentual: 62,67%  
- Votos: 69.209  

---

### Rodapé de Estatísticas Gerais

Bloco separado (background neutro) com três indicadores:

- **Votos Brancos**
  - Total absoluto
  - Percentual
- **Votos Nulos**
  - Total absoluto
  - Percentual
- **Votos Válidos**
  - Total absoluto
  - Percentual

Layout em **grid de 3 colunas**, alinhado horizontalmente.

---

### Requisitos Funcionais
- Dados devem ser atualizados dinamicamente (API)
- Percentuais calculados com base em votos válidos
- Ordenação automática dos candidatos
- Suporte a diferentes quantidades de candidatos

---

### Requisitos Não Funcionais
- Layout responsivo
- Performance otimizada para grandes volumes de municípios
- Acessibilidade (contraste, textos legíveis)

---

## 2. Tela de Busca e Seleção de Municípios

### Objetivo
Permitir que o usuário **busque e selecione rapidamente um município** para visualizar seus resultados eleitorais.

---

### Estrutura Geral

#### Campo de Busca
- Input de texto com placeholder (ex: “Buscar município”)
- Ícone de lupa à esquerda
- Botão de limpar (X) à direita
- Busca em tempo real (debounce recomendado)

---

#### Lista de Resultados (Dropdown)

Exibida logo abaixo do campo de busca.

Cada item contém:
- Nome do município
- Sigla do estado (UF)
- Ícone de “favoritar” (estrela) à direita

Exemplos:
- Rio Bom, PR
- Rio Real, BA
- Rio Azul, PR

---

### Comportamento
- Lista é filtrada conforme o usuário digita
- Clique em um município:
  - Fecha o dropdown
  - Redireciona ou atualiza a tela de resultados
- Clique na estrela:
  - Marca/desmarca município como favorito
  - Favoritos podem ser persistidos (localStorage ou backend)

---

### Requisitos Funcionais
- Busca tolerante a acentos
- Busca parcial (ex: “rio” retorna vários municípios)
- Suporte a milhares de registros

---

### Requisitos de UX
- Feedback visual ao passar o mouse
- Item selecionado com destaque
- Navegação possível via teclado

---

## Considerações Técnicas (Sugestão)

- **Frontend**
  - React / Vue / Svelte / Livewire / Streamlit
  - Componentização: `MunicipioSelector`, `ResultadoCandidato`, `ResumoVotacao`

- **Backend / Dados**
  - API REST ou GraphQL
  - Endpoint por município + eleição
  - Cache agressivo para resultados consolidados

- **Visual**
  - Cores controladas
  - Tipografia clara
  - Foco em leitura rápida de dados

---

## Visão Geral

O sistema combina:
- **Busca rápida**
- **Visualização clara**
- **Dados eleitorais confiáveis**
- **UX simples e direta**

Perfeito para dashboards públicos, análises eleitorais e projetos cívicos.  
Nada de overengineering: simples, escalável e elegante — igual código bom 😉