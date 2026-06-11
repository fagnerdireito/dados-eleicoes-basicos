# Dossiê Eleitoral - Dashboard Streamlit

Dashboard de análise de dados eleitorais brasileiros, baseado no Boletim de Urna do TSE.

## 📋 Descrição

O Dossiê Eleitoral é uma aplicação Streamlit que permite visualizar e analisar resultados de eleições brasileiras. A interface apresenta 9 abas com diferentes visualizações e métricas, todas filtráveis por:

- **Eleição/Ano**: 2018, 2022, 2024, etc.
- **UF**: Estado brasileiro
- **Cidade**: Município (obrigatório em municipais, opcional em gerais)
- **Cargo**: Presidente, Governador, Senador, Deputado, etc.
- **Candidato foco**: Candidato para destacar nas métricas

## 🚀 Instalação

```bash
# 1. Clone ou navegue até o diretório
cd /Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/app-python-minimax

# 2. Crie ambiente virtual (opcional)
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# 3. Instale dependências
pip install -r requirements.txt

# 4. Execute
streamlit run app.py
```

## 📊 Abas do Dashboard

1. **Sumário** - Índice/navegação do relatório
2. **Resumo município** - KPIs do candidato no município
3. **Perfil eleitorado (UF)** - Abstenção por idade/escolaridade (placeholder)
4. **Votos no estado** - Mapa + ranking por município
5. **Votos no município** - Mapa de bolhas por local
6. **Ranking município** - Comparativo entre anos
7. **Síntese territorial** - Locais liderados por candidato
8. **Card local** - Ranking em local específico
9. **Votos por bairro** - Drill-down territorial

## 🗄️ Banco de Dados

O app usa PostgreSQL com as seguintes tabelas:

- `boletim_de_urna` - Votos por seção
- `consulta_cand` - Dados de candidatos
- `local_votacao` - Locais de votação (necessário para mapas)
- `votos_candidatos` - Votos agregados

**Connection string:** `postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes`

## ⚠️ Dados Necessários

Para funcionamento completo, é necessário importar:

1. **Boletim de Urna (BU)** - `bu_{ano}.zip`
2. **Consulta Candidatos** - `consulta_cand_{ano}.zip`
3. **Local Votação** - `localvotacao_{ano}.zip` (para mapas)

Consulte `DADOS_TSE.md` para instruções detalhadas.

## 📁 Estrutura do Projeto

```
app-python-minimax/
├── app.py              # App principal
├── requirements.txt    # Dependências
├── DADOS_TSE.md        # Guia de importação
├── db/                 # Conexão com banco
│   ├── __init__.py
│   └── connection.py
├── queries/            # Queries SQL
│   ├── __init__.py
│   ├── catalogos.py    # Filtros
│   └── abas.py         # Dados das abas
├── ui/                 # Interface
│   ├── __init__.py
│   ├── components.py   # Componentes visuais
│   ├── utils.py        # Utilitários
│   └── tab_*.py        # 9 tabs
└── config/            # Configurações
```

## 🛠️ Desenvolvimento

Para adicionar novas abas:

1. Crie `ui/tab_nova.py` com classe `TabNova` e método `render(ctx)`
2. Importe no `app.py`
3. Adicione à lista de tabs

## 📝 Licença

Projeto interno para análise de dados eleitorais.