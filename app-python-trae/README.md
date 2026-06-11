# App Python Trae

App `Python + Streamlit` desacoplado para explorar dados eleitorais em PostgreSQL.

## Estrutura

- `streamlit_app.py`: entrada principal do Streamlit
- `app/core.py`: configuracao, conexao e formatacao
- `app/queries.py`: consultas SQL
- `app/ui.py`: helpers visuais e as 9 abas
- `docs/`: planejamento, subagentes e lacunas de dados

## Execucao

```bash
cd /Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/app-python-trae
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Banco padrao

```text
postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes
```

Para sobrescrever, defina `DATABASE_URL`.
