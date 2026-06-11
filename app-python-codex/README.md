# Dossie Eleitoral - app-python-codex

Aplicacao Streamlit desacoplada dos demais prototipos deste repositorio. O app consulta o PostgreSQL existente em modo somente leitura e reproduz as nove telas fornecidas como referencia, uma por aba.

## Execucao

```bash
cd /Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/app-python-codex
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./run.sh
```

Por padrao, a configuracao e lida de `../.env`, usando `PGSQL_VECTOR_HOST`, `PGSQL_VECTOR_PORT`, `PGSQL_VECTOR_DATABASE`, `PGSQL_VECTOR_USERNAME` e `PGSQL_VECTOR_PASSWORD`.

Para usar outro arquivo:

```bash
APP_ENV_FILE=/caminho/para/.env ./run.sh
```

## Filtros

Nenhuma analise e exibida antes de clicar em **Exibir dados**. O recorte aplicado contem:

- eleicao e ano;
- codigo da eleicao;
- turno;
- estado;
- cidade opcional em eleicoes gerais e obrigatoria para identificar candidatos municipais;
- cargo;
- candidato em foco.

As abas municipais exibem um placeholder enquanto nenhuma cidade estiver selecionada. Em eleicoes municipais, a cidade e exigida antes do candidato porque o mesmo numero pode ser usado por pessoas diferentes em municipios distintos.

## Abas

1. Sumario
2. Visao municipal
3. Perfil da UF
4. Distribuicao estadual
5. Distribuicao por local
6. Ranking historico
7. Lideranca territorial
8. Resultado do local
9. Detalhamento geografico

## Seguranca dos dados

- A conexao executa `SET default_transaction_read_only = on`.
- O codigo nao possui comandos de escrita ou migracoes automaticas.
- Nenhuma tabela existente e alterada.
- Sugestoes de indices estao em `sql/indices_opcionais.sql` e devem ser revisadas antes de qualquer uso manual.

## Estrutura

```text
app-python-codex/
├── streamlit_app.py
├── src/election_app/
│   ├── config.py
│   ├── database.py
│   ├── models.py
│   ├── repository.py
│   └── ui/
├── assets/
├── docs/
├── sql/
└── tests/
```

Consulte `docs/PLANEJAMENTO.md` e `docs/DADOS_TSE_FALTANTES.md` para escopo, cobertura e proximos dados a importar.
