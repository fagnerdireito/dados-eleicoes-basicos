# Dossiê eleitoral — Streamlit

App Python + Streamlit que reproduz as 9 telas do dossiê eleitoral a partir do PostgreSQL local (`eleicoes`).

## Pré-requisitos

- Python 3.10+
- PostgreSQL local com o banco `eleicoes` populado pelos pipelines em `../go_postgres/`
  (precisa pelo menos das tabelas `boletim_de_urna`, `local_votacao`, `municipios`, `estados`).
- Conexão: `postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes` (sobrescrevível via `DATABASE_URL`).

## Como rodar

```bash
cd app-python
chmod +x run.sh
./run.sh
```

O script cria `.venv`, instala `requirements.txt` e sobe o Streamlit em `http://localhost:8501`.

## Filtros globais

No topo da página: **Eleição/Ano · UF · Cidade · Cargo · Candidato foco**.
- Cidade é obrigatória em anos pares (eleições municipais — 2020/2024).
- Cidade é opcional em 2022 (eleições gerais).
- Candidato foco é usado pelas abas Resumo, Onde estão os votos (estado/município), Síntese territorial, Card local e Bairro.

## Abas

| # | Aba | Status |
|---|---|---|
| 1 | Sumário | OK |
| 2 | Resumo no município | OK |
| 3 | Perfil do eleitorado (UF) | placeholder (precisa importar `perfil_comparecimento_abstencao`) |
| 4 | Onde estão os votos no estado (mapa) | OK — geometria baixada pela `geobr` |
| 5 | Onde estão os votos no município (mapa) | OK só para 2024 (depende de `local_votacao`) |
| 6 | Ranking geral no município | OK |
| 7 | Síntese territorial | OK |
| 8 | Card local de votação | OK |
| 9 | Votos por bairro | OK só para 2024 |

## Dados ainda não importados (TSE)

1. `perfil_comparecimento_abstencao_<ANO>.zip` (2020/2022/2024)
2. `bweb_<turno>t_RO_<timestamp>.zip` (para reproduzir as imagens de referência de RO)
3. `consulta_cand_<ANO>.zip` para 2018/2020/2022
4. `eleitorado_local_votacao_<ANO>.zip` para 2018/2020/2022
5. `foto_cand<ANO>_<UF>_div.zip` (opcional, para avatares dos cards)

URLs no formato `https://cdn.tse.jus.br/estatistica/sead/odsele/<dataset>/`.
