# votos_bairro — votos por candidato agregados por bairro

Script: `go_postgres/9_create_table_votos_bairro.go` (rodar com `go run 9_create_table_votos_bairro.go`).

## Objetivo
Gerar a tabela `votos_bairro` com o total de votos de cada candidato **por bairro**,
alimentando a aba "Bairro" da tela de votação (ranking de bairros por candidato e
comparativo entre candidatos).

## Estratégia adotada
Tabela física recriada por script (mesmo molde do `4_create_table_votos_candidatos.go`),
**não** materialized view. Motivo: os dados de eleição são imutáveis após a apuração,
então o refresh incremental da MV não agrega valor; tabela física dá controle total de
tipos e índices e segue o pipeline numerado já existente. Peso de armazenamento/leitura
é idêntico entre MV e tabela — o que importa é a granularidade do GROUP BY e os índices.

## Origem dos dados e JOIN
- `boletim_de_urna` — votos na granularidade de seção (NR_ZONA, NR_SECAO).
- `local_votacao` — traz o `NM_BAIRRO` por seção (importado pelo `8_import_local_votacao.go`).

JOIN pela chave de seção. **Atenção:** `local_votacao` usa `AA_ELEICAO` para o ano,
`boletim_de_urna` usa `ANO_ELEICAO`.

```
lv.AA_ELEICAO = bu.ANO_ELEICAO
lv.NR_TURNO   = bu.NR_TURNO
lv.SG_UF      = bu.SG_UF
lv.CD_MUNICIPIO = bu.CD_MUNICIPIO
lv.NR_ZONA    = bu.NR_ZONA
lv.NR_SECAO   = bu.NR_SECAO
```

## Pontos de atenção
- `local_votacao` só contém **2024**. Por isso o script itera apenas pelos anos
  presentes em `local_votacao` (DISTINCT AA_ELEICAO) e usa `JOIN` (não `LEFT JOIN`),
  evitando milhões de linhas com `NM_BAIRRO` nulo.
- Pré-requisito: importar `local_votacao` antes (`8_import_local_votacao.go`).
- Carga por ano via procedure `load_votos_bairro_by_year(p_ano)`, com retry em deadlock.

## Índices
- Origem: `idx_bu_secao_join` no boletim (ano, uf, municipio, zona, secao, turno).
  No `local_votacao` o índice único `idx_unique_local_votacao` (criado no 8_) já cobre
  a chave de JOIN.
- Alvo: `idx_vb_busca` (ano, municipio, turno, cargo), `idx_vb_bairro` (municipio, bairro),
  `idx_vb_cand` (ano, municipio, nr_votavel).

## Colunas de votos_bairro
NM_BAIRRO, NM_URNA_CANDIDATO, NM_VOTAVEL, NR_VOTAVEL, total_votos, ANO_ELEICAO,
NM_MUNICIPIO, CD_MUNICIPIO, NR_TURNO, SG_UF, CD_CARGO_PERGUNTA, DS_CARGO_PERGUNTA.
