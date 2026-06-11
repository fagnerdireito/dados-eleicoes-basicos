# Dados Faltantes E Complementares

## Ja disponiveis no banco

- `boletim_de_urna`
- `local_votacao`
- `votos_candidatos`
- `votos_partido`
- `municipios`
- `estados`

## Lacunas principais

### Perfil do eleitorado por faixa etaria e escolaridade

Para reproduzir a aba `Perfil do eleitorado` com o mesmo nivel da referencia, baixe preferencialmente:

- `Eleitorado - <ANO>`
- `Perfil do eleitorado por secao eleitoral - <ANO>`
- `Comparecimento e Abstencao - <ANO>`

Links uteis:

- https://dadosabertos.tse.jus.br/dataset/eleitorado-2024
- https://dadosabertos.tse.jus.br/dataset/eleitorado-2022
- https://dadosabertos.tse.jus.br/dataset/comparecimento-e-abstencao-2024
- https://dadosabertos.tse.jus.br/group/eleitorado

## Complemento recomendado

### Mapa por municipio na UF

Para liberar o mapa coropletico da aba `Onde estao os votos no estado`, inclua tambem uma malha geografica municipal em `app-python-trae/app/assets`.

Essa malha nao vem do TSE; normalmente ela e obtida via IBGE ou `geobr`.

## Observacao

Nenhuma tabela existente foi alterada. Se voce decidir importar novos datasets, a recomendacao e criar tabelas auxiliares novas e scripts dedicados dentro desta pasta.
