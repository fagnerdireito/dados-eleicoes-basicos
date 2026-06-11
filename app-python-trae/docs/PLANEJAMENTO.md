# Planejamento Tecnico

## Objetivo

Construir um app `Python + Streamlit` desacoplado do Laravel, inteiramente dentro de `app-python-trae`, consumindo o PostgreSQL e reproduzindo as 9 referencias visuais como 9 abas independentes.

## Premissas

- nao alterar tabelas existentes
- trabalhar em modo somente leitura sobre o banco atual
- filtrar antes das abas por eleicao/ano e UF
- permitir cidade opcional em eleicoes gerais e exigir/induzir cidade nas visoes municipais
- criar placeholders explicitos quando faltar base ou geometrias

## Fontes confirmadas no PostgreSQL

- `boletim_de_urna`
- `local_votacao`
- `municipios`
- `estados`
- `votos_candidatos`
- `votos_partido`

## Abas planejadas

1. sumario
2. resumo no municipio
3. perfil do eleitorado
4. onde estao os votos no estado
5. onde estao os votos no municipio
6. ranking geral no municipio
7. sintese territorial
8. card do local de votacao
9. bairros e locais do candidato

## Estrategia

- fase 1: estrutura modular, conexao PostgreSQL e filtros globais
- fase 2: consultas reais sobre `boletim_de_urna` e `local_votacao`
- fase 3: placeholders documentados para perfil do eleitorado e mapa municipal por UF
- fase 4: validacao de importacao Python e diagnosticos
