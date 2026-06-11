# Dados TSE faltantes

Inventario realizado no PostgreSQL configurado por `PGSQL_VECTOR_*`.

## Cobertura atual

| Conjunto | Cobertura observada |
|---|---|
| `boletim_de_urna` | 2020, 2022 e 2024; cobertura parcial de UFs |
| `votos_candidatos` | 2020, 2022 e 2024 |
| `votos_partido` | 2020, 2022 e 2024 |
| `perfil_eleitorado` | 2022 e 2024 |
| `consulta_cand` | 2024 |
| `consulta_vagas` | 2024 |
| `local_votacao` | 2024 |

O boletim possui AC em 2020 e AC/GO em 2022 e 2024. Nao ha cobertura nacional completa.

## Downloads prioritarios no Portal de Dados Abertos do TSE

### 1. Boletim de urna

Baixar os pacotes de **Boletim de Urna** para as UFs e turnos ainda ausentes:

```text
bweb_1t_<UF>_<data>.csv
bweb_2t_<UF>_<data>.csv
```

Necessario para resultados, rankings, comparecimento total e distribuicao territorial.

### 2. Locais de votacao

Baixar **Eleitorado - Local de votacao**:

```text
localvotacao_2020.csv
localvotacao_2022.csv
```

Necessario para nomes de escolas, bairros, enderecos, latitude e longitude nas eleicoes anteriores a 2024.

### 3. Perfil do eleitorado

Baixar:

```text
perfil_eleitorado_2020.zip
```

Necessario para composicao cadastral por idade, escolaridade, genero e outros recortes em 2020.

### 4. Comparecimento e abstencao por perfil

Baixar os conjuntos de **Comparecimento e Abstencao** por secao/perfil para 2020, 2022 e 2024.

Esse conjunto e indispensavel para reproduzir corretamente as barras de **abstencao por faixa etaria** e **abstencao por escolaridade** da imagem. A tabela `perfil_eleitorado` so informa quantidade cadastrada em cada perfil e nao permite inferir quem compareceu.

### 5. Candidatos e vagas

Baixar:

```text
consulta_cand_2020_<UF>.csv
consulta_cand_2022_<UF>.csv
consulta_vagas_2020.csv
consulta_vagas_2022.csv
```

Necessario para fotos/metadados futuros, situacao da candidatura e quantidade de vagas nos anos anteriores.

## Placeholders existentes

- Perfil demografico ausente no ano/UF.
- Abstencao demografica ainda nao importada.
- Local, bairro ou coordenada ausente em 2020/2022.
- Historico sem ciclo anterior no banco.
- Mapa estadual sem GeoJSON empacotado para nova UF.
- Abas municipais sem cidade selecionada.

## Importacao

Este app nao importa dados nem cria tabelas. Os scripts de pipeline devem ser executados fora dele e revisados separadamente. Nenhum placeholder esconde erro SQL: falhas de consulta aparecem na interface como diagnostico.
