# Dados Eleitorais do TSE - Guia de Importação

## Visão Geral

Este documento explica como obter e importar os dados eleitorais do TSE (Tribunal Superior Eleitoral) para o banco de dados PostgreSQL `eleicoes`.

---

## Dados Disponíveis no Banco

O banco PostgreSQL `eleicoes` já contém:

| Tabela | Linhas Est. | Descrição |
|--------|-------------|-----------|
| `boletim_de_urna` | ~4M | Votos por seção |
| `consulta_cand` | ~463K | Dados de candidatos |
| `consulta_vagas` | ~16K | Vagas por cargo |
| `estados` | ~27 | Lista de estados |
| `local_votacao` | ~599K | Locais de votação (⚠️ VERIFICAR) |
| `municipios` | ~5K | Lista de municípios |
| `votos_candidatos` | ~138K | Votos agregados por candidato |
| `votos_partido` | ~26K | Votos agregados por partido |

---

## Dados que Podem Faltar

### 1. local_votacao (Locais de Votação)

**Verificar se tem dados:**
```sql
SELECT COUNT(*) FROM local_votacao;
```

Se retornar 0, importar de: https://dadosabertos.tse.jus.br/dataset/local-votacao

**Arquivo necessário:** `localvotacao_{ano}.csv`

**Por que é importante:**
- Mapa de bolhas por local de votação (Tab 5)
- Votos por bairro (Tab 9)
- Card local de votação (Tab 8)

### 2. Coordenadas GPS (NR_LATITUDE, NR_LONGITUDE)

**Verificar se existem:**
```sql
SELECT COUNT(*) FROM local_votacao 
WHERE "NR_LATITUDE" NOT IN ('-1', '') AND "NR_LATITUDE" IS NOT NULL;
```

Se retornar 0, os mapas interativos não funcionarão.

### 3. Perfil do Eleitorado (Abstenção)

**Não existe no banco atual.** Para obter:
- https://dadosabertos.tse.jus.br/dataset/perfil-eleitorado
- Criar nova tabela `perfil_eleitorado` com dados de idade/escolaridade

---

## Como Importar Dados do TSE

### Opção 1: Scripts Go (existentes)

Pasta: `../go_postgres/`

| Script | Função |
|--------|--------|
| `1_import_boletim_urna.go` | Importa BU (Boletim de Urna) |
| `2_import_consulta_cand.go` | Importa candidatos |
| `3_import_consulta_vagas.go` | Importa vagas |
| `4_create_table_votos_candidatos.go` | Cria view de votos |
| `5_create_table_votos_partido.go` | Cria view de votos por partido |
| `6_import_municipio_tse_ibge.go` | Importa municípios |
| `7_import_estados.go` | Importa estados |
| `8_import_local_votacao.go` | Importa locais de votação |
| `9_create_table_votos_bairro.go` | Cria view de votos por bairro |

**Executar:**
```bash
cd ../go_postgres
go run *.go
```

### Opção 2: Scripts Python (existentes)

Pasta raiz: `../`

| Script | Função |
|--------|--------|
| `import_boletim_urna.py` | Importa BU |
| `import_consulta_cand.py` | Importa candidatos |
| `import_consulta_vagas.py` | Importa vagas |
| `create_table_votos_candidatos.py` | Cria view |
| `create_table_votos_partido.py` | Cria view |

**Executar:**
```bash
cd ../
python import_boletim_urna.py
```

### Opção 3: Download Direto do TSE

Portal: https://dadosabertos.tse.jus.br/

**Datasets necessários:**

1. **BU (Boletim de Urna)**
   - Arquivo: `bu_{ano}.zip`
   - Campos: votos por candidato/seção

2. **Consulta Candidatos**
   - Arquivo: `consulta_cand_{ano}.zip`
   - Campos: dados pessoais, partido, cargo

3. **Local Votação**
   - Arquivo: `localvotacao_{ano}.zip`
   - Campos: escolas, bairros, GPS

4. **Perfil Eleitorado** (opcional)
   - Arquivo: `perfil_eleitorado_{ano}.zip`
   - Campos: idade, escolaridade, gênero

---

## Validação dos Dados

### Verificar anos disponíveis
```sql
SELECT DISTINCT "ANO_ELEICAO" FROM bulletin_de_urna ORDER BY 1;
```

### Verificar se local_votacao tem dados
```sql
SELECT COUNT(*) FROM local_votacao;
```

### Verificar coordenadas GPS
```sql
SELECT COUNT(*) FROM local_votacao 
WHERE "NR_LATITUDE" NOT IN ('-1', '') AND "NR_LATITUDE" IS NOT NULL;
```

### Verificar municipalities por UF
```sql
SELECT "SG_UF", COUNT(DISTINCT "CD_MUNICIPIO") as cnt
FROM bulletin_de_urna
GROUP BY "SG_UF"
ORDER BY cnt DESC;
```

---

## Placeholders Implementados

O app já contém tratamento para dados faltantes:

| Situação | Comportamento |
|----------|---------------|
| local_votacao vazio | Mapas mostram tabela em vez de mapa |
| Sem coordenadas GPS | Mostra lista em vez de bolhas |
| Ano anterior sem dados | Mostra só ano atual |
| Perfil não existe | Mostra mensagem explicativa |

---

## Scripts de Importação

### Script: 8_import_local_votacao.go

```go
// Estrutura para importar local_votacao
type LocalVotacao struct {
    AA_ELEICAO       string
    SG_UF            string
    CD_MUNICIPIO     string
    NM_MUNICIPIO     string
    NR_ZONA          string
    NR_SECAO         string
    NR_LOCAL_VOTACAO string
    NM_LOCAL_VOTACAO string
    NM_BAIRRO        string
    NR_LATITUDE      string
    NR_LONGITUDE     string
}
```

---

## Próximos Passos

1. **Verificar local_votacao:**
   ```sql
   SELECT COUNT(*) FROM local_votacao;
   ```

2. **Se vazio, baixar e importar:**
   - Baixe `localvotacao_2024.zip` do TSE
   - Execute `go run 8_import_local_votacao.go`

3. **Testar o dashboard:**
   ```bash
   cd app-python-minimax
   pip install -r requirements.txt
   streamlit run app.py
   ```

---

## Contato para Suporte

Se tiver dúvidas sobre importação de dados, consulte:
- Portal de Dados Abertos: https://dadosabertos.tse.jus.br/
- Repositório do projeto: `/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos`