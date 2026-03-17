#!/bin/bash

# ============================================================
# Importar CSVs do Google Storage para BigQuery
# ============================================================
# Configurações — ajuste conforme seu ambiente
export PROJECT_ID="elegis-1262"
DATASET="eleicoes2024"
GCS_BUCKET="gs://eleicoesdados/2024/boletins"

# Lista dos arquivos a importar (sem extensão = nome da tabela)
FILES=(
  "bweb_1t_AC_091020241636"
  "bweb_1t_AL_091020241636"
  "bweb_1t_GO_091020241636"
  "bweb_2t_AC_281020241046"
  "bweb_2t_GO_281020241046"
)

echo "================================================"
echo "Projeto  : $PROJECT_ID"
echo "Dataset  : $DATASET"
echo "Bucket   : $GCS_BUCKET"
echo "================================================"

# Garante que o dataset existe (cria se não existir)
echo ""
echo "Verificando dataset '$DATASET'..."
if ! bq --project_id="$PROJECT_ID" show "$DATASET" > /dev/null 2>&1; then
  echo "  → Dataset não encontrado. Criando..."
  bq --project_id="$PROJECT_ID" mk --dataset "$DATASET"
else
  echo "  ✓ Dataset já existe."
fi

echo ""

# Loop pelos arquivos
for TABLE in "${FILES[@]}"; do
  echo "--------------------------------------------"
  echo "Processando: $TABLE"

  # Verifica se a tabela já existe
  if bq --project_id="$PROJECT_ID" show "${DATASET}.${TABLE}" > /dev/null 2>&1; then
    echo "  ⚠️  Tabela '${DATASET}.${TABLE}' já existe. Pulando..."
    continue
  fi

  echo "  → Tabela não existe. Iniciando importação..."

  bq --project_id="$PROJECT_ID" load \
    --source_format=CSV \
    --field_delimiter=";" \
    --autodetect \
    --skip_leading_rows=1 \
    --max_bad_records=100 \
    "${DATASET}.${TABLE}" \
    "${GCS_BUCKET}/${TABLE}.csv"

  if [ $? -eq 0 ]; then
    echo "  ✓ Importado com sucesso: ${DATASET}.${TABLE}"
  else
    echo "  ✗ ERRO ao importar: ${TABLE}"
  fi
done

echo ""
echo "================================================"
echo "Concluído!"
echo "================================================"