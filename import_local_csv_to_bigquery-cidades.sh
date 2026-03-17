#!/bin/bash

# ============================================================
# Importar CSVs locais diretamente para o BigQuery (UTF-8)
# ============================================================
# - Converte arquivos para UTF-8 antes de enviar
# - Usa bq load direto do filesystem local (sem passar pelo GCS)
#
# Pré-requisitos:
# - gcloud/bq instalados e autenticados
# - Projeto com BigQuery habilitado

set -euo pipefail

# ------------------------------------------------------------
# Configurações — ajuste conforme seu ambiente
# ------------------------------------------------------------
export PROJECT_ID="elegis-1262"
DATASET="eleicoes2024"

# Pastas com os CSVs locais
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIRS=(
  "$BASE_DIR/dados/municipio_tse_ibge"
)

# Codificação de origem presumida dos arquivos
SRC_ENCODING="ISO-8859-1"
DEST_ENCODING="UTF-8"

# Diretório temporário para arquivos já convertidos em UTF-8
TMP_DIR="/tmp/bq_utf8_import_eleicoes2024"
mkdir -p "$TMP_DIR"

echo "================================================"
echo "Importar CSVs locais para BigQuery"
echo "------------------------------------------------"
echo "Projeto       : $PROJECT_ID"
echo "Dataset       : $DATASET"
echo "Pastas origem :"
for d in "${INPUT_DIRS[@]}"; do
  echo "  - $d"
done
echo "Codificação   : $SRC_ENCODING -> $DEST_ENCODING"
echo "Temp dir      : $TMP_DIR"
echo "================================================"

# ------------------------------------------------------------
# Garante que o dataset existe (cria se não existir)
# ------------------------------------------------------------
echo ""
echo "Verificando dataset '$DATASET'..."
if ! bq --project_id="$PROJECT_ID" show "$DATASET" > /dev/null 2>&1; then
  echo "  → Dataset não encontrado. Criando..."
  bq --project_id="$PROJECT_ID" mk --dataset "$DATASET"
else
  echo "  ✓ Dataset já existe."
fi

echo ""

# ------------------------------------------------------------
# Função para processar uma pasta
# ------------------------------------------------------------
process_dir() {
  local DIR="$1"

  if [ ! -d "$DIR" ]; then
    echo "⚠️  Diretório não encontrado: $DIR (pulando)"
    return
  fi

  shopt -s nullglob
  local FILES=("$DIR"/*.csv)
  shopt -u nullglob

  if [ ${#FILES[@]} -eq 0 ]; then
    echo "⚠️  Nenhum CSV encontrado em: $DIR"
    return
  fi

  for SRC_FILE in "${FILES[@]}"; do
    local BASENAME
    BASENAME="$(basename "$SRC_FILE")"
    local TABLE_NAME="${BASENAME%.csv}"

    echo "--------------------------------------------"
    echo "Pasta   : $DIR"
    echo "Arquivo : $BASENAME"
    echo "Tabela  : ${DATASET}.${TABLE_NAME}"

    # Verifica se a tabela já existe
    if bq --project_id="$PROJECT_ID" show "${DATASET}.${TABLE_NAME}" > /dev/null 2>&1; then
      echo "  ⚠️  Tabela já existe. Pulando..."
      continue
    fi

    # Converte para UTF-8 em arquivo temporário
    local TMP_FILE="${TMP_DIR}/${TABLE_NAME}.utf8.csv"
    echo "  → Convertendo para UTF-8: $TMP_FILE"
    # No macOS, o iconv não aceita opção -o; precisamos redirecionar a saída
    if ! iconv -f "$SRC_ENCODING" -t "$DEST_ENCODING" "$SRC_FILE" > "$TMP_FILE"; then
      echo "  ✗ ERRO ao converter arquivo: $SRC_FILE (pulando)"
      continue
    fi

    echo "  → Importando para BigQuery..."
    if bq --project_id="$PROJECT_ID" load \
      --source_format=CSV \
      --field_delimiter=";" \
      --autodetect \
      --skip_leading_rows=1 \
      --encoding="$DEST_ENCODING" \
      --max_bad_records=100 \
      "${DATASET}.${TABLE_NAME}" \
      "$TMP_FILE"; then
      echo "  ✓ Importado com sucesso: ${DATASET}.${TABLE_NAME}"
    else
      echo "  ✗ ERRO ao importar tabela: ${DATASET}.${TABLE_NAME}"
    fi
  done
}

# ------------------------------------------------------------
# Loop principal nas pastas de entrada
# ------------------------------------------------------------
for DIR in "${INPUT_DIRS[@]}"; do
  echo ""
  echo "========== Processando diretório: $DIR =========="
  process_dir "$DIR"
done

echo ""
echo "================================================"
echo "Concluído!"
echo "================================================"

