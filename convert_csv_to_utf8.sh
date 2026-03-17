#!/bin/bash

# ============================================================
# Converter CSVs para UTF-8 (in-place)
# ============================================================
# Converte CSVs de ISO-8859-1 (Latin1) para UTF-8, sobrescrevendo o original.
# - dados/consulta_cand_2024, dados/consulta_vagas_2024: CSVs diretos na pasta
# - bweb: CSVs dentro de subpastas (bweb/bweb_1t_AC_.../bweb_1t_AC_....csv)

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIRS=(
  "$BASE_DIR/dados/consulta_cand_2024"
  "$BASE_DIR/dados/consulta_vagas_2024"
  "$BASE_DIR/bweb"
)

# Latin1 = ISO-8859-1 (codificação comum dos CSVs do TSE)
SRC_ENCODING="ISO-8859-1"
DEST_ENCODING="UTF-8"
TMP_DIR="/tmp/csv_utf8_convert"
mkdir -p "$TMP_DIR"

echo "================================================"
echo "Converter CSVs para UTF-8"
echo "------------------------------------------------"
echo "Pastas:"
for d in "${INPUT_DIRS[@]}"; do
  echo "  - $d"
done
echo "Codificação: $SRC_ENCODING -> $DEST_ENCODING"
echo "================================================"
echo ""

for DIR in "${INPUT_DIRS[@]}"; do
  if [ ! -d "$DIR" ]; then
    echo "⚠️  Diretório não encontrado: $DIR"
    continue
  fi

  # CSVs diretos na pasta e (para bweb) CSVs um nível abaixo: pasta/nome/nome.csv
  shopt -s nullglob
  FILES=("$DIR"/*.csv "$DIR"/*/*.csv)
  shopt -u nullglob

  for SRC_FILE in "${FILES[@]}"; do
    BASENAME="$(basename "$SRC_FILE")"
    TMP_FILE="${TMP_DIR}/${BASENAME}"
    echo "Convertendo: $BASENAME"
    iconv -f "$SRC_ENCODING" -t "$DEST_ENCODING" "$SRC_FILE" > "$TMP_FILE"
    mv "$TMP_FILE" "$SRC_FILE"
    echo "  ✓ $BASENAME"
  done
done

echo ""
echo "================================================"
echo "Concluído!"
echo "================================================"
