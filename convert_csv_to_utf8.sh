#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIRS=(
  # "$BASE_DIR/dados/teste"
  "$BASE_DIR/dados/consulta_cand_2024"
  "$BASE_DIR/dados/consulta_vagas_2024"
  "$BASE_DIR/bweb"
)

SRC_ENCODING="ISO-8859-1"
DEST_ENCODING="UTF-8"
TMP_DIR="/tmp/csv_utf8_convert"
mkdir -p "$TMP_DIR"

is_utf8() {
  local f="$1"
  iconv -f UTF-8 -t UTF-8 "$f" > /dev/null 2>&1
}

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

  shopt -s nullglob
  FILES=("$DIR"/*.csv "$DIR"/*/*.csv)
  shopt -u nullglob

  for SRC_FILE in "${FILES[@]}"; do
    BASENAME="$(basename "$SRC_FILE")"

    if is_utf8 "$SRC_FILE"; then
      echo "Pulando (já UTF-8): $BASENAME"
      continue
    fi

    echo "Convertendo: $BASENAME"

    TMP_FILE="$(mktemp "$TMP_DIR/${BASENAME}.XXXXXX")"

    iconv -f "$SRC_ENCODING" -t "$DEST_ENCODING" "$SRC_FILE" > "$TMP_FILE"

    mv "$TMP_FILE" "$SRC_FILE"
    echo "  ✓ $BASENAME"
  done
done

echo ""
echo "================================================"
echo "Concluído!"
echo "================================================"