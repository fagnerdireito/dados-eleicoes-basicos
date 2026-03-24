#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== 1/7 - Criando tabela estados (UF + DF) ==="
cargo run --release --quiet --bin 6_create_table_estados

echo ""
echo "=== 2/7 - Importando municípios (municipio_tse_ibge.csv) ==="
cargo run --release --quiet --bin 7_import_municipios

echo ""
echo "=== 3/7 - Importando boletim de urna ==="
cargo run --release --quiet --bin 1_import_boletim_urna

echo ""
echo "=== 4/7 - Importando consulta candidatos ==="
cargo run --release --quiet --bin 2_import_consulta_cand

echo ""
echo "=== 5/7 - Importando consulta vagas ==="
cargo run --release --quiet --bin 3_import_consulta_vagas

echo ""
echo "=== 6/7 - Criando tabela votos_candidatos ==="
cargo run --release --quiet --bin 4_create_table_votos_candidatos

echo ""
echo "=== 7/7 - Criando tabela votos_partido ==="
cargo run --release --quiet --bin 5_create_table_votos_partido

echo ""
echo "=== Tudo concluído com sucesso! ==="
