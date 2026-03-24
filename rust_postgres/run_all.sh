#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== 1/5 - Importando boletim de urna ==="
cargo run --release --quiet --bin 1_import_boletim_urna

echo ""
echo "=== 2/5 - Importando consulta candidatos ==="
cargo run --release --quiet --bin 2_import_consulta_cand

echo ""
echo "=== 3/5 - Importando consulta vagas ==="
cargo run --release --quiet --bin 3_import_consulta_vagas

echo ""
echo "=== 4/5 - Criando tabela votos_candidatos ==="
cargo run --release --quiet --bin 4_create_table_votos_candidatos

echo ""
echo "=== 5/5 - Criando tabela votos_partido ==="
cargo run --release --quiet --bin 5_create_table_votos_partido

echo ""
echo "=== Tudo concluído com sucesso! ==="
