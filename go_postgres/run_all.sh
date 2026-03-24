#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== 1/7 - Importando boletim de urna ==="
go run 1_import_boletim_urna.go

echo ""
echo "=== 2/7 - Importando consulta candidatos ==="
go run 2_import_consulta_cand.go

echo ""
echo "=== 3/7 - Importando consulta vagas ==="
go run 3_import_consulta_vagas.go

echo ""
echo "=== 4/7 - Criando tabela votos_candidatos ==="
go run 4_create_table_votos_candidatos.go

echo ""
echo "=== 5/7 - Criando tabela votos_partido ==="
go run 5_create_table_votos_partido.go

echo ""
echo "=== 6/7 - Importando municipio_tse_ibge ==="
go run 6_import_municipio_tse_ibge.go

echo ""
echo "=== 7/7 - Importando estados (UF + DF) ==="
go run 7_import_estados.go

echo ""
echo "=== Tudo concluído com sucesso! ==="
