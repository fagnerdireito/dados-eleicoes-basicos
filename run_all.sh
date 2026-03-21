#!/bin/bash
set -e

echo "=== 1/5 - Importando boletim de urna ==="
go run 1_import_boletim_urna.go

echo ""
echo "=== 2/5 - Importando consulta candidatos ==="
go run 2_import_consulta_cand.go

echo ""
echo "=== 3/5 - Importando consulta vagas ==="
go run 3_import_consulta_vagas.go

echo ""
echo "=== 4/5 - Criando tabela votos_candidatos ==="
go run 4_create_table_votos_candidatos.go

echo ""
echo "=== 5/5 - Criando tabela votos_partido ==="
go run 5_create_table_votos_partido.go

echo ""
echo "=== Tudo concluído com sucesso! ==="
