package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

const votosCandidatosDedupIndex42 = "ux_vc_dedup_compare_fields"

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := pgEnvNonEmpty42("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnvNonEmpty42("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnvNonEmpty42("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnvNonEmpty42("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN42(host, port, user, password, dbname)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}

	fmt.Println("=== Iniciando Processamento Sequencial com Insert Ignore (PostgreSQL) ===")

	_, _ = db.Exec(`SET lock_timeout = '300s'`)

	fmt.Println("Verificando índices de origem...")
	ensureIndex42(db, "boletim_de_urna", "idx_bu_etl_base", "ANO_ELEICAO, CD_MUNICIPIO")
	ensureIndex42(db, "consulta_cand", "idx_cc_etl_base", "ANO_ELEICAO, NR_CANDIDATO, SG_UF, CD_CARGO, SG_UE")

	fmt.Println("Garantindo tabela votos_candidatos...")
	if err := createVotosCandidatosTable42(db); err != nil {
		log.Fatalf("Erro ao criar tabela alvo: %v", err)
	}

	fmt.Println("Removendo duplicidades antigas antes do índice único...")
	if deleted, err := cleanupDuplicateVotosCandidatos42(db); err != nil {
		log.Fatalf("Erro ao limpar duplicidades antigas: %v", err)
	} else if deleted > 0 {
		fmt.Printf("Duplicidades removidas: %d\n", deleted)
	}

	fmt.Println("Garantindo índice único de comparação...")
	if err := ensureUniqueCompareIndex42(db); err != nil {
		log.Fatalf("Erro ao criar índice único de comparação: %v", err)
	}

	rows, err := db.Query(`SELECT DISTINCT "ANO_ELEICAO", "CD_MUNICIPIO" FROM "boletim_de_urna" WHERE "ANO_ELEICAO" IS NOT NULL ORDER BY "ANO_ELEICAO", "CD_MUNICIPIO"`)
	if err != nil {
		log.Fatalf("Erro ao listar municípios: %v", err)
	}

	type Mun struct{ Ano, Cod string }
	var muns []Mun
	for rows.Next() {
		var m Mun
		if err := rows.Scan(&m.Ano, &m.Cod); err == nil {
			muns = append(muns, m)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		log.Fatalf("Erro ao ler municípios: %v", err)
	}
	rows.Close()

	total := len(muns)
	fmt.Printf("Total de %d municípios para carregar.\n", total)

	start := time.Now()
	var totalInserted int64

	for i, m := range muns {
		inserted, err := insertIgnoreVotosCandidatos42(db, m.Ano, m.Cod)
		if err != nil {
			msg := err.Error()
			if strings.Contains(msg, "40P01") || strings.Contains(strings.ToLower(msg), "deadlock") {
				fmt.Printf("\nDeadlock em %s-%s, tentando novamente...", m.Ano, m.Cod)
				inserted, err = insertIgnoreVotosCandidatos42(db, m.Ano, m.Cod)
			}
			if err != nil {
				log.Printf("\nErro fatal no município %s-%s: %v", m.Ano, m.Cod, err)
				continue
			}
		}

		totalInserted += inserted

		if (i+1)%100 == 0 || i+1 == total {
			pct := float64(i+1) / float64(total) * 100
			fmt.Printf("\rProgresso: %d/%d (%.1f%%) - Inseridos agora: %d - Tempo: %v", i+1, total, pct, totalInserted, time.Since(start).Truncate(time.Second))
		}
	}
	fmt.Println()

	fmt.Println("Criando índices finais...")
	ensureIndex42(db, "votos_candidatos", "idx_vc_busca", "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO")
	ensureIndex42(db, "votos_candidatos", "idx_vc_uf", "SG_UF")

	fmt.Printf("Carga finalizada em %v! Total de novas linhas inseridas: %d\n", time.Since(start), totalInserted)
}

func createVotosCandidatosTable42(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS "votos_candidatos" (
			id BIGSERIAL PRIMARY KEY,
			"NM_URNA_CANDIDATO" VARCHAR(255),
			"NM_VOTAVEL" VARCHAR(255),
			"total_votos" BIGINT,
			"ANO_ELEICAO" VARCHAR(10),
			"NM_MUNICIPIO" VARCHAR(255),
			"CD_MUNICIPIO" VARCHAR(50),
			"CD_ELEICAO" VARCHAR(50),
			"NR_TURNO" VARCHAR(10),
			"SG_UF" VARCHAR(10),
			"DS_CARGO_PERGUNTA" VARCHAR(255),
			"SG_PARTIDO" VARCHAR(50),
			"SITUACAO_ELEICAO" VARCHAR(255)
		)`)
	return err
}

func cleanupDuplicateVotosCandidatos42(db *sql.DB) (int64, error) {
	result, err := db.Exec(`
		WITH ranked AS (
			SELECT
				id,
				ROW_NUMBER() OVER (
					PARTITION BY
						COALESCE("ANO_ELEICAO", ''),
						COALESCE("CD_MUNICIPIO", ''),
						COALESCE("NM_MUNICIPIO", ''),
						COALESCE("CD_ELEICAO", ''),
						COALESCE("NR_TURNO", ''),
						COALESCE("SG_UF", ''),
						COALESCE("DS_CARGO_PERGUNTA", ''),
						COALESCE("NM_VOTAVEL", '')
					ORDER BY id
				) AS rn
			FROM "votos_candidatos"
		)
		DELETE FROM "votos_candidatos" vc
		USING ranked r
		WHERE vc.id = r.id
			AND r.rn > 1`)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func ensureUniqueCompareIndex42(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS "%s"
		ON "votos_candidatos" (
			(COALESCE("ANO_ELEICAO", '')),
			(COALESCE("CD_MUNICIPIO", '')),
			(COALESCE("NM_MUNICIPIO", '')),
			(COALESCE("CD_ELEICAO", '')),
			(COALESCE("NR_TURNO", '')),
			(COALESCE("SG_UF", '')),
			(COALESCE("DS_CARGO_PERGUNTA", '')),
			(COALESCE("NM_VOTAVEL", ''))
		)`, votosCandidatosDedupIndex42))
	return err
}

func insertIgnoreVotosCandidatos42(db *sql.DB, ano, codMunicipio string) (int64, error) {
	result, err := db.Exec(`
		INSERT INTO "votos_candidatos" (
			"NM_URNA_CANDIDATO", "NM_VOTAVEL", "total_votos", "ANO_ELEICAO",
			"NM_MUNICIPIO", "CD_MUNICIPIO", "CD_ELEICAO", "NR_TURNO",
			"SG_UF", "DS_CARGO_PERGUNTA", "SG_PARTIDO", "SITUACAO_ELEICAO"
		)
		SELECT
			sub.nm_urna, sub.nm_vot, sub.total_votos, sub."ANO_ELEICAO",
			sub."NM_MUNICIPIO", sub."CD_MUNICIPIO", sub."CD_ELEICAO", sub."NR_TURNO",
			sub."SG_UF", sub."DS_CARGO_PERGUNTA", sub."SG_PARTIDO", sub."SITUACAO_ELEICAO"
		FROM (
			SELECT
				bu."NM_VOTAVEL" AS nm_urna,
				bu."NM_VOTAVEL" AS nm_vot,
				SUM(CAST(bu."QT_VOTOS" AS BIGINT)) AS total_votos,
				bu."ANO_ELEICAO",
				bu."NM_MUNICIPIO",
				bu."CD_MUNICIPIO",
				bu."CD_ELEICAO",
				bu."NR_TURNO",
				bu."SG_UF",
				MAX(bu."DS_CARGO_PERGUNTA") AS "DS_CARGO_PERGUNTA",
				MAX(cc."SG_PARTIDO") AS "SG_PARTIDO",
				MAX(cc."DS_SIT_TOT_TURNO") AS "SITUACAO_ELEICAO"
			FROM "boletim_de_urna" bu
			LEFT JOIN "consulta_cand" cc ON bu."ANO_ELEICAO" = cc."ANO_ELEICAO"
				AND bu."NR_VOTAVEL" = cc."NR_CANDIDATO"
				AND bu."SG_UF" = cc."SG_UF"
				AND bu."CD_CARGO_PERGUNTA" = cc."CD_CARGO"
				AND (
					cc."SG_UE" = LPAD(bu."CD_MUNICIPIO"::text, 5, '0')
					OR cc."SG_UE" = bu."SG_UF"
					OR cc."SG_UE" = 'BR'
				)
			WHERE bu."ANO_ELEICAO" = $1
				AND bu."CD_MUNICIPIO" = $2
			GROUP BY
				bu."ANO_ELEICAO",
				bu."CD_MUNICIPIO",
				bu."NM_MUNICIPIO",
				bu."CD_ELEICAO",
				bu."NR_TURNO",
				bu."SG_UF",
				bu."CD_CARGO_PERGUNTA",
				bu."NR_VOTAVEL",
				bu."NM_VOTAVEL"
		) sub
		ON CONFLICT (
			(COALESCE("ANO_ELEICAO", '')),
			(COALESCE("CD_MUNICIPIO", '')),
			(COALESCE("NM_MUNICIPIO", '')),
			(COALESCE("CD_ELEICAO", '')),
			(COALESCE("NR_TURNO", '')),
			(COALESCE("SG_UF", '')),
			(COALESCE("DS_CARGO_PERGUNTA", '')),
			(COALESCE("NM_VOTAVEL", ''))
		) DO NOTHING`, ano, codMunicipio)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func ensureIndex42(db *sql.DB, table, name, cols string) {
	parts := strings.Split(cols, ",")
	quoted := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		quoted = append(quoted, fmt.Sprintf(`"%s"`, t))
	}
	q := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" ON "%s" (%s)`, name, table, strings.Join(quoted, ", "))
	if _, err := db.Exec(q); err != nil {
		log.Printf("Aviso índice %s: %v", name, err)
	}
}

func pgEnvNonEmpty42(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN42(host, port, user, password, database string) string {
	u := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + database,
	}
	if password != "" {
		u.User = url.UserPassword(user, password)
	} else {
		u.User = url.User(user)
	}
	q := url.Values{}
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}
