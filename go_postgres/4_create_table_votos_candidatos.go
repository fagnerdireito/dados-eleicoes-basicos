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

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := pgEnvNonEmpty4("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnvNonEmpty4("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnvNonEmpty4("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnvNonEmpty4("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN4(host, port, user, password, dbname)
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

	fmt.Println("=== Iniciando carga via procedure no PostgreSQL ===")

	_, _ = db.Exec(`SET lock_timeout = '300s'`)
	_, _ = db.Exec(`SET statement_timeout = '0'`)
	_, _ = db.Exec(`SET synchronous_commit = OFF`)

	fmt.Println("Verificando índices de origem...")
	ensureIndex(db, "boletim_de_urna", "idx_bu_etl_base", "ANO_ELEICAO, CD_MUNICIPIO")
	ensureIndex(db, "boletim_de_urna", "idx_bu_vc_join", "ANO_ELEICAO, NR_VOTAVEL, SG_UF, CD_CARGO_PERGUNTA, CD_MUNICIPIO")
	ensureIndex(db, "consulta_cand", "idx_cc_etl_base", "ANO_ELEICAO, NR_CANDIDATO, SG_UF, CD_CARGO, SG_UE")

	if err := requireTable(db, "boletim_de_urna"); err != nil {
		log.Fatal(err)
	}
	if err := requireTable(db, "consulta_cand"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Recriando tabela votos_candidatos para carga cheia...")
	if err := recreateTargetTable(db); err != nil {
		log.Fatalf("Erro ao recriar tabela alvo: %v", err)
	}

	fmt.Println("Atualizando estatísticas das tabelas de origem...")
	_, _ = db.Exec(`ANALYZE "boletim_de_urna"`)
	_, _ = db.Exec(`ANALYZE "consulta_cand"`)

	fmt.Println("Criando procedure de carga...")
	if err := createLoadProcedure(db); err != nil {
		log.Fatalf("Erro ao criar procedure: %v", err)
	}

	rows, err := db.Query(`SELECT DISTINCT "ANO_ELEICAO" FROM "boletim_de_urna" WHERE "ANO_ELEICAO" IS NOT NULL ORDER BY "ANO_ELEICAO"`)
	if err != nil {
		log.Fatalf("Erro ao listar anos: %v", err)
	}

	var anos []string
	for rows.Next() {
		var ano string
		if err := rows.Scan(&ano); err == nil {
			anos = append(anos, ano)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		log.Fatalf("Erro ao ler anos: %v", err)
	}
	rows.Close()

	total := len(anos)
	fmt.Printf("Total de %d ano(s) para carregar.\n", total)

	start := time.Now()
	for i, ano := range anos {
		_, err := db.Exec(`CALL "load_votos_candidatos_by_year"($1)`, ano)
		if err != nil {
			msg := err.Error()
			if strings.Contains(msg, "40P01") || strings.Contains(strings.ToLower(msg), "deadlock") {
				fmt.Printf("\nDeadlock no ano %s, tentando novamente...", ano)
				_, err = db.Exec(`CALL "load_votos_candidatos_by_year"($1)`, ano)
			}
			if err != nil {
				log.Printf("\nErro fatal no ano %s: %v", ano, err)
				continue
			}
		}

		if (i+1)%100 == 0 || i+1 == total {
			pct := float64(i+1) / float64(total) * 100
			fmt.Printf("\rProgresso: %d/%d (%.1f%%) - Tempo: %v", i+1, total, pct, time.Since(start).Truncate(time.Second))
		}
	}
	fmt.Println()

	fmt.Println("Criando índices finais...")
	ensureIndex(db, "votos_candidatos", "idx_vc_ano", "ANO_ELEICAO")
	ensureIndex(db, "votos_candidatos", "idx_vc_busca", "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO")
	ensureIndex(db, "votos_candidatos", "idx_vc_uf", "SG_UF")
	_, _ = db.Exec(`ANALYZE "votos_candidatos"`)

	fmt.Printf("Carga finalizada em %v!\n", time.Since(start))
}

func recreateTargetTable(db *sql.DB) error {
	if _, err := db.Exec(`DROP TABLE IF EXISTS "votos_candidatos"`); err != nil {
		return err
	}

	_, err := db.Exec(`
		CREATE TABLE "votos_candidatos" (
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
		)
	`)
	return err
}

func requireTable(db *sql.DB, table string) error {
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public'
				AND table_name = $1
		)
	`, table).Scan(&exists)
	if err != nil {
		return fmt.Errorf("erro ao verificar tabela %s: %w", table, err)
	}
	if !exists {
		return fmt.Errorf("tabela %q nao existe no schema public; importe a base antes de gerar votos_candidatos", table)
	}
	return nil
}

func createLoadProcedure(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE PROCEDURE "load_votos_candidatos_by_year"(p_ano VARCHAR)
		LANGUAGE SQL
		AS $$
			INSERT INTO "votos_candidatos" (
				"NM_URNA_CANDIDATO",
				"NM_VOTAVEL",
				"total_votos",
				"ANO_ELEICAO",
				"NM_MUNICIPIO",
				"CD_MUNICIPIO",
				"CD_ELEICAO",
				"NR_TURNO",
				"SG_UF",
				"DS_CARGO_PERGUNTA",
				"SG_PARTIDO",
				"SITUACAO_ELEICAO"
			)
			SELECT
				bu."NM_VOTAVEL" AS "NM_URNA_CANDIDATO",
				bu."NM_VOTAVEL" AS "NM_VOTAVEL",
				SUM(CAST(bu."QT_VOTOS" AS BIGINT)) AS "total_votos",
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
			WHERE bu."ANO_ELEICAO" = p_ano
			GROUP BY
				bu."ANO_ELEICAO",
				bu."CD_MUNICIPIO",
				bu."NM_MUNICIPIO",
				bu."CD_ELEICAO",
				bu."NR_TURNO",
				bu."SG_UF",
				bu."CD_CARGO_PERGUNTA",
				bu."NR_VOTAVEL",
				bu."NM_VOTAVEL";
		$$
	`)
	return err
}

func ensureIndex(db *sql.DB, table, name, cols string) {
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

func pgEnvNonEmpty4(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN4(host, port, user, password, database string) string {
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
