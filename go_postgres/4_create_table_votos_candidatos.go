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

	fmt.Println("=== Iniciando Processamento Sequencial Estável (PostgreSQL) ===")

	_, _ = db.Exec(`SET lock_timeout = '300s'`)

	fmt.Println("Verificando índices de origem...")
	ensureIndex(db, "boletim_de_urna", "idx_bu_etl_base", "ANO_ELEICAO, CD_MUNICIPIO")
	ensureIndex(db, "consulta_cand", "idx_cc_etl_base", "ANO_ELEICAO, NR_CANDIDATO, SG_UF, CD_CARGO, SG_UE")

	fmt.Println("Garantindo tabela votos_candidatos...")
	_, err = db.Exec(`
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
	if err != nil {
		log.Fatalf("Erro ao criar tabela alvo: %v", err)
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
	rows.Close()

	total := len(muns)
	fmt.Printf("Total de %d municípios para carregar.\n", total)

	insertSQL := `
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
			WHERE bu."ANO_ELEICAO" = $1 AND bu."CD_MUNICIPIO" = $2
			GROUP BY bu."ANO_ELEICAO", bu."CD_MUNICIPIO", bu."NM_MUNICIPIO", bu."CD_ELEICAO", bu."NR_TURNO", bu."SG_UF", bu."CD_CARGO_PERGUNTA", bu."NR_VOTAVEL", bu."NM_VOTAVEL"
		) sub
		WHERE NOT EXISTS (
			SELECT 1 FROM "votos_candidatos" vc
			WHERE vc."ANO_ELEICAO" IS NOT DISTINCT FROM sub."ANO_ELEICAO"
				AND vc."CD_MUNICIPIO" IS NOT DISTINCT FROM sub."CD_MUNICIPIO"
				AND vc."NM_MUNICIPIO" IS NOT DISTINCT FROM sub."NM_MUNICIPIO"
				AND vc."CD_ELEICAO" IS NOT DISTINCT FROM sub."CD_ELEICAO"
				AND vc."NR_TURNO" IS NOT DISTINCT FROM sub."NR_TURNO"
				AND vc."SG_UF" IS NOT DISTINCT FROM sub."SG_UF"
				AND vc."DS_CARGO_PERGUNTA" IS NOT DISTINCT FROM sub."DS_CARGO_PERGUNTA"
				AND vc."NM_VOTAVEL" IS NOT DISTINCT FROM sub.nm_vot
		)`

	start := time.Now()
	for i, m := range muns {
		_, err := db.Exec(insertSQL, m.Ano, m.Cod)

		if err != nil {
			msg := err.Error()
			if strings.Contains(msg, "40P01") || strings.Contains(strings.ToLower(msg), "deadlock") {
				fmt.Printf("\nDeadlock em %s-%s, tentando novamente...", m.Ano, m.Cod)
				_, err = db.Exec(insertSQL, m.Ano, m.Cod)
			}
			if err != nil {
				log.Printf("\nErro fatal no município %s-%s: %v", m.Ano, m.Cod, err)
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
	ensureIndex(db, "votos_candidatos", "idx_vc_busca", "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO")
	ensureIndex(db, "votos_candidatos", "idx_vc_uf", "SG_UF")

	fmt.Printf("Carga finalizada em %v!\n", time.Since(start))
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
