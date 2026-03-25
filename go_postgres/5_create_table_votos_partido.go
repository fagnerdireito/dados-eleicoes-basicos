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

	host := pgEnvNonEmpty5("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnvNonEmpty5("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnvNonEmpty5("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnvNonEmpty5("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN5(host, port, user, password, dbname)
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

	fmt.Println("=== Iniciando Processamento Sequencial Estável (Votos Partido — PostgreSQL) ===")

	_, _ = db.Exec(`SET lock_timeout = '300s'`)

	fmt.Println("Verificando índices de origem...")
	ensureIndex5(db, "boletim_de_urna", "idx_bu_etl_partido", "ANO_ELEICAO, CD_MUNICIPIO, NR_PARTIDO")

	fmt.Println("Recriando tabela votos_partido...")
	_, _ = db.Exec(`DROP TABLE IF EXISTS "votos_partido"`)
	_, err = db.Exec(`
		CREATE TABLE "votos_partido" (
			id BIGSERIAL PRIMARY KEY,
			"NR_PARTIDO" VARCHAR(50),
			"SG_PARTIDO" VARCHAR(50),
			"NM_PARTIDO" VARCHAR(255),
			"total_votos" BIGINT,
			"ANO_ELEICAO" VARCHAR(10),
			"NM_MUNICIPIO" VARCHAR(255),
			"CD_MUNICIPIO" VARCHAR(50),
			"CD_ELEICAO" VARCHAR(50),
			"NR_TURNO" VARCHAR(10),
			"SG_UF" VARCHAR(10),
			"DS_CARGO_PERGUNTA" VARCHAR(255),
			"CD_CARGO_PERGUNTA" VARCHAR(50)
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
		INSERT INTO "votos_partido" (
			"NR_PARTIDO", "SG_PARTIDO", "NM_PARTIDO", "total_votos", "ANO_ELEICAO",
			"NM_MUNICIPIO", "CD_MUNICIPIO", "CD_ELEICAO", "NR_TURNO",
			"SG_UF", "DS_CARGO_PERGUNTA", "CD_CARGO_PERGUNTA"
		)
		SELECT
			bu."NR_PARTIDO", bu."SG_PARTIDO", bu."NM_PARTIDO", SUM(CAST(bu."QT_VOTOS" AS BIGINT)), bu."ANO_ELEICAO",
			bu."NM_MUNICIPIO", bu."CD_MUNICIPIO", bu."CD_ELEICAO", bu."NR_TURNO",
			bu."SG_UF", MAX(bu."DS_CARGO_PERGUNTA"), bu."CD_CARGO_PERGUNTA"
		FROM "boletim_de_urna" bu
		WHERE bu."ANO_ELEICAO" = $1 AND bu."CD_MUNICIPIO" = $2
		GROUP BY bu."ANO_ELEICAO", bu."CD_MUNICIPIO", bu."NM_MUNICIPIO", bu."CD_ELEICAO", bu."NR_TURNO", bu."SG_UF", bu."CD_CARGO_PERGUNTA", bu."NR_PARTIDO", bu."SG_PARTIDO", bu."NM_PARTIDO"`

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
	ensureIndex5(db, "votos_partido", "idx_vp_busca", "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO")
	ensureIndex5(db, "votos_partido", "idx_vp_partido", "SG_PARTIDO")

	fmt.Printf("Carga de partidos finalizada em %v!\n", time.Since(start))
}

func ensureIndex5(db *sql.DB, table, name, cols string) {
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

func pgEnvNonEmpty5(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN5(host, port, user, password, database string) string {
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
