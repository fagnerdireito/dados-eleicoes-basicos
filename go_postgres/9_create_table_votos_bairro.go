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

// Gera a tabela votos_bairro: votos por candidato agregados por bairro.
// O bairro (NM_BAIRRO) vem de local_votacao, ligado ao boletim_de_urna
// pela chave de seção (ano/turno/uf/municipio/zona/secao).
//
// Observações importantes:
//   - local_votacao usa "AA_ELEICAO" para o ano; boletim_de_urna usa "ANO_ELEICAO".
//   - local_votacao só contém os dados de 2024, então só os boletins de 2024
//     casam no JOIN. Por isso iteramos apenas pelos anos presentes em
//     local_votacao e usamos JOIN (não LEFT JOIN) para não gerar linhas
//     com NM_BAIRRO nulo.
func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := pgEnvNonEmpty9("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnvNonEmpty9("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnvNonEmpty9("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnvNonEmpty9("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN9(host, port, user, password, dbname)
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

	fmt.Println("=== Iniciando carga de votos_bairro via procedure no PostgreSQL ===")

	_, _ = db.Exec(`SET lock_timeout = '300s'`)
	_, _ = db.Exec(`SET statement_timeout = '0'`)
	_, _ = db.Exec(`SET synchronous_commit = OFF`)

	if err := requireTable9(db, "boletim_de_urna"); err != nil {
		log.Fatal(err)
	}
	if err := requireTable9(db, "local_votacao"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Verificando índices de origem (chave de JOIN seção)...")
	// local_votacao: o 8_ já cria idx_unique_local_votacao com exatamente
	// (AA_ELEICAO, NR_TURNO, SG_UF, CD_MUNICIPIO, NR_ZONA, NR_SECAO).
	// Garantimos um índice equivalente no boletim para acelerar o JOIN.
	ensureIndex9(db, "boletim_de_urna", "idx_bu_secao_join",
		"ANO_ELEICAO, SG_UF, CD_MUNICIPIO, NR_ZONA, NR_SECAO, NR_TURNO")

	fmt.Println("Recriando tabela votos_bairro para carga cheia...")
	if err := recreateTargetTable9(db); err != nil {
		log.Fatalf("Erro ao recriar tabela alvo: %v", err)
	}

	fmt.Println("Atualizando estatísticas das tabelas de origem...")
	_, _ = db.Exec(`ANALYZE "boletim_de_urna"`)
	_, _ = db.Exec(`ANALYZE "local_votacao"`)

	fmt.Println("Criando procedure de carga...")
	if err := createLoadProcedure9(db); err != nil {
		log.Fatalf("Erro ao criar procedure: %v", err)
	}

	// Iteramos apenas pelos anos que existem em local_votacao (AA_ELEICAO),
	// pois só esses produzem bairros no JOIN.
	rows, err := db.Query(`SELECT DISTINCT "AA_ELEICAO" FROM "local_votacao" WHERE "AA_ELEICAO" IS NOT NULL ORDER BY "AA_ELEICAO"`)
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
	if total == 0 {
		log.Fatal("Nenhum ano encontrado em local_votacao; importe a base (8_import_local_votacao) antes.")
	}
	fmt.Printf("Total de %d ano(s) para carregar: %s\n", total, strings.Join(anos, ", "))

	start := time.Now()
	for i, ano := range anos {
		_, err := db.Exec(`CALL "load_votos_bairro_by_year"($1)`, ano)
		if err != nil {
			msg := err.Error()
			if strings.Contains(msg, "40P01") || strings.Contains(strings.ToLower(msg), "deadlock") {
				fmt.Printf("\nDeadlock no ano %s, tentando novamente...", ano)
				_, err = db.Exec(`CALL "load_votos_bairro_by_year"($1)`, ano)
			}
			if err != nil {
				log.Printf("\nErro fatal no ano %s: %v", ano, err)
				continue
			}
		}

		pct := float64(i+1) / float64(total) * 100
		fmt.Printf("\rProgresso: %d/%d (%.1f%%) - Tempo: %v", i+1, total, pct, time.Since(start).Truncate(time.Second))
	}
	fmt.Println()

	fmt.Println("Criando índices finais...")
	ensureIndex9(db, "votos_bairro", "idx_vb_busca", "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO, CD_CARGO_PERGUNTA")
	ensureIndex9(db, "votos_bairro", "idx_vb_bairro", "CD_MUNICIPIO, NM_BAIRRO")
	ensureIndex9(db, "votos_bairro", "idx_vb_cand", "ANO_ELEICAO, CD_MUNICIPIO, NR_VOTAVEL")
	_, _ = db.Exec(`ANALYZE "votos_bairro"`)

	var totalRows int64
	_ = db.QueryRow(`SELECT COUNT(*) FROM "votos_bairro"`).Scan(&totalRows)

	fmt.Printf("Carga finalizada em %v! Linhas em votos_bairro: %d\n", time.Since(start), totalRows)
}

func recreateTargetTable9(db *sql.DB) error {
	if _, err := db.Exec(`DROP TABLE IF EXISTS "votos_bairro"`); err != nil {
		return err
	}

	_, err := db.Exec(`
		CREATE TABLE "votos_bairro" (
			id BIGSERIAL PRIMARY KEY,
			"NM_BAIRRO" VARCHAR(80),
			"NM_URNA_CANDIDATO" VARCHAR(255),
			"NM_VOTAVEL" VARCHAR(255),
			"NR_VOTAVEL" VARCHAR(50),
			"total_votos" BIGINT,
			"ANO_ELEICAO" VARCHAR(10),
			"NM_MUNICIPIO" VARCHAR(255),
			"CD_MUNICIPIO" VARCHAR(50),
			"NR_TURNO" VARCHAR(10),
			"SG_UF" VARCHAR(10),
			"CD_CARGO_PERGUNTA" VARCHAR(50),
			"DS_CARGO_PERGUNTA" VARCHAR(255)
		)
	`)
	return err
}

func requireTable9(db *sql.DB, table string) error {
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
		return fmt.Errorf("tabela %q nao existe no schema public; importe a base antes de gerar votos_bairro", table)
	}
	return nil
}

func createLoadProcedure9(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE PROCEDURE "load_votos_bairro_by_year"(p_ano VARCHAR)
		LANGUAGE SQL
		AS $$
			INSERT INTO "votos_bairro" (
				"NM_BAIRRO",
				"NM_URNA_CANDIDATO",
				"NM_VOTAVEL",
				"NR_VOTAVEL",
				"total_votos",
				"ANO_ELEICAO",
				"NM_MUNICIPIO",
				"CD_MUNICIPIO",
				"NR_TURNO",
				"SG_UF",
				"CD_CARGO_PERGUNTA",
				"DS_CARGO_PERGUNTA"
			)
			SELECT
				lv."NM_BAIRRO",
				bu."NM_VOTAVEL" AS "NM_URNA_CANDIDATO",
				bu."NM_VOTAVEL" AS "NM_VOTAVEL",
				bu."NR_VOTAVEL",
				SUM(CAST(bu."QT_VOTOS" AS BIGINT)) AS "total_votos",
				bu."ANO_ELEICAO",
				bu."NM_MUNICIPIO",
				bu."CD_MUNICIPIO",
				bu."NR_TURNO",
				bu."SG_UF",
				bu."CD_CARGO_PERGUNTA",
				MAX(bu."DS_CARGO_PERGUNTA") AS "DS_CARGO_PERGUNTA"
			FROM "boletim_de_urna" bu
			JOIN "local_votacao" lv
				ON lv."AA_ELEICAO"   = bu."ANO_ELEICAO"
				AND lv."NR_TURNO"     = bu."NR_TURNO"
				AND lv."SG_UF"        = bu."SG_UF"
				AND lv."CD_MUNICIPIO" = bu."CD_MUNICIPIO"
				AND lv."NR_ZONA"      = bu."NR_ZONA"
				AND lv."NR_SECAO"     = bu."NR_SECAO"
			WHERE bu."ANO_ELEICAO" = p_ano
			GROUP BY
				lv."NM_BAIRRO",
				bu."ANO_ELEICAO",
				bu."CD_MUNICIPIO",
				bu."NM_MUNICIPIO",
				bu."NR_TURNO",
				bu."SG_UF",
				bu."CD_CARGO_PERGUNTA",
				bu."NR_VOTAVEL",
				bu."NM_VOTAVEL";
		$$
	`)
	return err
}

func ensureIndex9(db *sql.DB, table, name, cols string) {
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

func pgEnvNonEmpty9(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN9(host, port, user, password, database string) string {
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
