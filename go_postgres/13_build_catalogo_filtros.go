package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

// Materializa combinações distintas ano/UF/município/cargo/candidato a partir
// do boletim_de_urna para acelerar os filtros do app Streamlit.
func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := pgEnv13("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnv13("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnv13("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnv13("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN13(host, port, user, password, dbname)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		log.Fatalf("erro ao conectar ao banco: %v", err)
	}

	_, _ = db.Exec(`SET search_path TO public`)
	_, _ = db.Exec(`SET lock_timeout = '300s'`)

	var n int
	if err := db.QueryRow(`
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'boletim_de_urna'
`).Scan(&n); err != nil {
		log.Fatalf("verificar boletim_de_urna: %v", err)
	}
	if n == 0 {
		log.Fatal(`tabela "boletim_de_urna" não existe — importe o boletim antes.`)
	}

	fmt.Println("=== Catálogo de filtros (catalogo_boletim) ===")

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS public.catalogo_boletim (
	ano VARCHAR(4) NOT NULL,
	sg_uf VARCHAR(2) NOT NULL,
	cd_municipio VARCHAR(20) NOT NULL,
	nm_municipio TEXT NOT NULL,
	cd_cargo VARCHAR(20) NOT NULL,
	ds_cargo TEXT NOT NULL,
	nr_votavel VARCHAR(20) NOT NULL,
	nm_votavel TEXT NOT NULL,
	sg_partido VARCHAR(50),
	total_votos BIGINT NOT NULL DEFAULT 0,
	CONSTRAINT catalogo_boletim_pkey PRIMARY KEY (ano, sg_uf, cd_municipio, cd_cargo, nr_votavel)
)`)
	if err != nil {
		log.Fatalf("criar catalogo_boletim: %v", err)
	}

	fmt.Println("Limpando catálogo anterior…")
	if _, err := db.Exec(`TRUNCATE public.catalogo_boletim`); err != nil {
		log.Fatalf("truncate catalogo_boletim: %v", err)
	}

	fmt.Println("Agregando combinações do boletim_de_urna (pode demorar)…")
	start := time.Now()
	res, err := db.Exec(`
INSERT INTO public.catalogo_boletim (
	ano, sg_uf, cd_municipio, nm_municipio,
	cd_cargo, ds_cargo, nr_votavel, nm_votavel, sg_partido, total_votos
)
SELECT
	b."ANO_ELEICAO",
	UPPER(TRIM(BOTH FROM b."SG_UF")),
	b."CD_MUNICIPIO",
	MAX(b."NM_MUNICIPIO"),
	b."CD_CARGO_PERGUNTA",
	MAX(b."DS_CARGO_PERGUNTA"),
	b."NR_VOTAVEL",
	MAX(b."NM_VOTAVEL"),
	MAX(b."SG_PARTIDO"),
	SUM(b."QT_VOTOS"::bigint)
FROM boletim_de_urna b
WHERE COALESCE(b."DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
  AND b."ANO_ELEICAO" IS NOT NULL AND TRIM(b."ANO_ELEICAO") <> ''
  AND b."SG_UF" IS NOT NULL AND TRIM(b."SG_UF") <> ''
  AND b."CD_MUNICIPIO" IS NOT NULL AND TRIM(b."CD_MUNICIPIO") <> ''
  AND b."CD_CARGO_PERGUNTA" IS NOT NULL AND TRIM(b."CD_CARGO_PERGUNTA") <> ''
  AND b."NR_VOTAVEL" IS NOT NULL AND TRIM(b."NR_VOTAVEL") <> ''
GROUP BY b."ANO_ELEICAO", UPPER(TRIM(BOTH FROM b."SG_UF")),
         b."CD_MUNICIPIO", b."CD_CARGO_PERGUNTA", b."NR_VOTAVEL"
`)
	if err != nil {
		log.Fatalf("popular catalogo_boletim: %v", err)
	}
	inserted, _ := res.RowsAffected()
	fmt.Printf("Linhas inseridas: %d (em %s)\n", inserted, time.Since(start).Round(time.Millisecond))

	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_catalogo_boletim_ano ON public.catalogo_boletim (ano)`,
		`CREATE INDEX IF NOT EXISTS idx_catalogo_boletim_ano_uf ON public.catalogo_boletim (ano, sg_uf)`,
		`CREATE INDEX IF NOT EXISTS idx_catalogo_boletim_ano_uf_mun ON public.catalogo_boletim (ano, sg_uf, cd_municipio)`,
		`CREATE INDEX IF NOT EXISTS idx_catalogo_boletim_filtro_cargo ON public.catalogo_boletim (ano, sg_uf, cd_municipio, cd_cargo)`,
		`CREATE INDEX IF NOT EXISTS idx_catalogo_boletim_nm_votavel ON public.catalogo_boletim (ano, sg_uf, cd_municipio, cd_cargo, nm_votavel)`,
	}
	for _, ddl := range indexes {
		if _, err := db.Exec(ddl); err != nil {
			log.Fatalf("criar índice: %v", err)
		}
	}

	var anos, ufs, muns, cargos, cands int64
	_ = db.QueryRow(`SELECT COUNT(DISTINCT ano) FROM public.catalogo_boletim`).Scan(&anos)
	_ = db.QueryRow(`SELECT COUNT(DISTINCT (ano, sg_uf)) FROM public.catalogo_boletim`).Scan(&ufs)
	_ = db.QueryRow(`SELECT COUNT(DISTINCT (ano, sg_uf, cd_municipio)) FROM public.catalogo_boletim`).Scan(&muns)
	_ = db.QueryRow(`SELECT COUNT(DISTINCT (ano, sg_uf, cd_municipio, cd_cargo)) FROM public.catalogo_boletim`).Scan(&cargos)
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.catalogo_boletim`).Scan(&cands)

	fmt.Printf("Anos distintos: %d\n", anos)
	fmt.Printf("Pares (ano, UF): %d\n", ufs)
	fmt.Printf("Trios (ano, UF, município): %d\n", muns)
	fmt.Printf("Quartetos (ano, UF, município, cargo): %d\n", cargos)
	fmt.Printf("Combinações candidato (linhas totais): %d\n", cands)
	fmt.Println("Catálogo pronto.")
}

func pgEnv13(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN13(host, port, user, password, database string) string {
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
