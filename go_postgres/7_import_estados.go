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

// Código IBGE da UF (id) e nome oficial — boletim_de_urna só traz SG_UF.
var ufPorSigla = map[string]struct {
	ID   int64
	Nome string
}{
	"RO": {11, "Rondônia"},
	"AC": {12, "Acre"},
	"AM": {13, "Amazonas"},
	"RR": {14, "Roraima"},
	"PA": {15, "Pará"},
	"AP": {16, "Amapá"},
	"TO": {17, "Tocantins"},
	"MA": {21, "Maranhão"},
	"PI": {22, "Piauí"},
	"CE": {23, "Ceará"},
	"RN": {24, "Rio Grande do Norte"},
	"PB": {25, "Paraíba"},
	"PE": {26, "Pernambuco"},
	"AL": {27, "Alagoas"},
	"SE": {28, "Sergipe"},
	"BA": {29, "Bahia"},
	"MG": {31, "Minas Gerais"},
	"ES": {32, "Espírito Santo"},
	"RJ": {33, "Rio de Janeiro"},
	"SP": {35, "São Paulo"},
	"PR": {41, "Paraná"},
	"SC": {42, "Santa Catarina"},
	"RS": {43, "Rio Grande do Sul"},
	"MS": {50, "Mato Grosso do Sul"},
	"MT": {51, "Mato Grosso"},
	"GO": {52, "Goiás"},
	"DF": {53, "Distrito Federal"},
}

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := pgEnvNonEmpty7("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnvNonEmpty7("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnvNonEmpty7("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnvNonEmpty7("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN7(host, port, user, password, dbname)
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

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS public.estados (
	id BIGINT NOT NULL PRIMARY KEY,
	sigla VARCHAR(2) NOT NULL,
	"NM_ESTADO" VARCHAR(100) NOT NULL,
	CONSTRAINT estados_sigla_unique UNIQUE (sigla)
)`)
	if err != nil {
		log.Fatalf("criar tabela estados (se não existir): %v", err)
	}
	fmt.Println(`Tabela "estados" verificada (CREATE IF NOT EXISTS).`)

	var n int
	if err := db.QueryRow(`
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'boletim_de_urna'
`).Scan(&n); err != nil {
		log.Fatalf("verificar boletim_de_urna: %v", err)
	}
	if n == 0 {
		log.Fatal(`tabela "boletim_de_urna" não existe — importe o boletim antes (1_import_boletim_urna).`)
	}

	rows, err := db.Query(`
SELECT DISTINCT UPPER(TRIM(BOTH FROM "SG_UF")) AS sg
FROM "boletim_de_urna"
WHERE "SG_UF" IS NOT NULL AND TRIM(BOTH FROM "SG_UF") <> ''
ORDER BY 1
`)
	if err != nil {
		log.Fatalf("listar UFs distintas do boletim: %v", err)
	}
	defer rows.Close()

	var siglas []string
	for rows.Next() {
		var sg sql.NullString
		if err := rows.Scan(&sg); err != nil {
			log.Fatalf("ler UF: %v", err)
		}
		if !sg.Valid {
			continue
		}
		u := strings.TrimSpace(sg.String)
		if u == "" {
			continue
		}
		siglas = append(siglas, u)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterar UFs: %v", err)
	}

	if len(siglas) == 0 {
		fmt.Println("Nenhuma SG_UF distinta em boletim_de_urna; nada a inserir.")
		return
	}

	const ins = `INSERT INTO public.estados (id, sigla, "NM_ESTADO") VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`

	var inserted, skippedUnknown, skippedBad int64
	for _, sg := range siglas {
		if len(sg) != 2 {
			skippedBad++
			continue
		}
		info, ok := ufPorSigla[sg]
		if !ok {
			skippedUnknown++
			continue
		}
		res, err := db.Exec(ins, info.ID, sg, info.Nome)
		if err != nil {
			log.Fatalf("inserir estado %s: %v", sg, err)
		}
		aff, _ := res.RowsAffected()
		inserted += aff
	}

	var total int64
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.estados`).Scan(&total)

	fmt.Printf("UFs distintas no boletim: %d\n", len(siglas))
	fmt.Printf("Linhas inseridas nesta execução (novas): %d\n", inserted)
	if skippedUnknown > 0 {
		fmt.Printf("Siglas ignoradas (fora do mapa IBGE): %d\n", skippedUnknown)
	}
	if skippedBad > 0 {
		fmt.Printf("Valores SG_UF ignorados (≠ 2 caracteres após trim): %d\n", skippedBad)
	}
	fmt.Printf("Total de linhas em public.estados: %d\n", total)
}

func pgEnvNonEmpty7(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN7(host, port, user, password, database string) string {
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
