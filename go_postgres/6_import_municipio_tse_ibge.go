package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Layout municipio_tse_ibge.csv (separador ;):
// 0 DT_GERACAO, 1 HH_GERACAO, 2 CD_UF_TSE, 3 CD_UF_IBGE, 4 SG_UF, 5 NM_UF,
// 6 CD_MUNICIPIO_TSE, 7 NM_MUNICIPIO_TSE, 8 CD_MUNICIPIO_IBGE, 9 NM_MUNICIPIO_IBGE.
const (
	idxSGUF       = 4
	idxCDMunTSE   = 6
	idxNMunTSE    = 7
	csvSep        = ';'
	batchMaxRows  = 500
	pgMaxParams   = 65535
)

// Maiúsculas para o nome do município (regras Unicode pt-BR).
var nomeMunUpper = cases.Upper(language.BrazilianPortuguese)

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := pgEnvNonEmpty6("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := pgEnvNonEmpty6("PGSQL_VECTOR_PORT", "5432")
	dbname := pgEnvNonEmpty6("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := pgEnvNonEmpty6("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := postgresDSN6(host, port, user, password, dbname)
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

	csvPath, err := resolveMunicipioCSVPath()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("CSV: %s\n", csvPath)

	text, err := readCSVText(csvPath)
	if err != nil {
		log.Fatalf("ler CSV: %v", err)
	}

	r := csv.NewReader(strings.NewReader(text))
	r.Comma = csvSep
	r.LazyQuotes = true

	header, err := r.Read()
	if err != nil {
		log.Fatalf("CSV vazio ou inválido: %v", err)
	}
	firstIsHeader := len(header) > 0 && strings.EqualFold(strings.Trim(strings.TrimSpace(header[0]), `"`), "DT_GERACAO")
	if firstIsHeader {
		fmt.Println("Cabeçalho detectado (DT_GERACAO); linhas de dados usam SG_UF, CD_MUNICIPIO_TSE, NM_MUNICIPIO_TSE.")
	}

	_, _ = db.Exec(`SET search_path TO public`)

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS public.municipios (
	id BIGSERIAL PRIMARY KEY,
	estado_id BIGINT NOT NULL REFERENCES public.estados(id) ON DELETE RESTRICT,
	codigo_tse VARCHAR(20) NOT NULL,
	nome TEXT NOT NULL,
	CONSTRAINT municipios_estado_codigo_key UNIQUE (estado_id, codigo_tse)
)`)
	if err != nil {
		log.Fatalf("criar tabela municipios (se não existir): %v", err)
	}
	_, _ = db.Exec(`ALTER TABLE public.municipios ALTER COLUMN nome TYPE TEXT`)

	ufToID, err := loadUFToEstadoID(db)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if len(ufToID) == 0 {
		log.Fatal(`tabela "estados" vazia ou inexistente — carregue estados antes (ex.: script de estados no postgres).`)
	}
	fmt.Printf("Mapeamento UF → estado_id: %d siglas.\n", len(ufToID))

	maxBatch := batchMaxRows
	if maxBatch*3 > pgMaxParams {
		maxBatch = pgMaxParams / 3
	}

	var (
		batchEstado []int64
		batchCod    []string
		batchNome   []string
		inserted    int64
		skippedUF   int64
		skippedEmp  int64
		firstBadUF  string
	)

	flush := func() error {
		if len(batchEstado) == 0 {
			return nil
		}
		sqlStr, args := buildInsertMunicipiosSQL(batchEstado, batchCod, batchNome)
		res, err := db.Exec(sqlStr, args...)
		if err != nil {
			return err
		}
		aff, _ := res.RowsAffected()
		inserted += aff
		batchEstado = batchEstado[:0]
		batchCod = batchCod[:0]
		batchNome = batchNome[:0]
		return nil
	}

	processRow := func(rec []string) error {
		sg := normUF(cell(rec, idxSGUF))
		cod := strings.ToUpper(cell(rec, idxCDMunTSE))
		nome := nomeMunUpper.String(cell(rec, idxNMunTSE))
		if cod == "" || nome == "" {
			skippedEmp++
			return nil
		}
		eid, ok := ufToID[sg]
		if !ok {
			skippedUF++
			if firstBadUF == "" {
				firstBadUF = sg
			}
			return nil
		}
		batchEstado = append(batchEstado, eid)
		batchCod = append(batchCod, cod)
		batchNome = append(batchNome, nome)
		if len(batchEstado) >= maxBatch {
			return flush()
		}
		return nil
	}

	if !firstIsHeader {
		if err := processRow(header); err != nil {
			log.Fatalf("processar primeira linha: %v", err)
		}
	}

	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Printf("aviso: linha CSV ignorada: %v", err)
			continue
		}
		if err := processRow(rec); err != nil {
			log.Fatalf("importar linha: %v", err)
		}
	}

	if err := flush(); err != nil {
		log.Fatalf("flush final: %v", err)
	}

	var total int64
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.municipios`).Scan(&total)
	fmt.Printf("Linhas novas aplicadas (RowsAffected do INSERT): %d\n", inserted)
	fmt.Printf("Total na tabela municipios: %d\n", total)
	if skippedUF > 0 {
		fmt.Printf("Linhas ignoradas (UF sem estado_id): %d", skippedUF)
		if firstBadUF != "" {
			fmt.Printf(" (ex.: %q)", firstBadUF)
		}
		fmt.Println()
	}
	if skippedEmp > 0 {
		fmt.Printf("Linhas ignoradas (código ou nome vazio): %d\n", skippedEmp)
	}
}

func buildInsertMunicipiosSQL(estado []int64, cod, nome []string) (string, []interface{}) {
	n := len(estado)
	var sb strings.Builder
	sb.WriteString(`INSERT INTO public.municipios (estado_id, codigo_tse, nome) VALUES `)
	args := make([]interface{}, 0, n*3)
	p := 1
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d)", p, p+1, p+2))
		args = append(args, estado[i], cod[i], nome[i])
		p += 3
	}
	sb.WriteString(` ON CONFLICT (estado_id, codigo_tse) DO NOTHING`)
	return sb.String(), args
}

func cell(rec []string, idx int) string {
	if idx < 0 || idx >= len(rec) {
		return ""
	}
	return strings.Trim(strings.TrimSpace(rec[idx]), `"`)
}

func normUF(s string) string {
	return strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(s, "\r", "")))
}

func readCSVText(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		b = b[3:]
	}
	if utf8.Valid(b) {
		return string(b), nil
	}
	r := make([]rune, len(b))
	for i, x := range b {
		r[i] = rune(x)
	}
	return string(r), nil
}

func resolveMunicipioCSVPath() (string, error) {
	candidates := []string{
		filepath.Join("dados", "municipio_tse_ibge", "municipio_tse_ibge.csv"),
		filepath.Join("..", "dados", "municipio_tse_ibge", "municipio_tse_ibge.csv"),
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = append([]string{
			filepath.Join(wd, "dados", "municipio_tse_ibge", "municipio_tse_ibge.csv"),
			filepath.Join(wd, "..", "dados", "municipio_tse_ibge", "municipio_tse_ibge.csv"),
		}, candidates...)
	}
	seen := map[string]bool{}
	for _, p := range candidates {
		ap, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		if seen[ap] {
			continue
		}
		seen[ap] = true
		if st, err := os.Stat(ap); err == nil && !st.IsDir() {
			return ap, nil
		}
	}
	return "", fmt.Errorf("municipio_tse_ibge.csv não encontrado (execute na raiz do repositório ou em go_postgres/)")
}

func loadUFToEstadoID(db *sql.DB) (map[string]int64, error) {
	rows, err := db.Query(`SELECT id, sigla FROM public.estados`)
	if err != nil {
		return nil, fmt.Errorf("ler public.estados: %w", err)
	}
	defer rows.Close()
	m := make(map[string]int64)
	for rows.Next() {
		var id int64
		var sg string
		if err := rows.Scan(&id, &sg); err != nil {
			return nil, err
		}
		k := normUF(sg)
		if k != "" {
			m[k] = id
		}
	}
	return m, rows.Err()
}

func pgEnvNonEmpty6(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func postgresDSN6(host, port, user, password, database string) string {
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
