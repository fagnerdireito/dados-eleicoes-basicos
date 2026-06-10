package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

const (
	lvTableName         = "local_votacao"
	lvCsvRelativePath   = "dados/2024_localvotaca_local_votacao_2024.csv"
	lvCsvSeparator      = ','
	lvMaxPlaceholders   = 65_535
	lvBatchRowsFallback = 500
)

var lvColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"AA_ELEICAO", "4"},
	{"DT_ELEICAO", "10"},
	{"DS_ELEICAO", "20"},
	{"NR_TURNO", "1"},
	{"SG_UF", "2"},
	{"CD_MUNICIPIO", "10"},
	{"NM_MUNICIPIO", "100"},
	{"NR_ZONA", "10"},
	{"NR_SECAO", "10"},
	{"CD_TIPO_SECAO_AGREGADA", "5"},
	{"DS_TIPO_SECAO_AGREGADA", "30"},
	{"NR_SECAO_PRINCIPAL", "10"},
	{"NR_LOCAL_VOTACAO", "10"},
	{"NM_LOCAL_VOTACAO", "100"},
	{"CD_TIPO_LOCAL", "5"},
	{"DS_TIPO_LOCAL", "30"},
	{"DS_ENDERECO", "100"},
	{"NM_BAIRRO", "80"},
	{"NR_CEP", "15"},
	{"NR_TELEFONE_LOCAL", "20"},
	{"NR_LATITUDE", "20"},
	{"NR_LONGITUDE", "20"},
	{"CD_SITU_LOCAL_VOTACAO", "5"},
	{"DS_SITU_LOCAL_VOTACAO", "30"},
	{"CD_SITU_ZONA", "5"},
	{"DS_SITU_ZONA", "30"},
	{"CD_SITU_SECAO", "5"},
	{"DS_SITU_SECAO", "30"},
	{"CD_SITU_LOCALIDADE", "5"},
	{"DS_SITU_LOCALIDADE", "30"},
	{"CD_SITU_SECAO_ACESSIBILIDADE", "5"},
	{"DS_SITU_SECAO_ACESSIBILIDADE", "50"},
	{"QT_ELEITOR_SECAO", "10"},
	{"QT_ELEITOR_ELEICAO_FEDERAL", "10"},
	{"QT_ELEITOR_ELEICAO_ESTADUAL", "10"},
	{"QT_ELEITOR_ELEICAO_MUNICIPAL", "10"},
	{"NR_LOCAL_VOTACAO_ORIGINAL", "10"},
	{"NM_LOCAL_VOTACAO_ORIGINAL", "100"},
	{"DS_ENDERECO_LOCVT_ORIGINAL", "100"},
	{"indice", "20"},
}

// Chave natural: uma linha por seção eleitoral na eleição/turno.
var lvKeyColumns = []string{
	"AA_ELEICAO",
	"NR_TURNO",
	"SG_UF",
	"CD_MUNICIPIO",
	"NR_ZONA",
	"NR_SECAO",
}

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := lvPgEnvNonEmpty("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := lvPgEnvNonEmpty("PGSQL_VECTOR_PORT", "5432")
	dbname := lvPgEnvNonEmpty("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := lvPgEnvNonEmpty("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := lvPostgresDSN(host, port, user, password, dbname)
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

	if err := lvSetupTable(db); err != nil {
		log.Fatalf("erro ao configurar tabela: %v", err)
	}

	csvPath, err := lvResolveCSVPath()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("CSV: %s\n", csvPath)

	inserted, skipped, err := lvImportFile(db, csvPath)
	if err != nil {
		log.Fatalf("importação: %v", err)
	}

	var total int64
	_ = db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, lvTableName)).Scan(&total)

	fmt.Printf("Linhas inseridas nesta execução: %d\n", inserted)
	fmt.Printf("Linhas ignoradas (já existiam): %d\n", skipped)
	fmt.Printf("Total na tabela \"%s\": %d\n", lvTableName, total)
}

func lvRepoRoot() string {
	for _, root := range []string{".", ".."} {
		if st, err := os.Stat(filepath.Join(root, "dados")); err == nil && st.IsDir() {
			return root
		}
		if st, err := os.Stat(filepath.Join(root, "bweb")); err == nil && st.IsDir() {
			return root
		}
	}
	return "."
}

func lvResolveCSVPath() (string, error) {
	path := filepath.Join(lvRepoRoot(), lvCsvRelativePath)
	if st, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("arquivo não encontrado: %s", path)
		}
		return "", fmt.Errorf("acessar %s: %w", path, err)
	} else if st.IsDir() {
		return "", fmt.Errorf("%s é um diretório, esperado CSV", path)
	}
	return path, nil
}

func lvSetupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, `id BIGSERIAL PRIMARY KEY`)
	for _, col := range lvColumnLengths {
		cols = append(cols, fmt.Sprintf(`"%s" VARCHAR(%s) DEFAULT NULL`, col[0], col[1]))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (
  %s
)`,
		lvTableName, strings.Join(cols, ",\n  "),
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela \"%s\" verificada/criada.\n", lvTableName)

	idxQuery := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_local_votacao ON "%s" (%s)`,
		lvTableName, strings.Join(lvQuoteColumns(lvKeyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		return fmt.Errorf("criar índice único: %w", err)
	}
	fmt.Println("Índice único (idx_unique_local_votacao) garantido.")
	return nil
}

func lvImportFile(db *sql.DB, path string) (inserted, skipped int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("abrir arquivo: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = lvCsvSeparator
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	rawHeader, err := reader.Read()
	if err != nil {
		return 0, 0, fmt.Errorf("ler cabeçalho: %w", err)
	}

	colIndexes := make(map[string]int, len(rawHeader))
	for i, col := range rawHeader {
		name := strings.ToUpper(strings.Trim(col, `"`))
		colIndexes[name] = i
	}

	targetCols := make([]string, len(lvColumnLengths))
	for i, col := range lvColumnLengths {
		targetCols[i] = col[0]
	}

	batchSize := lvMaxPlaceholders / len(targetCols)
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > lvBatchRowsFallback {
		batchSize = lvBatchRowsFallback
	}

	conflictCols := strings.Join(lvQuoteColumns(lvKeyColumns), ", ")
	insertHead := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `,
		lvTableName, strings.Join(lvQuoteColumns(targetCols), ","))
	onConflict := fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`, conflictCols)

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, s, err := lvExecuteBatch(db, insertHead, onConflict, batch)
		if err != nil {
			return err
		}
		inserted += n
		skipped += s
		batch = batch[:0]
		return nil
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Aviso CSV em %s: %v\n", path, err)
			continue
		}

		row := make([]interface{}, len(targetCols))
		for i, colName := range targetCols {
			if idx, ok := colIndexes[colName]; ok && idx < len(record) {
				v := strings.TrimSpace(record[idx])
				if v == "" {
					row[i] = nil
				} else {
					row[i] = v
				}
			}
		}
		batch = append(batch, row)

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return inserted, skipped, fmt.Errorf("inserir lote: %w", err)
			}
		}
	}

	if err := flush(); err != nil {
		return inserted, skipped, fmt.Errorf("inserir lote final: %w", err)
	}

	return inserted, skipped, nil
}

func lvExecuteBatch(db *sql.DB, insertHead, onConflict string, batch [][]interface{}) (inserted, skipped int64, err error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	nCols := len(batch[0])
	var tuples []string
	argPos := 1
	args := make([]interface{}, 0, len(batch)*nCols)
	for _, row := range batch {
		cells := make([]string, len(row))
		for i := range row {
			cells[i] = fmt.Sprintf("$%d", argPos)
			argPos++
		}
		tuples = append(tuples, "("+strings.Join(cells, ",")+")")
		args = append(args, row...)
	}
	query := insertHead + strings.Join(tuples, ",") + onConflict
	res, err := db.Exec(query, args...)
	if err != nil {
		return 0, 0, err
	}
	aff, _ := res.RowsAffected()
	inserted = aff
	skipped = int64(len(batch)) - aff
	return inserted, skipped, nil
}

func lvQuoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	return quoted
}

func lvPgEnvNonEmpty(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func lvPostgresDSN(host, port, user, password, database string) string {
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
