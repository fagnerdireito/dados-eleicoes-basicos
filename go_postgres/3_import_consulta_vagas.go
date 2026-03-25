package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"golang.org/x/text/encoding/charmap"
)

// Config holds DB configuration
type Config struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

var vagasColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"ANO_ELEICAO", "4"},
	{"CD_TIPO_ELEICAO", "1"},
	{"NM_TIPO_ELEICAO", "20"},
	{"CD_ELEICAO", "4"},
	{"DS_ELEICAO", "40"},
	{"DT_ELEICAO", "10"},
	{"DT_POSSE", "10"},
	{"SG_UF", "2"},
	{"SG_UE", "5"},
	{"NM_UE", "40"},
	{"CD_CARGO", "2"},
	{"DS_CARGO", "20"},
	{"QT_VAGA", "5"},
}

var vagasKeyColumns = []string{"ANO_ELEICAO", "CD_ELEICAO", "SG_UE", "CD_CARGO"}

const (
	vagasTableName      = "consulta_vagas"
	vagasMaxWorkers     = 4
	vagasCsvSeparator   = ';'
	vagasMaxPlaceholders = 65_535
)

var vagasPrintMu sync.Mutex

func vagasSafePrintf(format string, args ...interface{}) {
	vagasPrintMu.Lock()
	fmt.Printf(format, args...)
	vagasPrintMu.Unlock()
}

type vagasFileResult struct {
	path string
	err  error
}

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	config := Config{
		Host:     vagasPgEnvNonEmpty("PGSQL_VECTOR_HOST", "127.0.0.1"),
		Port:     vagasPgEnvNonEmpty("PGSQL_VECTOR_PORT", "5432"),
		Database: vagasPgEnvNonEmpty("PGSQL_VECTOR_DATABASE", "eleicoes"),
		User:     vagasPgEnvNonEmpty("PGSQL_VECTOR_USERNAME", "postgres"),
		Password: os.Getenv("PGSQL_VECTOR_PASSWORD"),
	}

	dsn := vagasPostgresDSN(config)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(vagasMaxWorkers + 2)
	db.SetMaxIdleConns(vagasMaxWorkers + 2)

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}

	if err := vagasSetupTable(db); err != nil {
		log.Fatalf("Erro ao configurar tabela: %v", err)
	}

	files, err := vagasFindCSVFiles(filepath.Join(vagasRepoDataRoot(), "dados"))
	if err != nil {
		log.Fatalf("Erro ao buscar arquivos CSV: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("Nenhum arquivo CSV encontrado em 'dados/consulta_vagas_*/'.")
		return
	}

	sort.Strings(files)
	fmt.Printf("Encontrados %d arquivo(s) CSV. Usando %d goroutine(s).\n", len(files), vagasMaxWorkers)

	jobs := make(chan string, len(files))
	for _, f := range files {
		jobs <- f
	}
	close(jobs)

	results := make(chan vagasFileResult, len(files))

	var wg sync.WaitGroup
	for i := 0; i < vagasMaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				err := vagasProcessFile(db, path)
				results <- vagasFileResult{path: path, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var erros []vagasFileResult
	for r := range results {
		if r.err != nil {
			erros = append(erros, r)
			vagasSafePrintf("ERRO em %s: %v\n", r.path, r.err)
		}
	}

	if len(erros) > 0 {
		fmt.Printf("\nProcessamento concluído com %d erro(s).\n", len(erros))
	} else {
		fmt.Println("\nProcessamento concluído com sucesso.")
	}
}

func vagasRepoDataRoot() string {
	for _, root := range []string{".", ".."} {
		bweb := filepath.Join(root, "bweb")
		dados := filepath.Join(root, "dados")
		if st, err := os.Stat(bweb); err == nil && st.IsDir() {
			return root
		}
		if st, err := os.Stat(dados); err == nil && st.IsDir() {
			return root
		}
	}
	return "."
}

func vagasFindCSVFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := filepath.Base(path)
			if path != root && !strings.HasPrefix(base, "consulta_vagas_") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".csv") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func vagasPgEnvNonEmpty(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func vagasPostgresDSN(c Config) string {
	u := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(c.Host, c.Port),
		Path:   "/" + c.Database,
	}
	if c.Password != "" {
		u.User = url.UserPassword(c.User, c.Password)
	} else {
		u.User = url.User(c.User)
	}
	q := url.Values{}
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

func vagasSetupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, `id BIGSERIAL PRIMARY KEY`)
	for _, col := range vagasColumnLengths {
		cols = append(cols, fmt.Sprintf(`"%s" VARCHAR(%s) DEFAULT NULL`, col[0], col[1]))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (
  %s
)`,
		vagasTableName, strings.Join(cols, ",\n  "),
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela \"%s\" verificada/criada.\n", vagasTableName)

	idxQuery := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_consulta_vagas ON "%s" (%s)`,
		vagasTableName, strings.Join(vagasQuoteColumns(vagasKeyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		return fmt.Errorf("criar índice: %w", err)
	}
	fmt.Println("Índice único (idx_unique_consulta_vagas) garantido.")
	return nil
}

func vagasQuoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	return quoted
}

func vagasProcessFile(db *sql.DB, path string) error {
	vagasSafePrintf("Iniciando: %s\n", path)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("abrir arquivo: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(charmap.ISO8859_1.NewDecoder().Reader(f))
	reader.Comma = vagasCsvSeparator
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	rawHeader, err := reader.Read()
	if err != nil {
		return fmt.Errorf("ler cabeçalho: %w", err)
	}

	colIndexes := make(map[string]int, len(rawHeader))
	for i, col := range rawHeader {
		name := strings.ToUpper(strings.Trim(col, `"`))
		colIndexes[name] = i
	}

	targetCols := make([]string, len(vagasColumnLengths))
	for i, col := range vagasColumnLengths {
		targetCols[i] = col[0]
	}

	batchSize := vagasMaxPlaceholders / len(targetCols)
	if batchSize < 1 {
		batchSize = 1
	}

	conflictCols := strings.Join(vagasQuoteColumns(vagasKeyColumns), ", ")
	insertHead := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `,
		vagasTableName, strings.Join(vagasQuoteColumns(targetCols), ","))
	onConflict := fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`, conflictCols)

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := vagasExecuteBatch(db, insertHead, onConflict, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			vagasSafePrintf("Aviso CSV em %s: %v\n", path, err)
			continue
		}

		row := make([]interface{}, len(targetCols))
		for i, colName := range targetCols {
			if idx, ok := colIndexes[colName]; ok && idx < len(record) {
				v := record[idx]
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
				return fmt.Errorf("inserir lote: %w", err)
			}
		}
	}

	if err := flush(); err != nil {
		return fmt.Errorf("inserir lote final: %w", err)
	}

	vagasSafePrintf("Finalizado: %s\n", path)
	return nil
}

func vagasExecuteBatch(db *sql.DB, insertHead, onConflict string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
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
	_, err := db.Exec(query, args...)
	return err
}
