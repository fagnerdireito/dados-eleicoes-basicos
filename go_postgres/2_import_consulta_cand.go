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

var candColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"ANO_ELEICAO", "4"},
	{"CD_TIPO_ELEICAO", "1"},
	{"NM_TIPO_ELEICAO", "100"},
	{"NR_TURNO", "1"},
	{"CD_ELEICAO", "4"},
	{"DS_ELEICAO", "100"},
	{"DT_ELEICAO", "10"},
	{"TP_ABRANGENCIA", "10"},
	{"SG_UF", "2"},
	{"SG_UE", "5"},
	{"NM_UE", "255"},
	{"CD_CARGO", "2"},
	{"DS_CARGO", "100"},
	{"SQ_CANDIDATO", "15"},
	{"NR_CANDIDATO", "5"},
	{"NM_CANDIDATO", "70"},
	{"NM_URNA_CANDIDATO", "255"},
	{"NM_SOCIAL_CANDIDATO", "255"},
	{"NR_CPF_CANDIDATO", "15"},
	{"DS_EMAIL", "100"},
	{"CD_SITUACAO_CANDIDATURA", "2"},
	{"DS_SITUACAO_CANDIDATURA", "100"},
	{"TP_AGREMIACAO", "100"},
	{"NR_PARTIDO", "5"},
	{"SG_PARTIDO", "15"},
	{"NM_PARTIDO", "50"},
	{"NR_FEDERACAO", "5"},
	{"NM_FEDERACAO", "50"},
	{"SG_FEDERACAO", "100"},
	{"DS_COMPOSICAO_FEDERACAO", "100"},
	{"SQ_COLIGACAO", "15"},
	{"NM_COLIGACAO", "100"},
	{"DS_COMPOSICAO_COLIGACAO", "255"},
	{"SG_UF_NASCIMENTO", "15"},
	{"DT_NASCIMENTO", "10"},
	{"NR_TITULO_ELEITORAL_CANDIDATO", "15"},
	{"CD_GENERO", "2"},
	{"DS_GENERO", "15"},
	{"CD_GRAU_INSTRUCAO", "2"},
	{"DS_GRAU_INSTRUCAO", "30"},
	{"CD_ESTADO_CIVIL", "2"},
	{"DS_ESTADO_CIVIL", "30"},
	{"CD_COR_RACA", "2"},
	{"DS_COR_RACA", "15"},
	{"CD_OCUPACAO", "5"},
	{"DS_OCUPACAO", "80"},
	{"CD_SIT_TOT_TURNO", "2"},
	{"DS_SIT_TOT_TURNO", "100"},
}

var candKeyColumns = []string{"ANO_ELEICAO", "SQ_CANDIDATO"}

const (
	candTableName    = "consulta_cand"
	candMaxWorkers   = 4
	candCsvSeparator = ';'
	candMaxPlaceholders = 65_535
)

var candPrintMu sync.Mutex

func candSafePrintf(format string, args ...interface{}) {
	candPrintMu.Lock()
	fmt.Printf(format, args...)
	candPrintMu.Unlock()
}

type candFileResult struct {
	path string
	err  error
}

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	config := Config{
		Host:     candPgEnvNonEmpty("PGSQL_VECTOR_HOST", "127.0.0.1"),
		Port:     candPgEnvNonEmpty("PGSQL_VECTOR_PORT", "5432"),
		Database: candPgEnvNonEmpty("PGSQL_VECTOR_DATABASE", "eleicoes"),
		User:     candPgEnvNonEmpty("PGSQL_VECTOR_USERNAME", "postgres"),
		Password: os.Getenv("PGSQL_VECTOR_PASSWORD"),
	}

	dsn := candPostgresDSN(config)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(candMaxWorkers + 2)
	db.SetMaxIdleConns(candMaxWorkers + 2)

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}

	if err := candSetupTable(db); err != nil {
		log.Fatalf("Erro ao configurar tabela: %v", err)
	}

	files, err := candFindCSVFiles(filepath.Join(candRepoDataRoot(), "dados"))
	if err != nil {
		log.Fatalf("Erro ao buscar arquivos CSV: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("Nenhum arquivo CSV encontrado em 'dados/consulta_cand_*/'.")
		return
	}

	sort.Strings(files)
	fmt.Printf("Encontrados %d arquivo(s) CSV. Usando %d goroutine(s).\n", len(files), candMaxWorkers)

	jobs := make(chan string, len(files))
	for _, f := range files {
		jobs <- f
	}
	close(jobs)

	results := make(chan candFileResult, len(files))

	var wg sync.WaitGroup
	for i := 0; i < candMaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				err := candProcessFile(db, path)
				results <- candFileResult{path: path, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var erros []candFileResult
	for r := range results {
		if r.err != nil {
			erros = append(erros, r)
			candSafePrintf("ERRO em %s: %v\n", r.path, r.err)
		}
	}

	if len(erros) > 0 {
		fmt.Printf("\nProcessamento concluído com %d erro(s).\n", len(erros))
	} else {
		fmt.Println("\nProcessamento concluído com sucesso.")
	}
}

func candRepoDataRoot() string {
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

func candFindCSVFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Filtra apenas diretórios consulta_cand_* e arquivos .csv
		if d.IsDir() {
			base := filepath.Base(path)
			if path != root && !strings.HasPrefix(base, "consulta_cand_") {
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

func candPgEnvNonEmpty(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func candPostgresDSN(c Config) string {
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

func candSetupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, `id BIGSERIAL PRIMARY KEY`)
	for _, col := range candColumnLengths {
		cols = append(cols, fmt.Sprintf(`"%s" VARCHAR(%s) DEFAULT NULL`, col[0], col[1]))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (
  %s
)`,
		candTableName, strings.Join(cols, ",\n  "),
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela \"%s\" verificada/criada.\n", candTableName)

	idxQuery := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_consulta_cand ON "%s" (%s)`,
		candTableName, strings.Join(candQuoteColumns(candKeyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		return fmt.Errorf("criar índice: %w", err)
	}
	fmt.Println("Índice único (idx_unique_consulta_cand) garantido.")
	return nil
}

func candQuoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	return quoted
}

func candProcessFile(db *sql.DB, path string) error {
	candSafePrintf("Iniciando: %s\n", path)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("abrir arquivo: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(charmap.ISO8859_1.NewDecoder().Reader(f))
	reader.Comma = candCsvSeparator
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

	targetCols := make([]string, len(candColumnLengths))
	for i, col := range candColumnLengths {
		targetCols[i] = col[0]
	}

	batchSize := candMaxPlaceholders / len(targetCols)
	if batchSize < 1 {
		batchSize = 1
	}

	conflictCols := strings.Join(candQuoteColumns(candKeyColumns), ", ")
	insertHead := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `,
		candTableName, strings.Join(candQuoteColumns(targetCols), ","))
	onConflict := fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`, conflictCols)

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := candExecuteBatch(db, insertHead, onConflict, batch); err != nil {
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
			candSafePrintf("Aviso CSV em %s: %v\n", path, err)
			continue
		}

		row := make([]interface{}, len(targetCols))
		for i, colName := range targetCols {
			if idx, ok := colIndexes[colName]; ok && idx < len(record) {
				v := strings.TrimSpace(record[idx])
				if v == "" {
					row[i] = nil
				} else if colName == "DS_CARGO" {
					row[i] = strings.ToUpper(v)
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

	candSafePrintf("Finalizado: %s\n", path)
	return nil
}

func candExecuteBatch(db *sql.DB, insertHead, onConflict string, batch [][]interface{}) error {
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
