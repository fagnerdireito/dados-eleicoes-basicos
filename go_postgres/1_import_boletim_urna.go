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

var columnMapping = map[string]string{
	"C_ELEICAO":                 "CD_ELEICAO",
	"DS_AGREGADAS":              "DS_SECOES_AGREGADAS",
	"QT_ELEITORES_BIOMETRIA_NH": "QT_ELEI_BIOM_SEM_HABILITACAO",
}

var buColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"ANO_ELEICAO", "4"},
	{"CD_TIPO_ELEICAO", "1"},
	{"NM_TIPO_ELEICAO", "17"},
	{"CD_PLEITO", "3"},
	{"DT_PLEITO", "19"},
	{"NR_TURNO", "1"},
	{"CD_ELEICAO", "3"},
	{"DS_ELEICAO", "30"},
	{"SG_UF", "2"},
	{"CD_MUNICIPIO", "5"},
	{"NM_MUNICIPIO", "22"},
	{"NR_ZONA", "3"},
	{"NR_SECAO", "3"},
	{"NR_LOCAL_VOTACAO", "4"},
	{"CD_CARGO_PERGUNTA", "2"},
	{"DS_CARGO_PERGUNTA", "17"},
	{"NR_PARTIDO", "2"},
	{"SG_PARTIDO", "13"},
	{"NM_PARTIDO", "46"},
	{"DT_BU_RECEBIDO", "19"},
	{"QT_APTOS", "3"},
	{"QT_COMPARECIMENTO", "3"},
	{"QT_ABSTENCOES", "3"},
	{"CD_TIPO_URNA", "1"},
	{"DS_TIPO_URNA", "7"},
	{"CD_TIPO_VOTAVEL", "1"},
	{"DS_TIPO_VOTAVEL", "7"},
	{"NR_VOTAVEL", "5"},
	{"NM_VOTAVEL", "28"},
	{"QT_VOTOS", "3"},
	{"NR_URNA_EFETIVADA", "7"},
	{"CD_CARGA_1_URNA_EFETIVADA", "24"},
	{"CD_CARGA_2_URNA_EFETIVADA", "7"},
	{"CD_FLASHCARD_URNA_EFETIVADA", "8"},
	{"DT_CARGA_URNA_EFETIVADA", "19"},
	{"DS_CARGO_PERGUNTA_SECAO", "8"},
	{"DS_SECOES_AGREGADAS", "15"},
	{"DT_ABERTURA", "19"},
	{"DT_ENCERRAMENTO", "19"},
	{"QT_ELEI_BIOM_SEM_HABILITACAO", "2"},
	{"DT_EMISSAO_BU", "19"},
	{"NR_JUNTA_APURADORA", "2"},
	{"NR_TURMA_APURADORA", "2"},
}

var keyColumns = []string{"CD_PLEITO", "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO", "CD_CARGO_PERGUNTA", "NR_VOTAVEL"}

const (
	tableName    = "boletim_de_urna"
	maxWorkers   = 4
	csvSeparator = ';'
	// PostgreSQL: limite de parâmetros por statement (protocolo).
	pgMaxPlaceholders = 65_535
)

// printMu garante saída sem mistura entre goroutines
var printMu sync.Mutex

func safePrintf(format string, args ...interface{}) {
	printMu.Lock()
	fmt.Printf(format, args...)
	printMu.Unlock()
}

// fileResult carrega o resultado de cada goroutine
type fileResult struct {
	path string
	err  error
}

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	config := Config{
		Host:     pgEnvNonEmpty("PGSQL_VECTOR_HOST", "127.0.0.1"),
		Port:     pgEnvNonEmpty("PGSQL_VECTOR_PORT", "5432"),
		Database: pgEnvNonEmpty("PGSQL_VECTOR_DATABASE", "eleicoes"),
		User:     pgEnvNonEmpty("PGSQL_VECTOR_USERNAME", "postgres"),
		Password: os.Getenv("PGSQL_VECTOR_PASSWORD"),
	}

	dsn := postgresDSN(config)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	// Pool de conexões: workers + margem para setup
	db.SetMaxOpenConns(maxWorkers + 2)
	db.SetMaxIdleConns(maxWorkers + 2)

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}

	if err := setupTable(db); err != nil {
		log.Fatalf("Erro ao configurar tabela: %v", err)
	}

	// filepath.Glob não suporta ** recursivo em Go; usamos WalkDir
	files, err := findCSVFiles(filepath.Join(repoDataRoot(), "bweb"))
	if err != nil {
		log.Fatalf("Erro ao buscar arquivos CSV: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("Nenhum arquivo CSV encontrado em 'bweb/'.")
		return
	}

	sort.Strings(files)
	fmt.Printf("Encontrados %d arquivo(s) CSV. Usando %d goroutine(s).\n", len(files), maxWorkers)

	// Canal de jobs com buffer do total de arquivos (sem bloqueio ao enviar)
	jobs := make(chan string, len(files))
	for _, f := range files {
		jobs <- f
	}
	close(jobs) // fecha após enviar todos; workers drenam sem deadlock

	// Canal de resultados com buffer suficiente para todos os arquivos
	results := make(chan fileResult, len(files))

	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				err := processFile(db, path)
				results <- fileResult{path: path, err: err}
			}
		}()
	}

	// Fecha o canal de resultados quando todos os workers terminarem
	go func() {
		wg.Wait()
		close(results)
	}()

	// Coleta resultados sem bloquear (canal fechado pelo goroutine acima)
	var erros []fileResult
	for r := range results {
		if r.err != nil {
			erros = append(erros, r)
			safePrintf("ERRO em %s: %v\n", r.path, r.err)
		}
	}

	if len(erros) > 0 {
		fmt.Printf("\nProcessamento concluído com %d erro(s).\n", len(erros))
	} else {
		fmt.Println("\nProcessamento concluído com sucesso.")
	}
}

// repoDataRoot retorna "." na raiz do repositório ou ".." quando o CWD é go_postgres/.
func repoDataRoot() string {
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

// findCSVFiles percorre recursivamente o diretório e retorna arquivos .csv
func findCSVFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.EqualFold(filepath.Ext(path), ".csv") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func pgEnvNonEmpty(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func postgresDSN(c Config) string {
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

func setupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, `id BIGSERIAL PRIMARY KEY`)
	for _, col := range buColumnLengths {
		cols = append(cols, fmt.Sprintf(`"%s" VARCHAR(%s) DEFAULT NULL`, col[0], col[1]))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (
  %s
)`,
		tableName, strings.Join(cols, ",\n  "),
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela \"%s\" verificada/criada.\n", tableName)

	idxQuery := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_bu ON "%s" (%s)`,
		tableName, strings.Join(quoteColumns(keyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		return fmt.Errorf("criar índice: %w", err)
	}
	fmt.Println("Índice único (idx_unique_bu) garantido.")
	return nil
}

func quoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	return quoted
}

func processFile(db *sql.DB, path string) error {
	safePrintf("Iniciando: %s\n", path)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("abrir arquivo: %w", err)
	}
	defer f.Close()

	// Decodifica latin1 (ISO-8859-1) para UTF-8 em stream
	reader := csv.NewReader(charmap.ISO8859_1.NewDecoder().Reader(f))
	reader.Comma = csvSeparator
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	// Arquivos de anos diferentes podem ter quantidades distintas de colunas; não
	// exigir o mesmo número de campos em todas as linhas. Colunas são ligadas pelo
	// nome no cabeçalho; as que não existem no arquivo viram NULL na inserção.
	reader.FieldsPerRecord = -1

	// Lê o cabeçalho e normaliza os nomes das colunas
	rawHeader, err := reader.Read()
	if err != nil {
		return fmt.Errorf("ler cabeçalho: %w", err)
	}

	colIndexes := make(map[string]int, len(rawHeader))
	for i, col := range rawHeader {
		name := strings.ToUpper(strings.Trim(col, `"`))
		if mapped, ok := columnMapping[name]; ok {
			name = mapped
		}
		colIndexes[name] = i
	}

	// Lista de colunas alvo (na ordem da tabela)
	targetCols := make([]string, len(buColumnLengths))
	for i, col := range buColumnLengths {
		targetCols[i] = col[0]
	}

	batchSize := pgMaxPlaceholders / len(targetCols)
	if batchSize < 1 {
		batchSize = 1
	}

	conflictCols := strings.Join(quoteColumns(keyColumns), ", ")
	insertHead := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `,
		tableName, strings.Join(quoteColumns(targetCols), ","))
	onConflict := fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`, conflictCols)

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := executeBatch(db, insertHead, onConflict, batch); err != nil {
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
			// Linha malformada: loga e continua
			safePrintf("Aviso CSV em %s: %v\n", path, err)
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
			// se coluna ausente, row[i] permanece nil
		}
		batch = append(batch, row)

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return fmt.Errorf("inserir lote: %w", err)
			}
		}
	}

	// Flush do lote restante
	if err := flush(); err != nil {
		return fmt.Errorf("inserir lote final: %w", err)
	}

	safePrintf("Finalizado: %s\n", path)
	return nil
}

func executeBatch(db *sql.DB, insertHead, onConflict string, batch [][]interface{}) error {
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
