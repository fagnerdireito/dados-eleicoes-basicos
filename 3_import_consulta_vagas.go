package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
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
	vagasMaxWorkers     = 2
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
	if err := godotenv.Load(); err != nil {
		log.Println("Arquivo .env não encontrado, usando variáveis de ambiente do sistema.")
	}

	config := Config{
		Host:     vagasGetEnv("DB_HOST", "127.0.0.1"),
		Port:     vagasGetEnv("DB_PORT", "3306"),
		Database: vagasGetEnv("DB_DATABASE", "eleicoes"),
		User:     vagasGetEnv("DB_USER", vagasGetEnv("DB_USERNAME", "root")),
		Password: vagasGetEnv("DB_PASSWORD", ""),
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
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

	files, err := vagasFindCSVFiles("dados")
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

func vagasGetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func vagasSetupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, "`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT")
	for _, col := range vagasColumnLengths {
		cols = append(cols, fmt.Sprintf("`%s` VARCHAR(%s) DEFAULT NULL", col[0], col[1]))
	}
	cols = append(cols, "PRIMARY KEY (`id`)")
	cols = append(cols, fmt.Sprintf("UNIQUE KEY `idx_unique_consulta_vagas` (%s)",
		strings.Join(vagasQuoteColumns(vagasKeyColumns), ",")))

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		vagasTableName, strings.Join(cols, ",\n  "),
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela `%s` verificada/criada.\n", vagasTableName)

	idxQuery := fmt.Sprintf(
		"ALTER TABLE `%s` ADD UNIQUE INDEX idx_unique_consulta_vagas (%s)",
		vagasTableName, strings.Join(vagasQuoteColumns(vagasKeyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		s := err.Error()
		if strings.Contains(s, "1061") || strings.Contains(s, "Duplicate key") {
			fmt.Println("Índice único (idx_unique_consulta_vagas) já existe.")
		} else {
			return fmt.Errorf("criar índice: %w", err)
		}
	} else {
		fmt.Println("Índice único (idx_unique_consulta_vagas) criado com sucesso.")
	}
	return nil
}

func vagasQuoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf("`%s`", c)
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

	baseQuery := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES ",
		vagasTableName, strings.Join(vagasQuoteColumns(targetCols), ","))
	placeholder := "(" + strings.Repeat("?,", len(targetCols)-1) + "?)"

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := vagasExecuteBatch(db, baseQuery, placeholder, batch); err != nil {
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

func vagasExecuteBatch(db *sql.DB, baseQuery, placeholder string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	placeholders := make([]string, len(batch))
	for i := range batch {
		placeholders[i] = placeholder
	}
	query := baseQuery + strings.Join(placeholders, ",")

	args := make([]interface{}, 0, len(batch)*len(batch[0]))
	for _, row := range batch {
		args = append(args, row...)
	}

	_, err := db.Exec(query, args...)
	return err
}
