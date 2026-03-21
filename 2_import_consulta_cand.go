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

var candColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"ANO_ELEICAO", "4"},
	{"CD_TIPO_ELEICAO", "1"},
	{"NM_TIPO_ELEICAO", "20"},
	{"NR_TURNO", "1"},
	{"CD_ELEICAO", "4"},
	{"DS_ELEICAO", "40"},
	{"DT_ELEICAO", "10"},
	{"TP_ABRANGENCIA", "10"},
	{"SG_UF", "2"},
	{"SG_UE", "5"},
	{"NM_UE", "40"},
	{"CD_CARGO", "2"},
	{"DS_CARGO", "20"},
	{"SQ_CANDIDATO", "15"},
	{"NR_CANDIDATO", "5"},
	{"NM_CANDIDATO", "70"},
	{"NM_URNA_CANDIDATO", "40"},
	{"NM_SOCIAL_CANDIDATO", "40"},
	{"NR_CPF_CANDIDATO", "15"},
	{"DS_EMAIL", "100"},
	{"CD_SITUACAO_CANDIDATURA", "2"},
	{"DS_SITUACAO_CANDIDATURA", "20"},
	{"TP_AGREMIACAO", "20"},
	{"NR_PARTIDO", "5"},
	{"SG_PARTIDO", "15"},
	{"NM_PARTIDO", "50"},
	{"NR_FEDERACAO", "5"},
	{"NM_FEDERACAO", "50"},
	{"SG_FEDERACAO", "20"},
	{"DS_COMPOSICAO_FEDERACAO", "20"},
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
	{"DS_SIT_TOT_TURNO", "20"},
}

var candKeyColumns = []string{"ANO_ELEICAO", "SQ_CANDIDATO"}

const (
	candTableName    = "consulta_cand"
	candMaxWorkers   = 2
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
	if err := godotenv.Load(); err != nil {
		log.Println("Arquivo .env não encontrado, usando variáveis de ambiente do sistema.")
	}

	config := Config{
		Host:     candGetEnv("DB_HOST", "127.0.0.1"),
		Port:     candGetEnv("DB_PORT", "3306"),
		Database: candGetEnv("DB_DATABASE", "eleicoes"),
		User:     candGetEnv("DB_USER", candGetEnv("DB_USERNAME", "root")),
		Password: candGetEnv("DB_PASSWORD", ""),
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
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

	files, err := candFindCSVFiles("dados")
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

func candGetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func candSetupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, "`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT")
	for _, col := range candColumnLengths {
		cols = append(cols, fmt.Sprintf("`%s` VARCHAR(%s) DEFAULT NULL", col[0], col[1]))
	}
	cols = append(cols, "PRIMARY KEY (`id`)")
	cols = append(cols, fmt.Sprintf("UNIQUE KEY `idx_unique_consulta_cand` (%s)",
		strings.Join(candQuoteColumns(candKeyColumns), ",")))

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		candTableName, strings.Join(cols, ",\n  "),
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela `%s` verificada/criada.\n", candTableName)

	idxQuery := fmt.Sprintf(
		"ALTER TABLE `%s` ADD UNIQUE INDEX idx_unique_consulta_cand (%s)",
		candTableName, strings.Join(candQuoteColumns(candKeyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		s := err.Error()
		if strings.Contains(s, "1061") || strings.Contains(s, "Duplicate key") {
			fmt.Println("Índice único (idx_unique_consulta_cand) já existe.")
		} else {
			return fmt.Errorf("criar índice: %w", err)
		}
	} else {
		fmt.Println("Índice único (idx_unique_consulta_cand) criado com sucesso.")
	}
	return nil
}

func candQuoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf("`%s`", c)
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

	baseQuery := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES ",
		candTableName, strings.Join(candQuoteColumns(targetCols), ","))
	placeholder := "(" + strings.Repeat("?,", len(targetCols)-1) + "?)"

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := candExecuteBatch(db, baseQuery, placeholder, batch); err != nil {
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

	candSafePrintf("Finalizado: %s\n", path)
	return nil
}

func candExecuteBatch(db *sql.DB, baseQuery, placeholder string, batch [][]interface{}) error {
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
