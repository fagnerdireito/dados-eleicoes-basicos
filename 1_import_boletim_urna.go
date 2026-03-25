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

// Config holds DB configuration.
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
	{"NM_TIPO_ELEICAO", "20"},
	{"CD_PLEITO", "3"},
	{"DT_PLEITO", "19"},
	{"NR_TURNO", "1"},
	{"CD_ELEICAO", "3"},
	{"DS_ELEICAO", "100"},
	{"SG_UF", "2"},
	{"CD_MUNICIPIO", "5"},
	{"NM_MUNICIPIO", "22"},
	{"NR_ZONA", "3"},
	{"NR_SECAO", "3"},
	{"NR_LOCAL_VOTACAO", "4"},
	{"CD_CARGO_PERGUNTA", "2"},
	{"DS_CARGO_PERGUNTA", "100"},
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
	// MySQL suporta no máximo 65.535 placeholders por statement.
	// O tamanho do lote é calculado em runtime com base no número de colunas.
	mysqlMaxPlaceholders = 65_535
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
	if err := godotenv.Load(); err != nil {
		log.Println("Arquivo .env não encontrado, usando variáveis de ambiente do sistema.")
	}

	config := Config{
		Host:     getEnv("DB_HOST", "127.0.0.1"),
		Port:     getEnv("DB_PORT", "3306"),
		Database: getEnv("DB_DATABASE", "eleicoes"),
		User:     getEnv("DB_USER", getEnv("DB_USERNAME", "root")),
		Password: getEnv("DB_PASSWORD", ""),
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
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
	files, err := findCSVFiles("bweb")
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

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func setupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, "`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT")
	for _, col := range buColumnLengths {
		cols = append(cols, fmt.Sprintf("`%s` VARCHAR(%s) DEFAULT NULL", col[0], col[1]))
	}
	cols = append(cols, "PRIMARY KEY (`id`)")
	cols = append(cols, fmt.Sprintf("UNIQUE KEY `idx_unique_bu` (%s)",
		strings.Join(quoteColumns(keyColumns), ",")))

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		tableName, strings.Join(cols, ",\n  "),
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela `%s` verificada/criada.\n", tableName)

	// Tenta adicionar o índice; ignora se já existir (erro 1061)
	idxQuery := fmt.Sprintf(
		"ALTER TABLE `%s` ADD UNIQUE INDEX idx_unique_bu (%s)",
		tableName, strings.Join(quoteColumns(keyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		s := err.Error()
		if strings.Contains(s, "1061") || strings.Contains(s, "Duplicate key") {
			fmt.Println("Índice único (idx_unique_bu) já existe.")
		} else {
			return fmt.Errorf("criar índice: %w", err)
		}
	} else {
		fmt.Println("Índice único (idx_unique_bu) criado com sucesso.")
	}
	return nil
}

func quoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf("`%s`", c)
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

	// Calcula o máximo de linhas por lote respeitando o limite de 65.535 placeholders do MySQL
	batchSize := mysqlMaxPlaceholders / len(targetCols)
	if batchSize < 1 {
		batchSize = 1
	}

	baseQuery := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES ",
		tableName, strings.Join(quoteColumns(targetCols), ","))
	placeholder := "(" + strings.Repeat("?,", len(targetCols)-1) + "?)"

	var batch [][]interface{}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := executeBatch(db, baseQuery, placeholder, batch); err != nil {
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

func executeBatch(db *sql.DB, baseQuery, placeholder string, batch [][]interface{}) error {
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
