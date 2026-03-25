package main

// Importa um único arquivo CSV de boletim de urna para o PostgreSQL.
//
// Uso:
//
//	go run 1_import_boletim_urna_single.go <caminho/para/arquivo.csv>
//
// Exemplos:
//
//	go run 1_import_boletim_urna_single.go ../bweb/bweb_1t_MA_091020241636.csv
//	go run 1_import_boletim_urna_single.go /dados/bweb/bweb_1t_MG_051020221321.csv

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"golang.org/x/text/encoding/charmap"
)

// ── configuração ─────────────────────────────────────────────────────────────

type singleConfig struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

var singleColumnMapping = map[string]string{
	"C_ELEICAO":                 "CD_ELEICAO",
	"DS_AGREGADAS":              "DS_SECOES_AGREGADAS",
	"QT_ELEITORES_BIOMETRIA_NH": "QT_ELEI_BIOM_SEM_HABILITACAO",
}

var singleBuColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"ANO_ELEICAO", "4"},
	{"CD_TIPO_ELEICAO", "1"},
	{"NM_TIPO_ELEICAO", "51"},
	{"CD_PLEITO", "20"},
	{"DT_PLEITO", "19"},
	{"NR_TURNO", "1"},
	{"CD_ELEICAO", "20"},
	{"DS_ELEICAO", "500"},
	{"SG_UF", "20"},
	{"CD_MUNICIPIO", "5"},
	{"NM_MUNICIPIO", "255"},
	{"NR_ZONA", "20"},
	{"NR_SECAO", "20"},
	{"NR_LOCAL_VOTACAO", "4"},
	{"CD_CARGO_PERGUNTA", "20"},
	{"DS_CARGO_PERGUNTA", "500"},
	{"NR_PARTIDO", "20"},
	{"SG_PARTIDO", "13"},
	{"NM_PARTIDO", "46"},
	{"DT_BU_RECEBIDO", "19"},
	{"QT_APTOS", "20"},
	{"QT_COMPARECIMENTO", "20"},
	{"QT_ABSTENCOES", "20"},
	{"CD_TIPO_URNA", "1"},
	{"DS_TIPO_URNA", "20"},
	{"CD_TIPO_VOTAVEL", "1"},
	{"DS_TIPO_VOTAVEL", "20"},
	{"NR_VOTAVEL", "5"},
	{"NM_VOTAVEL", "255"},
	{"QT_VOTOS", "20"},
	{"NR_URNA_EFETIVADA", "255"},
	{"CD_CARGA_1_URNA_EFETIVADA", "24"},
	{"CD_CARGA_2_URNA_EFETIVADA", "255"},
	{"CD_FLASHCARD_URNA_EFETIVADA", "255"},
	{"DT_CARGA_URNA_EFETIVADA", "19"},
	{"DS_CARGO_PERGUNTA_SECAO", "500"},
	{"DS_SECOES_AGREGADAS", "500"},
	{"DT_ABERTURA", "19"},
	{"DT_ENCERRAMENTO", "19"},
	{"QT_ELEI_BIOM_SEM_HABILITACAO", "255"},
	{"DT_EMISSAO_BU", "19"},
	{"NR_JUNTA_APURADORA", "255"},
	{"NR_TURMA_APURADORA", "255"},
}

var singleKeyColumns = []string{"CD_PLEITO", "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO", "CD_CARGO_PERGUNTA", "NR_VOTAVEL"}

const (
	singleTableName        = "boletim_de_urna"
	singleCsvSeparator     = ';'
	singlePgMaxPlaceholders = 65_535
)

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Uso: go run 1_import_boletim_urna_single.go <caminho/para/arquivo.csv>\n")
		fmt.Fprintf(os.Stderr, "\nExemplo:\n")
		fmt.Fprintf(os.Stderr, "  go run 1_import_boletim_urna_single.go ../bweb/bweb_1t_MA_091020241636.csv\n")
		os.Exit(1)
	}

	csvPath := os.Args[1]

	if _, err := os.Stat(csvPath); err != nil {
		log.Fatalf("Arquivo não encontrado: %s\n  %v", csvPath, err)
	}

	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	cfg := singleConfig{
		Host:     singleEnv("PGSQL_VECTOR_HOST", "127.0.0.1"),
		Port:     singleEnv("PGSQL_VECTOR_PORT", "5432"),
		Database: singleEnv("PGSQL_VECTOR_DATABASE", "eleicoes"),
		User:     singleEnv("PGSQL_VECTOR_USERNAME", "postgres"),
		Password: os.Getenv("PGSQL_VECTOR_PASSWORD"),
	}

	db, err := sql.Open("pgx", singleDSN(cfg))
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}
	fmt.Println("Conexão com o banco estabelecida.")

	if err := singleSetupTable(db); err != nil {
		log.Fatalf("Erro ao configurar tabela: %v", err)
	}

	fmt.Printf("Importando: %s\n", csvPath)
	if err := singleProcessFile(db, csvPath); err != nil {
		log.Fatalf("Erro ao processar arquivo: %v", err)
	}

	fmt.Println("Importação concluída com sucesso.")
}

// ── banco ─────────────────────────────────────────────────────────────────────

func singleEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func singleDSN(c singleConfig) string {
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

func singleSetupTable(db *sql.DB) error {
	var cols []string
	cols = append(cols, `id BIGSERIAL PRIMARY KEY`)
	for _, col := range singleBuColumnLengths {
		cols = append(cols, fmt.Sprintf(`"%s" VARCHAR(%s) DEFAULT NULL`, col[0], col[1]))
	}
	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS \"%s\" (\n  %s\n)",
		singleTableName, strings.Join(cols, ",\n  "),
	)
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela \"%s\" verificada/criada.\n", singleTableName)

	// Garante que colunas de instâncias anteriores estejam com o tamanho correto
	migrations := []struct{ col, size string }{
		{"DS_ELEICAO", "500"},
		{"NM_MUNICIPIO", "255"},
		{"DS_CARGO_PERGUNTA", "500"},
		{"CD_FLASHCARD_URNA_EFETIVADA", "255"},
		{"DS_CARGO_PERGUNTA_SECAO", "500"},
		{"DS_SECOES_AGREGADAS", "500"},
	}
	for _, m := range migrations {
		q := fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" TYPE VARCHAR(%s)`, singleTableName, m.col, m.size)
		if _, err := db.Exec(q); err != nil && !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("alterar coluna %s: %w", m.col, err)
		}
	}

	idxSQL := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_bu ON "%s" (%s)`,
		singleTableName, strings.Join(singleQuoteCols(singleKeyColumns), ", "),
	)
	if _, err := db.Exec(idxSQL); err != nil {
		return fmt.Errorf("criar índice: %w", err)
	}
	fmt.Println("Índice único (idx_unique_bu) garantido.")
	return nil
}

// ── processamento do CSV ──────────────────────────────────────────────────────

func singleProcessFile(db *sql.DB, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("abrir arquivo: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(charmap.ISO8859_1.NewDecoder().Reader(f))
	reader.Comma = singleCsvSeparator
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	rawHeader, err := reader.Read()
	if err != nil {
		return fmt.Errorf("ler cabeçalho: %w", err)
	}

	colIndexes := make(map[string]int, len(rawHeader))
	for i, col := range rawHeader {
		name := strings.ToUpper(strings.Trim(col, `"`))
		if mapped, ok := singleColumnMapping[name]; ok {
			name = mapped
		}
		colIndexes[name] = i
	}

	targetCols := make([]string, len(singleBuColumnLengths))
	for i, col := range singleBuColumnLengths {
		targetCols[i] = col[0]
	}

	batchSize := singlePgMaxPlaceholders / len(targetCols)
	if batchSize < 1 {
		batchSize = 1
	}

	conflictCols := strings.Join(singleQuoteCols(singleKeyColumns), ", ")
	insertHead := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `,
		singleTableName, strings.Join(singleQuoteCols(targetCols), ","))
	onConflict := fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`, conflictCols)

	var (
		batch      [][]interface{}
		totalRows  int
		totalBatch int
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := singleExecBatch(db, insertHead, onConflict, batch); err != nil {
			return err
		}
		totalBatch++
		fmt.Printf("\r  lotes enviados: %d  |  linhas lidas: %d", totalBatch, totalRows)
		batch = batch[:0]
		return nil
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nAviso CSV linha %d: %v\n", totalRows+1, err)
			continue
		}

		row := make([]interface{}, len(targetCols))
		for i, colName := range targetCols {
			if idx, ok := colIndexes[colName]; ok && idx < len(record) {
				v := strings.TrimSpace(record[idx])
				if v == "" {
					row[i] = nil
				} else if colName == "DS_CARGO_PERGUNTA" || colName == "DS_CARGO_PERGUNTA_SECAO" {
					row[i] = strings.ToUpper(v)
				} else {
					row[i] = v
				}
			}
		}
		batch = append(batch, row)
		totalRows++

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return fmt.Errorf("inserir lote: %w", err)
			}
		}
	}

	if err := flush(); err != nil {
		return fmt.Errorf("inserir lote final: %w", err)
	}

	fmt.Printf("\nTotal de linhas processadas: %d\n", totalRows)
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func singleQuoteCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = fmt.Sprintf(`"%s"`, c)
	}
	return out
}

func singleIsRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "57p01") ||
		strings.Contains(msg, "57p03") ||
		strings.Contains(msg, "08006") ||
		strings.Contains(msg, "terminating connection") ||
		strings.Contains(msg, "shutting down") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}

func singleExecBatch(db *sql.DB, insertHead, onConflict string, batch [][]interface{}) error {
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

	const maxRetries = 6
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err := db.Exec(query, args...)
		if err == nil {
			return nil
		}
		if !singleIsRetryable(err) {
			return err
		}
		wait := time.Duration(1<<uint(attempt-1)) * 10 * time.Second
		fmt.Fprintf(os.Stderr, "\nConexão perdida (tentativa %d/%d), aguardando %v: %v\n", attempt, maxRetries, wait, err)
		time.Sleep(wait)
		for range 12 {
			if db.Ping() == nil {
				break
			}
			time.Sleep(5 * time.Second)
		}
	}
	return fmt.Errorf("lote falhou após %d tentativas de reconexão", maxRetries)
}
