package main

import (
	"archive/zip"
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
	"golang.org/x/text/encoding/charmap"
)

// Importa o dataset "comparecimento_abstencao" do TSE — granularidade
// seção × perfil (gênero, faixa etária, escolaridade) com colunas QT_APTOS,
// QT_COMPARECIMENTO e QT_ABSTENCAO. É a fonte autoritativa para a aba
// "Perfil do eleitorado" do app.
//
// Onde baixar (ex.: 2024):
//   https://cdn.tse.jus.br/estatistica/sead/odsele/comparecimento_abstencao/comparecimento_abstencao_2024.zip
// Coloque o ZIP em dados/perfil/ e rode `go run 12_import_comparecimento_abstencao.go`.

const (
	caTableName         = "comparecimento_abstencao"
	caDadosRelativeDir  = "dados/perfil"
	caZipPrefix         = "comparecimento_abstencao_"
	caCsvSeparator      = ';'
	caMaxPlaceholders   = 65_535
	caBatchRowsFallback = 1500
)

// Schema baseado no leiame do TSE. Pode haver pequenas variações entre anos;
// se o cabeçalho do CSV trouxer uma coluna desconhecida, ela é ignorada
// silenciosamente (o INSERT só usa as colunas alvo abaixo).
var caColumnLengths = [][]string{
	{"DT_GERACAO", "10"},
	{"HH_GERACAO", "8"},
	{"ANO_ELEICAO", "4"},
	{"CD_TIPO_ELEICAO", "5"},
	{"NM_TIPO_ELEICAO", "60"},
	{"NR_TURNO", "1"},
	{"CD_ELEICAO", "10"},
	{"DS_ELEICAO", "120"},
	{"DT_ELEICAO", "10"},
	{"TP_ABRANGENCIA", "10"},
	{"SG_UF", "2"},
	{"CD_MUNICIPIO", "10"},
	{"NM_MUNICIPIO", "100"},
	{"NR_ZONA", "10"},
	{"NR_SECAO", "10"},
	{"CD_GENERO", "5"},
	{"DS_GENERO", "30"},
	{"CD_ESTADO_CIVIL", "5"},
	{"DS_ESTADO_CIVIL", "30"},
	{"CD_FAIXA_ETARIA", "5"},
	{"DS_FAIXA_ETARIA", "30"},
	{"CD_GRAU_ESCOLARIDADE", "5"},
	{"DS_GRAU_ESCOLARIDADE", "60"},
	{"QT_APTOS", "20"},
	{"QT_COMPARECIMENTO", "20"},
	{"QT_ABSTENCAO", "20"},
}

var caKeyColumns = []string{
	"ANO_ELEICAO",
	"NR_TURNO",
	"SG_UF",
	"CD_MUNICIPIO",
	"NR_ZONA",
	"NR_SECAO",
	"CD_GENERO",
	"CD_ESTADO_CIVIL",
	"CD_FAIXA_ETARIA",
	"CD_GRAU_ESCOLARIDADE",
}

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")

	host := caEnv("PGSQL_VECTOR_HOST", "127.0.0.1")
	port := caEnv("PGSQL_VECTOR_PORT", "5432")
	dbname := caEnv("PGSQL_VECTOR_DATABASE", "eleicoes")
	user := caEnv("PGSQL_VECTOR_USERNAME", "postgres")
	password := os.Getenv("PGSQL_VECTOR_PASSWORD")

	dsn := caDSN(host, port, user, password, dbname)
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

	if err := caSetupTable(db); err != nil {
		log.Fatalf("erro ao configurar tabela: %v", err)
	}

	zips, err := caDiscoverZips()
	if err != nil {
		log.Fatal(err)
	}
	if len(zips) == 0 {
		fmt.Printf("Nenhum arquivo encontrado em %s/%s*.zip\n", caDadosRelativeDir, caZipPrefix)
		fmt.Println("Sugestão:")
		fmt.Println("  cd dados/perfil")
		fmt.Println("  curl -O https://cdn.tse.jus.br/estatistica/sead/odsele/comparecimento_abstencao/comparecimento_abstencao_2024.zip")
		return
	}

	var totalIns, totalSkp int64
	for _, z := range zips {
		fmt.Printf("\n→ Processando %s\n", filepath.Base(z))
		ins, skp, err := caImportZip(db, z)
		if err != nil {
			log.Printf("  ERRO em %s: %v", z, err)
			continue
		}
		fmt.Printf("  inseridas=%d ignoradas=%d\n", ins, skp)
		totalIns += ins
		totalSkp += skp
	}

	var total int64
	_ = db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, caTableName)).Scan(&total)
	fmt.Printf("\n=== Resumo ===\n")
	fmt.Printf("Inseridas nesta execução: %d\n", totalIns)
	fmt.Printf("Ignoradas (já existiam) : %d\n", totalSkp)
	fmt.Printf("Total na tabela %q: %d\n", caTableName, total)
}

func caRepoRoot() string {
	for _, root := range []string{".", ".."} {
		if st, err := os.Stat(filepath.Join(root, "dados")); err == nil && st.IsDir() {
			return root
		}
	}
	return "."
}

func caDiscoverZips() ([]string, error) {
	dir := filepath.Join(caRepoRoot(), caDadosRelativeDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("ler %s: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, caZipPrefix) || !strings.HasSuffix(name, ".zip") {
			continue
		}
		// evita confundir com perfil_comparecimento_abstencao_*.zip caso convivam
		if strings.HasPrefix(name, "perfil_") {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	return out, nil
}

func caSetupTable(db *sql.DB) error {
	cols := []string{`id BIGSERIAL PRIMARY KEY`}
	for _, c := range caColumnLengths {
		cols = append(cols, fmt.Sprintf(`"%s" VARCHAR(%s) DEFAULT NULL`, c[0], c[1]))
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
  %s
)`, caTableName, strings.Join(cols, ",\n  "))
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("criar tabela: %w", err)
	}
	fmt.Printf("Tabela %q verificada/criada.\n", caTableName)

	idxQuery := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_comparecimento_abstencao ON "%s" (%s)`,
		caTableName, strings.Join(caQuoteCols(caKeyColumns), ", "),
	)
	if _, err := db.Exec(idxQuery); err != nil {
		return fmt.Errorf("criar índice único: %w", err)
	}

	helpers := []string{
		`CREATE INDEX IF NOT EXISTS idx_ca_ano_uf      ON "comparecimento_abstencao" ("ANO_ELEICAO","SG_UF")`,
		`CREATE INDEX IF NOT EXISTS idx_ca_ano_uf_fe   ON "comparecimento_abstencao" ("ANO_ELEICAO","SG_UF","CD_FAIXA_ETARIA")`,
		`CREATE INDEX IF NOT EXISTS idx_ca_ano_uf_esc  ON "comparecimento_abstencao" ("ANO_ELEICAO","SG_UF","CD_GRAU_ESCOLARIDADE")`,
	}
	for _, q := range helpers {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("criar índice auxiliar: %w", err)
		}
	}
	fmt.Println("Índices garantidos (único + auxiliares).")
	return nil
}

func caImportZip(db *sql.DB, zipPath string) (inserted, skipped int64, err error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, 0, fmt.Errorf("abrir zip: %w", err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			continue
		}
		fmt.Printf("  CSV: %s (%d bytes)\n", f.Name, f.UncompressedSize64)
		rc, err := f.Open()
		if err != nil {
			return inserted, skipped, fmt.Errorf("abrir %s: %w", f.Name, err)
		}
		ins, skp, err := caImportCsvStream(db, rc)
		rc.Close()
		if err != nil {
			return inserted + ins, skipped + skp, err
		}
		inserted += ins
		skipped += skp
	}
	return inserted, skipped, nil
}

func caImportCsvStream(db *sql.DB, r io.Reader) (inserted, skipped int64, err error) {
	dec := charmap.ISO8859_1.NewDecoder().Reader(r)
	cr := csv.NewReader(dec)
	cr.Comma = caCsvSeparator
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true
	cr.FieldsPerRecord = -1

	rawHeader, err := cr.Read()
	if err != nil {
		return 0, 0, fmt.Errorf("ler cabeçalho: %w", err)
	}
	colIdx := make(map[string]int, len(rawHeader))
	for i, c := range rawHeader {
		colIdx[strings.ToUpper(strings.Trim(c, `"`))] = i
	}

	target := make([]string, len(caColumnLengths))
	for i, c := range caColumnLengths {
		target[i] = c[0]
	}
	batchSize := caMaxPlaceholders / len(target)
	if batchSize > caBatchRowsFallback {
		batchSize = caBatchRowsFallback
	}

	insertHead := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `,
		caTableName, strings.Join(caQuoteCols(target), ","))
	onConflict := fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`,
		strings.Join(caQuoteCols(caKeyColumns), ", "))

	var batch [][]interface{}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, s, err := caExecBatch(db, insertHead, onConflict, batch)
		if err != nil {
			return err
		}
		inserted += n
		skipped += s
		batch = batch[:0]
		return nil
	}

	lineCount := int64(0)
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("    aviso CSV (linha %d): %v\n", lineCount, err)
			continue
		}
		lineCount++
		row := make([]interface{}, len(target))
		for i, name := range target {
			if idx, ok := colIdx[name]; ok && idx < len(rec) {
				v := strings.TrimSpace(rec[idx])
				if v == "" || v == "#NULO#" || v == "#NULO" {
					row[i] = nil
				} else {
					row[i] = v
				}
			}
		}
		batch = append(batch, row)

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return inserted, skipped, fmt.Errorf("lote: %w", err)
			}
			if lineCount%200000 == 0 {
				fmt.Printf("    progresso: %d linhas\n", lineCount)
			}
		}
	}
	if err := flush(); err != nil {
		return inserted, skipped, fmt.Errorf("lote final: %w", err)
	}
	fmt.Printf("    total lido: %d linhas\n", lineCount)
	return inserted, skipped, nil
}

func caExecBatch(db *sql.DB, head, tail string, batch [][]interface{}) (int64, int64, error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	nCols := len(batch[0])
	tuples := make([]string, 0, len(batch))
	args := make([]interface{}, 0, len(batch)*nCols)
	pos := 1
	for _, row := range batch {
		cells := make([]string, len(row))
		for i := range row {
			cells[i] = fmt.Sprintf("$%d", pos)
			pos++
		}
		tuples = append(tuples, "("+strings.Join(cells, ",")+")")
		args = append(args, row...)
	}
	res, err := db.Exec(head+strings.Join(tuples, ",")+tail, args...)
	if err != nil {
		return 0, 0, err
	}
	aff, _ := res.RowsAffected()
	return aff, int64(len(batch)) - aff, nil
}

func caQuoteCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = fmt.Sprintf(`"%s"`, c)
	}
	return out
}

func caEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func caDSN(host, port, user, password, database string) string {
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
