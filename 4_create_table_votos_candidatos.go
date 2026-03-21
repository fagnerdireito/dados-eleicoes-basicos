package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

// Config holds DB configuration
type Config struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

const votosTableName = "votos_candidatos"

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Arquivo .env não encontrado, usando variáveis de ambiente do sistema.")
	}

	config := Config{
		Host:     votosGetEnv("DB_HOST", "127.0.0.1"),
		Port:     votosGetEnv("DB_PORT", "3306"),
		Database: votosGetEnv("DB_DATABASE", "eleicoes"),
		User:     votosGetEnv("DB_USER", votosGetEnv("DB_USERNAME", "root")),
		Password: votosGetEnv("DB_PASSWORD", ""),
	}

	// READ COMMITTED reduces locking pressure for INSERT ... SELECT on large municipalities.
	isolationLevel := url.QueryEscape("'READ-COMMITTED'")
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&transaction_isolation=%s",
		config.User, config.Password, config.Host, config.Port, config.Database, isolationLevel,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}

	if err := ensureSourceIndexes(db); err != nil {
		log.Fatalf("Erro ao criar índices nas tabelas de origem: %v", err)
	}

	if err := recreateTargetTable(db); err != nil {
		log.Fatalf("Erro ao recriar tabela %s: %v", votosTableName, err)
	}

	anos, err := getAnos(db)
	if err != nil {
		log.Fatalf("Erro ao buscar anos: %v", err)
	}

	fmt.Printf("Anos encontrados: %v\n", anos)

	for _, ano := range anos {
		municipios, err := getMunicipios(db, ano)
		if err != nil {
			log.Fatalf("Erro ao buscar municípios do ano %s: %v", ano, err)
		}
		fmt.Printf("Ano %s: %d município(s) encontrado(s).\n", ano, len(municipios))
		for i, mun := range municipios {
			if err := insertForYearMunicipio(db, ano, mun); err != nil {
				log.Fatalf("Erro ao inserir dados do ano %s município %s: %v", ano, mun, err)
			}
			if (i+1)%100 == 0 {
				fmt.Printf("  Ano %s: %d/%d municípios processados.\n", ano, i+1, len(municipios))
			}
		}
		fmt.Printf("Ano %s concluído (%d municípios).\n", ano, len(municipios))
	}

	if err := createTargetIndexes(db); err != nil {
		log.Fatalf("Erro ao criar índices na tabela %s: %v", votosTableName, err)
	}

	fmt.Printf("Tabela '%s' criada com sucesso.\n", votosTableName)
}

func votosGetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func ensureSourceIndexes(db *sql.DB) error {
	fmt.Println("Garantindo índices nas tabelas de origem...")

	indexes := []string{
		`CREATE INDEX idx_bu_join_votos_candidatos
		 ON boletim_de_urna (ANO_ELEICAO, NR_VOTAVEL, SG_UF, CD_CARGO_PERGUNTA, CD_MUNICIPIO)`,
		`CREATE INDEX idx_cc_join_votos_candidatos
		 ON consulta_cand (ANO_ELEICAO, NR_CANDIDATO, SG_UF, CD_CARGO, SG_UE)`,
	}

	for _, stmt := range indexes {
		if _, err := db.Exec(stmt); err != nil {
			s := err.Error()
			if strings.Contains(s, "1061") || strings.Contains(s, "Duplicate key") {
				continue
			}
			return fmt.Errorf("criar índice: %w", err)
		}
	}
	return nil
}

func recreateTargetTable(db *sql.DB) error {
	fmt.Printf("Recriando a tabela '%s'...\n", votosTableName)

	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", votosTableName)); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			NM_URNA_CANDIDATO VARCHAR(255) NULL,
			NM_VOTAVEL VARCHAR(255) NULL,
			total_votos BIGINT UNSIGNED NOT NULL,
			ANO_ELEICAO VARCHAR(10) NULL,
			NM_MUNICIPIO VARCHAR(255) NULL,
			CD_MUNICIPIO VARCHAR(50) NULL,
			CD_ELEICAO VARCHAR(50) NULL,
			NR_TURNO VARCHAR(10) NULL,
			SG_UF VARCHAR(10) NULL,
			DS_CARGO_PERGUNTA VARCHAR(255) NULL,
			SG_PARTIDO VARCHAR(50) NULL,
			SITUACAO_ELEICAO VARCHAR(255) NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
	`, votosTableName)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	fmt.Printf("Tabela '%s' criada.\n", votosTableName)
	return nil
}

func getAnos(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT DISTINCT ANO_ELEICAO
		FROM boletim_de_urna
		WHERE ANO_ELEICAO IS NOT NULL
		ORDER BY ANO_ELEICAO
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var anos []string
	for rows.Next() {
		var ano string
		if err := rows.Scan(&ano); err != nil {
			return nil, err
		}
		anos = append(anos, ano)
	}
	return anos, rows.Err()
}

func getMunicipios(db *sql.DB, ano string) ([]string, error) {
	rows, err := db.Query(`
		SELECT DISTINCT CD_MUNICIPIO
		FROM boletim_de_urna
		WHERE ANO_ELEICAO = ? AND CD_MUNICIPIO IS NOT NULL
		ORDER BY CD_MUNICIPIO
	`, ano)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var municipios []string
	for rows.Next() {
		var mun string
		if err := rows.Scan(&mun); err != nil {
			return nil, err
		}
		municipios = append(municipios, mun)
	}
	return municipios, rows.Err()
}

func insertForYearMunicipio(db *sql.DB, ano, municipio string) error {
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (
			NM_URNA_CANDIDATO,
			NM_VOTAVEL,
			total_votos,
			ANO_ELEICAO,
			NM_MUNICIPIO,
			CD_MUNICIPIO,
			CD_ELEICAO,
			NR_TURNO,
			SG_UF,
			DS_CARGO_PERGUNTA,
			SG_PARTIDO,
			SITUACAO_ELEICAO
		)
		SELECT
			MAX(cc.NM_URNA_CANDIDATO) AS NM_URNA_CANDIDATO,
			bu.NM_VOTAVEL,
			SUM(CAST(bu.QT_VOTOS AS UNSIGNED)) AS total_votos,
			bu.ANO_ELEICAO,
			bu.NM_MUNICIPIO,
			bu.CD_MUNICIPIO,
			bu.CD_ELEICAO,
			bu.NR_TURNO,
			bu.SG_UF,
			MAX(bu.DS_CARGO_PERGUNTA) AS DS_CARGO_PERGUNTA,
			MAX(cc.SG_PARTIDO) AS SG_PARTIDO,
			MAX(cc.DS_SIT_TOT_TURNO) AS SITUACAO_ELEICAO
		FROM boletim_de_urna AS bu
		LEFT JOIN consulta_cand AS cc
			ON bu.ANO_ELEICAO = cc.ANO_ELEICAO
			AND bu.NR_VOTAVEL = cc.NR_CANDIDATO
			AND bu.SG_UF = cc.SG_UF
			AND bu.CD_CARGO_PERGUNTA = cc.CD_CARGO
			AND bu.CD_MUNICIPIO = cc.SG_UE
		WHERE bu.ANO_ELEICAO = ? AND bu.CD_MUNICIPIO = ?
		GROUP BY
			bu.ANO_ELEICAO,
			bu.CD_MUNICIPIO,
			bu.NM_MUNICIPIO,
			bu.CD_ELEICAO,
			bu.NR_TURNO,
			bu.SG_UF,
			bu.CD_CARGO_PERGUNTA,
			bu.NR_VOTAVEL,
			bu.NM_VOTAVEL
	`, votosTableName)

	_, err := db.Exec(insertSQL, ano, municipio)
	if err != nil {
		return fmt.Errorf("insert ano %s município %s: %w", ano, municipio, err)
	}
	return nil
}

func createTargetIndexes(db *sql.DB) error {
	fmt.Printf("Criando índices na tabela '%s'...\n", votosTableName)

	indexes := []string{
		fmt.Sprintf("CREATE INDEX idx_%s_ano ON %s (ANO_ELEICAO)", votosTableName, votosTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_municipio ON %s (CD_MUNICIPIO)", votosTableName, votosTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_uf ON %s (SG_UF)", votosTableName, votosTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_turno ON %s (NR_TURNO)", votosTableName, votosTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_votos ON %s (total_votos)", votosTableName, votosTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_ano_municipio ON %s (ANO_ELEICAO, CD_MUNICIPIO)", votosTableName, votosTableName),
	}

	for _, stmt := range indexes {
		if _, err := db.Exec(stmt); err != nil {
			s := err.Error()
			if strings.Contains(s, "1061") || strings.Contains(s, "Duplicate key") {
				continue
			}
			return fmt.Errorf("criar índice: %w", err)
		}
	}

	fmt.Println("Índices criados com sucesso.")
	return nil
}
