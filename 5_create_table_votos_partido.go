package main

import (
	"database/sql"
	"fmt"
	"log"
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

const partidoTableName = "votos_partido"

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Arquivo .env não encontrado, usando variáveis de ambiente do sistema.")
	}

	config := Config{
		Host:     partidoGetEnv("DB_HOST", "127.0.0.1"),
		Port:     partidoGetEnv("DB_PORT", "3306"),
		Database: partidoGetEnv("DB_DATABASE", "eleicoes"),
		User:     partidoGetEnv("DB_USER", partidoGetEnv("DB_USERNAME", "root")),
		Password: partidoGetEnv("DB_PASSWORD", ""),
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port, config.Database)

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

	if err := partidoEnsureSourceIndexes(db); err != nil {
		log.Fatalf("Erro ao criar índices na tabela de origem: %v", err)
	}

	if err := partidoRecreateTargetTable(db); err != nil {
		log.Fatalf("Erro ao recriar tabela %s: %v", partidoTableName, err)
	}

	anos, err := partidoGetAnos(db)
	if err != nil {
		log.Fatalf("Erro ao buscar anos: %v", err)
	}

	fmt.Printf("Anos encontrados: %v\n", anos)

	for _, ano := range anos {
		if err := partidoInsertForYear(db, ano); err != nil {
			log.Fatalf("Erro ao inserir dados do ano %s: %v", ano, err)
		}
	}

	if err := partidoCreateTargetIndexes(db); err != nil {
		log.Fatalf("Erro ao criar índices na tabela %s: %v", partidoTableName, err)
	}

	fmt.Printf("Tabela '%s' criada com sucesso.\n", partidoTableName)
}

func partidoGetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func partidoEnsureSourceIndexes(db *sql.DB) error {
	fmt.Println("Garantindo índices na tabela de origem...")

	indexes := []string{
		`CREATE INDEX idx_bu_partido_agrupamento
		 ON boletim_de_urna (ANO_ELEICAO, CD_MUNICIPIO, CD_ELEICAO, NR_TURNO, SG_UF, CD_CARGO_PERGUNTA, NR_PARTIDO)`,
		`CREATE INDEX idx_bu_partido_aux
		 ON boletim_de_urna (ANO_ELEICAO, SG_PARTIDO, NM_PARTIDO)`,
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

func partidoRecreateTargetTable(db *sql.DB) error {
	fmt.Printf("Recriando a tabela '%s'...\n", partidoTableName)

	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", partidoTableName)); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			NR_PARTIDO VARCHAR(50) NULL,
			SG_PARTIDO VARCHAR(50) NULL,
			NM_PARTIDO VARCHAR(255) NULL,
			total_votos BIGINT UNSIGNED NOT NULL,
			ANO_ELEICAO VARCHAR(10) NULL,
			NM_MUNICIPIO VARCHAR(255) NULL,
			CD_MUNICIPIO VARCHAR(50) NULL,
			CD_ELEICAO VARCHAR(50) NULL,
			NR_TURNO VARCHAR(10) NULL,
			SG_UF VARCHAR(10) NULL,
			DS_CARGO_PERGUNTA VARCHAR(255) NULL,
			CD_CARGO_PERGUNTA VARCHAR(50) NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
	`, partidoTableName)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	fmt.Printf("Tabela '%s' criada.\n", partidoTableName)
	return nil
}

func partidoGetAnos(db *sql.DB) ([]string, error) {
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

func partidoInsertForYear(db *sql.DB, ano string) error {
	fmt.Printf("Inserindo dados do ano %s...\n", ano)

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (
			NR_PARTIDO,
			SG_PARTIDO,
			NM_PARTIDO,
			total_votos,
			ANO_ELEICAO,
			NM_MUNICIPIO,
			CD_MUNICIPIO,
			CD_ELEICAO,
			NR_TURNO,
			SG_UF,
			DS_CARGO_PERGUNTA,
			CD_CARGO_PERGUNTA
		)
		SELECT
			bu.NR_PARTIDO,
			bu.SG_PARTIDO,
			bu.NM_PARTIDO,
			SUM(CAST(bu.QT_VOTOS AS UNSIGNED)) AS total_votos,
			bu.ANO_ELEICAO,
			bu.NM_MUNICIPIO,
			bu.CD_MUNICIPIO,
			bu.CD_ELEICAO,
			bu.NR_TURNO,
			bu.SG_UF,
			MAX(bu.DS_CARGO_PERGUNTA) AS DS_CARGO_PERGUNTA,
			bu.CD_CARGO_PERGUNTA
		FROM boletim_de_urna AS bu
		WHERE bu.ANO_ELEICAO = ?
		GROUP BY
			bu.ANO_ELEICAO,
			bu.CD_MUNICIPIO,
			bu.NM_MUNICIPIO,
			bu.CD_ELEICAO,
			bu.NR_TURNO,
			bu.SG_UF,
			bu.CD_CARGO_PERGUNTA,
			bu.NR_PARTIDO,
			bu.SG_PARTIDO,
			bu.NM_PARTIDO
	`, partidoTableName)

	_, err := db.Exec(insertSQL, ano)
	if err != nil {
		return fmt.Errorf("insert ano %s: %w", ano, err)
	}

	fmt.Printf("Ano %s concluído.\n", ano)
	return nil
}

func partidoCreateTargetIndexes(db *sql.DB) error {
	fmt.Printf("Criando índices na tabela '%s'...\n", partidoTableName)

	indexes := []string{
		fmt.Sprintf("CREATE INDEX idx_%s_ano ON %s (ANO_ELEICAO)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_municipio ON %s (CD_MUNICIPIO)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_uf ON %s (SG_UF)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_turno ON %s (NR_TURNO)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_partido ON %s (NR_PARTIDO)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_cargo ON %s (CD_CARGO_PERGUNTA)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_votos ON %s (total_votos)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_ano_municipio ON %s (ANO_ELEICAO, CD_MUNICIPIO)", partidoTableName, partidoTableName),
		fmt.Sprintf("CREATE INDEX idx_%s_ano_uf_partido ON %s (ANO_ELEICAO, SG_UF, NR_PARTIDO)", partidoTableName, partidoTableName),
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
