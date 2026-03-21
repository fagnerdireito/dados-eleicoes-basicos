package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	// Configuração de conexão
	user := getEnv("DB_USER", "root")
	pass := getEnv("DB_PASSWORD", "")
	host := getEnv("DB_HOST", "127.0.0.1")
	port := getEnv("DB_PORT", "3306")
	dbname := getEnv("DB_DATABASE", "eleicoes")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, pass, host, port, dbname)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Erro ao abrir conexão: %v", err)
	}
	defer db.Close()

	// Configurações para estabilidade (Sequencial e sem threads)
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Hour)

	fmt.Println("=== Iniciando Processamento Sequencial Estável (Votos Partido) ===")

	// 1. Otimizações de Sessão para evitar Erro 1206 (Lock table size exceeded)
	fmt.Println("Otimizando isolamento e timeouts...")
	db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	db.Exec("SET SESSION innodb_lock_wait_timeout = 300")

	// 2. Índices de Origem para Performance
	fmt.Println("Verificando índices de origem...")
	ensureIndex(db, "boletim_de_urna", "idx_bu_etl_partido", "ANO_ELEICAO, CD_MUNICIPIO, NR_PARTIDO")

	// 3. Preparar Tabela Alvo
	fmt.Println("Recriando tabela votos_partido...")
	db.Exec("DROP TABLE IF EXISTS votos_partido")
	_, err = db.Exec(`
		CREATE TABLE votos_partido (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			NR_PARTIDO VARCHAR(50),
			SG_PARTIDO VARCHAR(50),
			NM_PARTIDO VARCHAR(255),
			total_votos BIGINT UNSIGNED,
			ANO_ELEICAO VARCHAR(10),
			NM_MUNICIPIO VARCHAR(255),
			CD_MUNICIPIO VARCHAR(50),
			CD_ELEICAO VARCHAR(50),
			NR_TURNO VARCHAR(10),
			SG_UF VARCHAR(10),
			DS_CARGO_PERGUNTA VARCHAR(255),
			CD_CARGO_PERGUNTA VARCHAR(50)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)
	if err != nil {
		log.Fatalf("Erro ao criar tabela alvo: %v", err)
	}

	// 4. Obter lista de Municípios a processar
	rows, err := db.Query("SELECT DISTINCT ANO_ELEICAO, CD_MUNICIPIO FROM boletim_de_urna WHERE ANO_ELEICAO IS NOT NULL ORDER BY ANO_ELEICAO, CD_MUNICIPIO")
	if err != nil {
		log.Fatalf("Erro ao listar municípios: %v", err)
	}
	
	type Mun struct{ Ano, Cod string }
	var muns []Mun
	for rows.Next() {
		var m Mun
		if err := rows.Scan(&m.Ano, &m.Cod); err == nil {
			muns = append(muns, m)
		}
	}
	rows.Close()

	total := len(muns)
	fmt.Printf("Total de %d municípios para carregar.\n", total)

	// 5. Carga de Dados Sequencial (Um por um)
	start := time.Now()
	insertSQL := `
		INSERT INTO votos_partido (
			NR_PARTIDO, SG_PARTIDO, NM_PARTIDO, total_votos, ANO_ELEICAO, 
			NM_MUNICIPIO, CD_MUNICIPIO, CD_ELEICAO, NR_TURNO, 
			SG_UF, DS_CARGO_PERGUNTA, CD_CARGO_PERGUNTA
		)
		SELECT 
			bu.NR_PARTIDO, bu.SG_PARTIDO, bu.NM_PARTIDO, SUM(CAST(bu.QT_VOTOS AS UNSIGNED)), bu.ANO_ELEICAO,
			bu.NM_MUNICIPIO, bu.CD_MUNICIPIO, bu.CD_ELEICAO, bu.NR_TURNO,
			bu.SG_UF, MAX(bu.DS_CARGO_PERGUNTA), bu.CD_CARGO_PERGUNTA
		FROM boletim_de_urna bu
		WHERE bu.ANO_ELEICAO = ? AND bu.CD_MUNICIPIO = ?
		GROUP BY bu.ANO_ELEICAO, bu.CD_MUNICIPIO, bu.NM_MUNICIPIO, bu.CD_ELEICAO, bu.NR_TURNO, bu.SG_UF, bu.CD_CARGO_PERGUNTA, bu.NR_PARTIDO, bu.SG_PARTIDO, bu.NM_PARTIDO`

	for i, m := range muns {
		_, err := db.Exec(insertSQL, m.Ano, m.Cod)

		if err != nil {
			// Se der erro de lock, tentamos uma vez com isolamento mínimo antes de desistir
			if strings.Contains(err.Error(), "1206") {
				fmt.Printf("\nLock detectado em %s-%s, reduzindo isolamento...", m.Ano, m.Cod)
				db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
				_, err = db.Exec(insertSQL, m.Ano, m.Cod)
				db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
			}
			
			if err != nil {
				log.Printf("\nErro fatal no município %s-%s: %v", m.Ano, m.Cod, err)
				continue
			}
		}

		if (i+1)%100 == 0 || i+1 == total {
			pct := float64(i+1) / float64(total) * 100
			fmt.Printf("\rProgresso: %d/%d (%.1f%%) - Tempo: %v", i+1, total, pct, time.Since(start).Truncate(time.Second))
		}
	}
	fmt.Println()

	// 6. Índices Finais
	fmt.Println("Criando índices finais...")
	ensureIndex(db, "votos_partido", "idx_vp_busca", "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO")
	ensureIndex(db, "votos_partido", "idx_vp_partido", "SG_PARTIDO")

	fmt.Printf("Carga de partidos finalizada em %v!\n", time.Since(start))
}

func ensureIndex(db *sql.DB, table, name, cols string) {
	_, err := db.Exec(fmt.Sprintf("CREATE INDEX %s ON %s (%s)", name, table, cols))
	if err != nil && !strings.Contains(err.Error(), "1061") {
		log.Printf("Aviso índice %s: %v", name, err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	if v := os.Getenv("DB_USERNAME"); key == "DB_USER" && v != "" {
		return v
	}
	return fallback
}
