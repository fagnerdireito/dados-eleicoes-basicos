use anyhow::{Context, Result};
use eleicoes_etl::common::{ensure_index, load_dotenv_quiet, mysql_pool};
use mysql::prelude::*;
use std::time::Instant;

const INSERT_SQL: &str = r#"
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
		GROUP BY bu.ANO_ELEICAO, bu.CD_MUNICIPIO, bu.NM_MUNICIPIO, bu.CD_ELEICAO, bu.NR_TURNO, bu.SG_UF, bu.CD_CARGO_PERGUNTA, bu.NR_PARTIDO, bu.SG_PARTIDO, bu.NM_PARTIDO"#;

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = mysql_pool(1)?;
    let mut conn = pool.get_conn().context("conectar")?;

    println!("=== Iniciando Processamento Sequencial Estável (Votos Partido) ===");
    println!("Otimizando isolamento e timeouts...");
    let _ = conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
    let _ = conn.query_drop("SET SESSION innodb_lock_wait_timeout = 300");

    println!("Verificando índices de origem...");
    ensure_index(
        &mut conn,
        "boletim_de_urna",
        "idx_bu_etl_partido",
        "ANO_ELEICAO, CD_MUNICIPIO, NR_PARTIDO",
    );

    println!("Recriando tabela votos_partido...");
    conn.query_drop("DROP TABLE IF EXISTS votos_partido")
        .context("drop votos_partido")?;
    conn.query_drop(
        r"CREATE TABLE votos_partido (
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
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
    )
    .context("criar tabela alvo")?;

    let muns: Vec<(String, String)> = conn
        .query(
            "SELECT DISTINCT ANO_ELEICAO, CD_MUNICIPIO FROM boletim_de_urna WHERE ANO_ELEICAO IS NOT NULL ORDER BY ANO_ELEICAO, CD_MUNICIPIO",
        )
        .context("listar municípios")?;

    let total = muns.len();
    println!("Total de {total} municípios para carregar.");

    let start = Instant::now();
    if total == 0 {
        println!("Nada a processar.");
        return Ok(());
    }

    for (i, (ano, cod)) in muns.iter().enumerate() {
        let mut r = conn.exec_drop(INSERT_SQL, (ano.as_str(), cod.as_str()));

        if let Err(ref e) = r {
            if e.to_string().contains("1206") {
                eprintln!("\nLock detectado em {ano}-{cod}, reduzindo isolamento...");
                let _ = conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                r = conn.exec_drop(INSERT_SQL, (ano.as_str(), cod.as_str()));
                let _ = conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
            }
        }

        if let Err(e) = r {
            eprintln!("\nErro fatal no município {ano}-{cod}: {e}");
            continue;
        }

        let n = i + 1;
        if n % 100 == 0 || n == total {
            let pct = n as f64 / total as f64 * 100.0;
            let secs = start.elapsed().as_secs();
            print!("\rProgresso: {n}/{total} ({pct:.1}%) - Tempo: {secs}s");
            use std::io::Write;
            let _ = std::io::stdout().flush();
        }
    }
    println!();

    println!("Criando índices finais...");
    ensure_index(
        &mut conn,
        "votos_partido",
        "idx_vp_busca",
        "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO",
    );
    ensure_index(&mut conn, "votos_partido", "idx_vp_partido", "SG_PARTIDO");

    println!("Carga de partidos finalizada em {:?}!", start.elapsed());
    Ok(())
}
