use anyhow::{Context, Result};
use eleicoes_etl::common::{ensure_index, load_dotenv_quiet, mysql_pool};
use mysql::prelude::*;
use std::time::Instant;

const INSERT_SQL: &str = r#"
		INSERT INTO votos_candidatos (
			NM_URNA_CANDIDATO, NM_VOTAVEL, total_votos, ANO_ELEICAO,
			NM_MUNICIPIO, CD_MUNICIPIO, CD_ELEICAO, NR_TURNO,
			SG_UF, DS_CARGO_PERGUNTA, SG_PARTIDO, SITUACAO_ELEICAO
		)
		SELECT
			sub.nm_urna, sub.nm_vot, sub.total_votos, sub.ANO_ELEICAO,
			sub.NM_MUNICIPIO, sub.CD_MUNICIPIO, sub.CD_ELEICAO, sub.NR_TURNO,
			sub.SG_UF, sub.DS_CARGO_PERGUNTA, sub.SG_PARTIDO, sub.SITUACAO_ELEICAO
		FROM (
			SELECT
				bu.NM_VOTAVEL AS nm_urna,
				bu.NM_VOTAVEL AS nm_vot,
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
			FROM boletim_de_urna bu
			LEFT JOIN consulta_cand cc ON bu.ANO_ELEICAO = cc.ANO_ELEICAO
				AND bu.NR_VOTAVEL = cc.NR_CANDIDATO
				AND bu.SG_UF = cc.SG_UF
				AND bu.CD_CARGO_PERGUNTA = cc.CD_CARGO
				AND (
					cc.SG_UE = LPAD(bu.CD_MUNICIPIO, 5, '0')
					OR cc.SG_UE = bu.SG_UF
					OR cc.SG_UE = 'BR'
				)
			WHERE bu.ANO_ELEICAO = ? AND bu.CD_MUNICIPIO = ?
			GROUP BY bu.ANO_ELEICAO, bu.CD_MUNICIPIO, bu.NM_MUNICIPIO, bu.CD_ELEICAO, bu.NR_TURNO, bu.SG_UF, bu.CD_CARGO_PERGUNTA, bu.NR_VOTAVEL, bu.NM_VOTAVEL
		) sub
		WHERE NOT EXISTS (
			SELECT 1 FROM votos_candidatos vc
			WHERE vc.ANO_ELEICAO <=> sub.ANO_ELEICAO
				AND vc.CD_MUNICIPIO <=> sub.CD_MUNICIPIO
				AND vc.NM_MUNICIPIO <=> sub.NM_MUNICIPIO
				AND vc.CD_ELEICAO <=> sub.CD_ELEICAO
				AND vc.NR_TURNO <=> sub.NR_TURNO
				AND vc.SG_UF <=> sub.SG_UF
				AND vc.DS_CARGO_PERGUNTA <=> sub.DS_CARGO_PERGUNTA
				AND vc.NM_VOTAVEL <=> sub.nm_vot
		)"#;

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = mysql_pool(1)?;
    let mut conn = pool.get_conn().context("conectar")?;

    println!("=== Iniciando Processamento Sequencial Estável ===");
    println!("Otimizando isolamento e timeouts...");
    let _ = conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
    let _ = conn.query_drop("SET SESSION innodb_lock_wait_timeout = 300");

    println!("Verificando índices de origem...");
    ensure_index(&mut conn, "boletim_de_urna", "idx_bu_etl_base", "ANO_ELEICAO, CD_MUNICIPIO");
    ensure_index(
        &mut conn,
        "consulta_cand",
        "idx_cc_etl_base",
        "ANO_ELEICAO, NR_CANDIDATO, SG_UF, CD_CARGO, SG_UE",
    );

    println!("Garantindo tabela votos_candidatos...");
    conn.query_drop(
        r"CREATE TABLE IF NOT EXISTS votos_candidatos (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			NM_URNA_CANDIDATO VARCHAR(255),
			NM_VOTAVEL VARCHAR(255),
			total_votos BIGINT UNSIGNED,
			ANO_ELEICAO VARCHAR(10),
			NM_MUNICIPIO VARCHAR(255),
			CD_MUNICIPIO VARCHAR(50),
			CD_ELEICAO VARCHAR(50),
			NR_TURNO VARCHAR(10),
			SG_UF VARCHAR(10),
			DS_CARGO_PERGUNTA VARCHAR(255),
			SG_PARTIDO VARCHAR(50),
			SITUACAO_ELEICAO VARCHAR(255)
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
            let elapsed = start.elapsed();
            let secs = elapsed.as_secs();
            print!("\rProgresso: {n}/{total} ({pct:.1}%) - Tempo: {}s", secs);
            use std::io::Write;
            let _ = std::io::stdout().flush();
        }
    }
    println!();

    println!("Criando índices finais...");
    ensure_index(
        &mut conn,
        "votos_candidatos",
        "idx_vc_busca",
        "ANO_ELEICAO, CD_MUNICIPIO, NR_TURNO",
    );
    ensure_index(&mut conn, "votos_candidatos", "idx_vc_uf", "SG_UF");

    println!("Carga finalizada em {:?}!", start.elapsed());
    Ok(())
}
