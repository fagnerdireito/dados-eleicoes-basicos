use anyhow::{Context, Result};
use eleicoes_etl_postgres::common::{ensure_index, load_dotenv_quiet, postgres_pool};
use std::time::Instant;

const INSERT_SQL: &str = r#"
		INSERT INTO "votos_partido" (
			"NR_PARTIDO", "SG_PARTIDO", "NM_PARTIDO", "total_votos", "ANO_ELEICAO",
			"NM_MUNICIPIO", "CD_MUNICIPIO", "CD_ELEICAO", "NR_TURNO",
			"SG_UF", "DS_CARGO_PERGUNTA", "CD_CARGO_PERGUNTA"
		)
		SELECT
			bu."NR_PARTIDO", bu."SG_PARTIDO", bu."NM_PARTIDO", SUM(CAST(bu."QT_VOTOS" AS BIGINT)), bu."ANO_ELEICAO",
			bu."NM_MUNICIPIO", bu."CD_MUNICIPIO", bu."CD_ELEICAO", bu."NR_TURNO",
			bu."SG_UF", MAX(bu."DS_CARGO_PERGUNTA"), bu."CD_CARGO_PERGUNTA"
		FROM "boletim_de_urna" bu
		WHERE bu."ANO_ELEICAO" = $1 AND bu."CD_MUNICIPIO" = $2
		GROUP BY bu."ANO_ELEICAO", bu."CD_MUNICIPIO", bu."NM_MUNICIPIO", bu."CD_ELEICAO", bu."NR_TURNO", bu."SG_UF", bu."CD_CARGO_PERGUNTA", bu."NR_PARTIDO", bu."SG_PARTIDO", bu."NM_PARTIDO""#;

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = postgres_pool(1)?;
    let mut conn = pool.get().context("conectar")?;

    println!("=== Iniciando Processamento Sequencial Estável — Votos Partido (PostgreSQL) ===");
    let _ = conn.execute("SET lock_timeout = '300s'", &[]);

    println!("Verificando índices de origem...");
    ensure_index(
        &mut conn,
        "boletim_de_urna",
        "idx_bu_etl_partido",
        "ANO_ELEICAO, CD_MUNICIPIO, NR_PARTIDO",
    );

    println!("Recriando tabela votos_partido...");
    conn.execute(r#"DROP TABLE IF EXISTS "votos_partido""#, &[])
        .context("drop votos_partido")?;
    conn.execute(
        r#"CREATE TABLE "votos_partido" (
			id BIGSERIAL PRIMARY KEY,
			"NR_PARTIDO" VARCHAR(50),
			"SG_PARTIDO" VARCHAR(50),
			"NM_PARTIDO" VARCHAR(255),
			"total_votos" BIGINT,
			"ANO_ELEICAO" VARCHAR(10),
			"NM_MUNICIPIO" VARCHAR(255),
			"CD_MUNICIPIO" VARCHAR(50),
			"CD_ELEICAO" VARCHAR(50),
			"NR_TURNO" VARCHAR(10),
			"SG_UF" VARCHAR(10),
			"DS_CARGO_PERGUNTA" VARCHAR(255),
			"CD_CARGO_PERGUNTA" VARCHAR(50)
		)"#,
        &[],
    )
    .context("criar tabela alvo")?;

    let rows = conn
        .query(
            r#"SELECT DISTINCT "ANO_ELEICAO", "CD_MUNICIPIO" FROM "boletim_de_urna" WHERE "ANO_ELEICAO" IS NOT NULL ORDER BY "ANO_ELEICAO", "CD_MUNICIPIO""#,
            &[],
        )
        .context("listar municípios")?;
    let muns: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();

    let total = muns.len();
    println!("Total de {total} municípios para carregar.");

    let start = Instant::now();
    if total == 0 {
        println!("Nada a processar.");
        return Ok(());
    }

    for (i, (ano, cod)) in muns.iter().enumerate() {
        let mut r = conn.execute(INSERT_SQL, &[&ano.as_str(), &cod.as_str()]);

        if let Err(ref e) = r {
            let msg = e.to_string();
            if msg.contains("40P01") || msg.to_lowercase().contains("deadlock") {
                eprintln!("\nDeadlock em {ano}-{cod}, tentando novamente...");
                r = conn.execute(INSERT_SQL, &[&ano.as_str(), &cod.as_str()]);
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
