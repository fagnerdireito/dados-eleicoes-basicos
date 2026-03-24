//! Cria a tabela `municipios` e carrega a partir de `dados/municipio_tse_ibge/municipio_tse_ibge.csv`.
//! Colunas: `id` (BIGSERIAL PK), `estado_id` (FK → `estados.id`), `codigo_tse`, `nome`; único `(estado_id, codigo_tse)`.
//!
//! Estratégia: esvaziar a tabela e inserir de novo (`DELETE` + `INSERT` em lotes), sem `ON CONFLICT`,
//! para evitar falhas quando o esquema no Postgres não bate com o esperado pelo upsert.
//!
//! O CSV é lido como **UTF-8** (com BOM opcional) quando válido; caso contrário **Latin-1** byte a byte.
//!
//! Layout esperado (TSE/IBGE), separador **`;`**:
//! 0 `DT_GERACAO`, 1 `HH_GERACAO`, 2 `CD_UF_TSE`, 3 `CD_UF_IBGE`, 4 `SG_UF`, 5 `NM_UF`,
//! 6 `CD_MUNICIPIO_TSE`, 7 `NM_MUNICIPIO_TSE`, 8 `CD_MUNICIPIO_IBGE`, 9 `NM_MUNICIPIO_IBGE`.
//! Se a 1ª célula for `DT_GERACAO`, essa linha é só cabeçalho e é ignorada nos inserts.

use anyhow::{Context, Result};
use csv::ReaderBuilder;
use eleicoes_etl_postgres::common::{
    load_dotenv_quiet, postgres_pool, resolve_data_dir, PooledPgConn,
};
use postgres::types::ToSql;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;

const CSV_REL_DIR: &str = "dados/municipio_tse_ibge";
const CSV_NAME: &str = "municipio_tse_ibge.csv";

fn norm_header_cell(raw: &str) -> String {
    raw.trim()
        .trim_start_matches('\u{feff}')
        .trim_matches('"')
        .trim()
        .to_uppercase()
}

fn cell(record: &csv::StringRecord, idx: usize) -> String {
    record
        .get(idx)
        .map(|s| s.trim().trim_matches('"').to_string())
        .unwrap_or_default()
}

fn norm_uf(s: &str) -> String {
    s.trim()
        .trim_matches('"')
        .replace('\r', "")
        .to_uppercase()
}

/// UTF-8 (com ou sem BOM) se for válido; senão **Latin-1** byte a byte (ISO-8859-1).
fn read_csv_text(path: &Path) -> Result<String> {
    let mut bytes = std::fs::read(path).with_context(|| format!("ler {}", path.display()))?;
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        bytes.drain(..3);
    }
    if std::str::from_utf8(&bytes).is_ok() {
        return String::from_utf8(bytes).map_err(|e| anyhow::anyhow!(e));
    }
    let mut s = String::with_capacity(bytes.len());
    for &b in &bytes {
        s.push(char::from_u32(u32::from(b)).expect("Latin-1"));
    }
    Ok(s)
}

/// Índices 0-based no layout `municipio_tse_ibge.csv` (linhas de dado).
const IDX_SG_UF: usize = 4;
const IDX_CD_MUN_TSE: usize = 6;
const IDX_NM_MUN_TSE: usize = 7;

fn load_uf_to_estado_id(conn: &mut PooledPgConn) -> Result<HashMap<String, i64>> {
    let rows = conn
        .query(r#"SELECT id, sigla FROM public.estados"#, &[])
        .context(r#"ler tabela estados (rode 6_create_table_estados antes)"#)?;
    let mut m = HashMap::new();
    for row in rows {
        let id: i64 = row.get(0);
        let sg: String = row.get(1);
        let key = norm_uf(&sg);
        if !key.is_empty() {
            m.insert(key, id);
        }
    }
    Ok(m)
}

/// Garante `id BIGSERIAL` como PK e unicidade de `(estado_id, codigo_tse)` em bases antigas.
fn ensure_municipios_schema(conn: &mut PooledPgConn) -> Result<()> {
    let has_id: bool = conn
        .query_one(
            r#"SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'municipios' AND column_name = 'id'
            )"#,
            &[],
        )?
        .get(0);

    if !has_id {
        conn.execute(
            r#"ALTER TABLE public.municipios DROP CONSTRAINT IF EXISTS municipios_pkey"#,
            &[],
        )
        .ok();
        conn.execute(
            r#"ALTER TABLE public.municipios ADD COLUMN id BIGSERIAL PRIMARY KEY"#,
            &[],
        )
        .context("adicionar coluna id (BIGSERIAL) como PK em municipios")?;
        conn.execute(
            r#"CREATE UNIQUE INDEX IF NOT EXISTS municipios_estado_codigo_uq
               ON public.municipios (estado_id, codigo_tse)"#,
            &[],
        )
        .context("índice único (estado_id, codigo_tse) após migração")?;
        println!("Migração: coluna id adicionada; unicidade (estado_id, codigo_tse) preservada.");
    }
    Ok(())
}

fn execute_batch_insert(
    conn: &mut PooledPgConn,
    batch: &[(i64, String, String)],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let n_cols = 3usize;
    let mut placeholders = Vec::new();
    let mut n = 1usize;
    for _ in 0..batch.len() {
        placeholders.push(format!(
            "(${a},${b},${c})",
            a = n,
            b = n + 1,
            c = n + 2
        ));
        n += n_cols;
    }
    let sql = format!(
        "INSERT INTO public.municipios (estado_id, codigo_tse, nome) VALUES {}",
        placeholders.join(",")
    );
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(batch.len() * n_cols);
    for (eid, cod, nm) in batch.iter() {
        params.push(eid as &(dyn ToSql + Sync));
        params.push(cod as &(dyn ToSql + Sync));
        params.push(nm as &(dyn ToSql + Sync));
    }
    conn.execute(&sql, &params[..])
        .context("inserir lote de municípios")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_municipio_row(
    record: &csv::StringRecord,
    i_sg: usize,
    i_cod: usize,
    i_nome: usize,
    uf_to_id: &HashMap<String, i64>,
    batch: &mut Vec<(i64, String, String)>,
    max_batch: usize,
    conn: &mut PooledPgConn,
    total_rows: &mut u64,
    skipped_empty: &mut u64,
    skipped_uf: &mut u64,
    first_bad_uf: &mut Option<String>,
) -> Result<()> {
    let sg = norm_uf(&cell(record, i_sg));
    let cod = cell(record, i_cod);
    let nome = cell(record, i_nome);
    if cod.is_empty() || nome.is_empty() {
        *skipped_empty += 1;
        return Ok(());
    }
    let Some(&estado_id) = uf_to_id.get(&sg) else {
        *skipped_uf += 1;
        if first_bad_uf.is_none() {
            *first_bad_uf = Some(sg.clone());
        }
        return Ok(());
    };
    batch.push((estado_id, cod, nome));
    if batch.len() >= max_batch {
        execute_batch_insert(conn, batch)?;
        *total_rows += batch.len() as u64;
        batch.clear();
    }
    Ok(())
}

fn main() -> Result<()> {
    load_dotenv_quiet();

    let csv_path = resolve_data_dir(CSV_REL_DIR).join(CSV_NAME);
    if !csv_path.is_file() {
        anyhow::bail!(
            "Arquivo não encontrado: {} (caminho absoluto esperado a partir da raiz do repositório)",
            csv_path.display()
        );
    }
    println!("CSV: {}", csv_path.display());

    let pool = postgres_pool(1)?;
    let mut conn = pool.get().context("conectar ao PostgreSQL")?;

    conn.batch_execute("SET search_path TO public")
        .context("search_path public")?;

    conn.execute(
        r#"CREATE TABLE IF NOT EXISTS public.municipios (
    id BIGSERIAL PRIMARY KEY,
    estado_id BIGINT NOT NULL REFERENCES public.estados(id) ON DELETE RESTRICT,
    codigo_tse VARCHAR(20) NOT NULL,
    nome TEXT NOT NULL,
    CONSTRAINT municipios_estado_codigo_key UNIQUE (estado_id, codigo_tse)
)"#,
        &[],
    )
    .context("criar tabela municipios")?;

    println!("Tabela municipios verificada/criada.");

    ensure_municipios_schema(&mut conn)?;

    conn.execute(
        "ALTER TABLE public.municipios ALTER COLUMN nome TYPE TEXT",
        &[],
    )
    .ok();

    let uf_to_id = load_uf_to_estado_id(&mut conn)?;
    if uf_to_id.is_empty() {
        anyhow::bail!(r#"Tabela "estados" vazia — execute 6_create_table_estados primeiro."#);
    }
    println!("Mapeamento UF → estado_id: {} siglas carregadas.", uf_to_id.len());

    let csv_text = read_csv_text(&csv_path)?;
    let mut rdr = ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(Cursor::new(csv_text.into_bytes()));

    let mut first_rec = csv::StringRecord::new();
    if !rdr.read_record(&mut first_rec)? {
        anyhow::bail!("CSV vazio");
    }

    let first_line_is_header = first_rec
        .get(0)
        .is_some_and(|c| norm_header_cell(c) == "DT_GERACAO");

    let i_sg = IDX_SG_UF;
    let i_cod = IDX_CD_MUN_TSE;
    let i_nome = IDX_NM_MUN_TSE;

    if first_line_is_header {
        println!(
            "1ª linha = cabeçalho (DT_GERACAO). Dados: colunas {}=SG_UF, {}=CD_MUNICIPIO_TSE, {}=NM_MUNICIPIO_TSE.",
            i_sg + 1,
            i_cod + 1,
            i_nome + 1
        );
    } else {
        eprintln!(
            "Aviso: 1ª coluna não é DT_GERACAO — tratando a 1ª linha como dado (mesmos índices {}/{}/{}).",
            i_sg + 1,
            i_cod + 1,
            i_nome + 1
        );
    }

    const BATCH_ROWS: usize = 400;
    let max_batch = (65_000 / 3).min(BATCH_ROWS);
    let mut batch: Vec<(i64, String, String)> = Vec::with_capacity(max_batch);
    let mut total_rows = 0u64;
    let mut skipped_uf = 0u64;
    let mut skipped_empty = 0u64;
    let mut skipped_parse = 0u64;
    let mut first_bad_uf: Option<String> = None;

    conn.execute("BEGIN", &[])
        .context("BEGIN transação")?;

    conn.execute("DELETE FROM public.municipios", &[])
        .context("DELETE FROM municipios (recarga completa)")?;
    println!("Registros anteriores removidos; importando CSV…");

    let import_result: Result<()> = (|| {
        if !first_line_is_header {
            apply_municipio_row(
                &first_rec,
                i_sg,
                i_cod,
                i_nome,
                &uf_to_id,
                &mut batch,
                max_batch,
                &mut conn,
                &mut total_rows,
                &mut skipped_empty,
                &mut skipped_uf,
                &mut first_bad_uf,
            )?;
        }
        let mut record = csv::StringRecord::new();
        loop {
            match rdr.read_record(&mut record) {
                Ok(false) => break,
                Ok(true) => apply_municipio_row(
                    &record,
                    i_sg,
                    i_cod,
                    i_nome,
                    &uf_to_id,
                    &mut batch,
                    max_batch,
                    &mut conn,
                    &mut total_rows,
                    &mut skipped_empty,
                    &mut skipped_uf,
                    &mut first_bad_uf,
                )?,
                Err(e) => {
                    skipped_parse += 1;
                    if skipped_parse <= 3 {
                        eprintln!("Aviso: linha CSV ignorada ({e})");
                    }
                }
            }
        }
        if !batch.is_empty() {
            execute_batch_insert(&mut conn, &batch)?;
            total_rows += batch.len() as u64;
        }
        Ok(())
    })();

    if import_result.is_err() {
        let _ = conn.execute("ROLLBACK", &[]);
        return import_result;
    }

    conn.execute("COMMIT", &[])
        .context("COMMIT transação")?;

    let count_db: i64 = conn
        .query_one("SELECT COUNT(*)::bigint FROM public.municipios", &[])?
        .get(0);

    println!("Linhas inseridas (contador): {total_rows}");
    println!("Total na tabela municipios (COUNT): {count_db}");
    if skipped_uf > 0 {
        println!("Linhas ignoradas (UF desconhecida): {skipped_uf}");
        if let Some(ref u) = first_bad_uf {
            println!("(exemplo de UF não encontrada em estados: {:?})", u);
        }
    }
    if skipped_empty > 0 {
        println!("Linhas ignoradas (código ou nome vazio): {skipped_empty}");
    }
    if skipped_parse > 0 {
        println!("Registros com erro de parse CSV (ignorados): {skipped_parse}");
    }

    if count_db == 0 {
        anyhow::bail!(
            "municipios continua vazia. Confira: (1) CSV em {}; (2) cargo run a partir de rust_postgres; (3) se a tabela tiver coluna id/PK diferente, execute no Postgres: DROP TABLE public.municipios CASCADE; e rode este binário de novo.",
            csv_path.display()
        );
    }

    Ok(())
}
