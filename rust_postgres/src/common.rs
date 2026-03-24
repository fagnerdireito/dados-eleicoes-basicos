//! Conexão PostgreSQL, descoberta de CSVs e importação em lote (equivalente aos scripts Go).

use anyhow::{Context, Result};
use csv::ReaderBuilder;
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use postgres::types::ToSql;
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use walkdir::WalkDir;

pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;
pub type PooledPgConn = r2d2::PooledConnection<PostgresConnectionManager<NoTls>>;

/// Limite de placeholders por statement no PostgreSQL.
pub const PG_MAX_PARAMS: usize = 65_535;

static PRINT_LOCK: Mutex<()> = Mutex::new(());

fn locked_println(args: std::fmt::Arguments<'_>) {
    let _g = PRINT_LOCK.lock().unwrap();
    println!("{args}");
}

fn locked_eprintln(args: std::fmt::Arguments<'_>) {
    let _g = PRINT_LOCK.lock().unwrap();
    eprintln!("{args}");
}

pub fn load_dotenv_quiet() {
    for path in [Path::new(".env"), Path::new("../.env")] {
        if path.is_file() {
            let _ = dotenvy::from_path(path);
            break;
        }
    }
}

pub fn get_env(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

/// Credenciais: `PGSQL_VECTOR_HOST`, `PGSQL_VECTOR_PORT`, `PGSQL_VECTOR_DATABASE`, `PGSQL_VECTOR_USERNAME`, `PGSQL_VECTOR_PASSWORD`.
pub fn postgres_pool(max_open: usize) -> Result<PgPool> {
    let host = get_env("PGSQL_VECTOR_HOST", "127.0.0.1");
    let port: u16 = get_env("PGSQL_VECTOR_PORT", "5432")
        .parse()
        .context("PGSQL_VECTOR_PORT inválido")?;
    let database = get_env("PGSQL_VECTOR_DATABASE", "eleicoes");
    let user = get_env("PGSQL_VECTOR_USERNAME", "postgres");
    let password = std::env::var("PGSQL_VECTOR_PASSWORD").unwrap_or_default();

    let mut cfg = postgres::Config::new();
    cfg.host(&host);
    cfg.port(port);
    cfg.dbname(&database);
    cfg.user(&user);
    if !password.is_empty() {
        cfg.password(&password);
    }

    let manager = PostgresConnectionManager::new(cfg, NoTls);
    Pool::builder()
        .max_size(max_open.max(1) as u32)
        .build(manager)
        .context("criar pool PostgreSQL")
}

/// `rel` é relativo à raiz do repositório (`bweb`, `dados`). Funciona com CWD na raiz ou em `rust_postgres/`.
pub fn resolve_data_dir(rel: &str) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if cwd.join(rel).is_dir() {
        return cwd.join(rel);
    }
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let parent = manifest.parent().unwrap_or(manifest);
    parent.join(rel)
}

/// Lista `.csv` recursivamente. Se `dir_prefix` for `Some(p)`, não desce em pastas cujo nome não começa com `p` (exceto a raiz).
pub fn find_csv_files(root: &Path, dir_prefix: Option<&str>) -> Result<Vec<PathBuf>> {
    let root = root
        .canonicalize()
        .with_context(|| format!("path inválido: {}", root.display()))?;
    let mut out = Vec::new();
    let walker = WalkDir::new(&root).follow_links(false);
    for ent in walker.into_iter().filter_entry(|e| {
        if !e.file_type().is_dir() {
            return true;
        }
        if e.path() == root.as_path() {
            return true;
        }
        if let Some(prefix) = dir_prefix {
            let base = e.file_name().to_string_lossy();
            return base.starts_with(prefix);
        }
        true
    }) {
        let ent = ent?;
        if ent.file_type().is_file() {
            let p = ent.path();
            if p.extension().is_some_and(|e| e.eq_ignore_ascii_case("csv")) {
                out.push(p.to_path_buf());
            }
        }
    }
    out.sort();
    Ok(out)
}

pub fn quote_columns_pg(cols: &[&str]) -> Vec<String> {
    cols.iter().map(|c| format!(r#""{c}""#)).collect()
}

pub fn setup_table(
    conn: &mut PooledPgConn,
    table_name: &str,
    columns: &[(&str, &str)],
    key_columns: &[&str],
    unique_index_name: &str,
) -> Result<()> {
    let mut parts: Vec<String> = vec!["id BIGSERIAL PRIMARY KEY".into()];
    for (name, len) in columns {
        parts.push(format!(r#""{name}" VARCHAR({len}) DEFAULT NULL"#));
    }
    let create = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}" (
  {}
)"#,
        table_name,
        parts.join(",\n  ")
    );
    conn.execute(&create, &[])
        .with_context(|| format!("criar tabela {table_name}"))?;
    locked_println(format_args!("Tabela `{table_name}` verificada/criada."));

    let idx_cols = quote_columns_pg(key_columns).join(", ");
    let idx_sql = format!(
        r#"CREATE UNIQUE INDEX IF NOT EXISTS "{}" ON "{}" ({})"#,
        unique_index_name, table_name, idx_cols
    );
    conn.execute(&idx_sql, &[])
        .with_context(|| format!("criar índice único em {table_name}"))?;
    locked_println(format_args!(
        "Índice único ({unique_index_name}) verificado/criado."
    ));
    Ok(())
}

fn normalize_header_cell(raw: &str, renames: &[(&str, &str)]) -> String {
    let mut name = raw.trim().trim_matches('"').to_uppercase();
    for (from, to) in renames {
        if name == *from {
            name = (*to).to_string();
            break;
        }
    }
    name
}

fn row_values(
    record: &csv::StringRecord,
    target_cols: &[&str],
    col_indexes: &HashMap<String, usize>,
) -> Vec<Option<String>> {
    let mut row = vec![None; target_cols.len()];
    for (i, col_name) in target_cols.iter().enumerate() {
        if let Some(&idx) = col_indexes.get(*col_name) {
            if let Some(v) = record.get(idx) {
                if !v.is_empty() {
                    row[i] = Some(v.to_string());
                }
            }
        }
    }
    row
}

fn execute_batch(
    conn: &mut PooledPgConn,
    table_name: &str,
    target_cols: &[&str],
    key_columns: &[&str],
    batch: &[Vec<Option<String>>],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let col_list = quote_columns_pg(target_cols).join(", ");
    let conflict_cols = quote_columns_pg(key_columns).join(", ");
    let n_cols = target_cols.len();
    let mut placeholders = Vec::new();
    let mut param_num = 1usize;
    for _ in 0..batch.len() {
        let row_ph: Vec<String> = (0..n_cols)
            .map(|_| {
                let p = format!("${}", param_num);
                param_num += 1;
                p
            })
            .collect();
        placeholders.push(format!("({})", row_ph.join(",")));
    }
    let sql = format!(
        r#"INSERT INTO "{}" ({}) VALUES {} ON CONFLICT ({}) DO NOTHING"#,
        table_name,
        col_list,
        placeholders.join(","),
        conflict_cols
    );
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(batch.len() * n_cols);
    for row in batch {
        for cell in row {
            params.push(cell as &(dyn ToSql + Sync));
        }
    }
    conn.execute(&sql, &params[..])
        .context("inserir lote")?;
    Ok(())
}

pub struct CsvImportParams {
    pub table_name: &'static str,
    pub columns: &'static [(&'static str, &'static str)],
    pub key_columns: &'static [&'static str],
    pub unique_index_name: &'static str,
    pub data_subdir: &'static str,
    pub dir_prefix: Option<&'static str>,
    pub column_renames: &'static [(&'static str, &'static str)],
    pub max_workers: usize,
    pub csv_separator: u8,
}

pub fn run_csv_import(pool: &PgPool, p: CsvImportParams) -> Result<()> {
    let root = resolve_data_dir(p.data_subdir);
    if !root.is_dir() {
        anyhow::bail!("Diretório não encontrado: {}", root.display());
    }

    let mut conn = pool.get().context("conexão inicial")?;
    setup_table(
        &mut conn,
        p.table_name,
        p.columns,
        p.key_columns,
        p.unique_index_name,
    )?;

    let files = find_csv_files(&root, p.dir_prefix)?;
    if files.is_empty() {
        println!(
            "Nenhum arquivo CSV encontrado em '{}'.",
            root.display()
        );
        return Ok(());
    }

    let workers = p.max_workers.min(files.len()).max(1);
    let chunks = partition_round_robin(&files, workers);
    locked_println(format_args!(
        "Encontrados {} arquivo(s) CSV. Usando {} worker(s).",
        files.len(),
        workers
    ));

    let pool = pool.clone();
    let table_name = p.table_name.to_string();
    let key_columns: Vec<&'static str> = p.key_columns.to_vec();
    let columns: Vec<(&'static str, &'static str)> = p.columns.to_vec();
    let column_renames: Vec<(&'static str, &'static str)> = p.column_renames.to_vec();
    let sep = p.csv_separator;

    let handles: Vec<_> = chunks
        .into_iter()
        .enumerate()
        .filter(|(_, paths)| !paths.is_empty())
        .map(|(_id, paths)| {
            let pool = pool.clone();
            let table_name = table_name.clone();
            let key_columns = key_columns.clone();
            let columns = columns.clone();
            let column_renames = column_renames.clone();
            std::thread::spawn(move || -> Vec<(PathBuf, anyhow::Error)> {
                let mut errors = Vec::new();
                let mut conn = match pool.get() {
                    Ok(c) => c,
                    Err(e) => {
                        errors.push((
                            PathBuf::from("<pool>"),
                            anyhow::Error::new(e).context("get_conn"),
                        ));
                        return errors;
                    }
                };
                for path in paths {
                    if let Err(e) = process_one_csv_file(
                        &mut conn,
                        &path,
                        &table_name,
                        &columns,
                        key_columns.as_slice(),
                        &column_renames,
                        sep,
                    ) {
                        errors.push((path, e));
                    }
                }
                errors
            })
        })
        .collect();

    let mut all_errors = Vec::new();
    for h in handles {
        match h.join() {
            Ok(v) => all_errors.extend(v),
            Err(_) => eprintln!("worker panicked"),
        }
    }

    for (path, e) in &all_errors {
        locked_eprintln(format_args!("ERRO em {}: {:#}", path.display(), e));
    }

    if !all_errors.is_empty() {
        println!(
            "\nProcessamento concluído com {} erro(s).",
            all_errors.len()
        );
    } else {
        println!("\nProcessamento concluído com sucesso.");
    }
    Ok(())
}

fn partition_round_robin<T: Clone>(items: &[T], n: usize) -> Vec<Vec<T>> {
    let mut out: Vec<Vec<T>> = (0..n).map(|_| Vec::new()).collect();
    for (i, item) in items.iter().enumerate() {
        out[i % n].push(item.clone());
    }
    out
}

fn process_one_csv_file(
    conn: &mut PooledPgConn,
    path: &Path,
    table_name: &str,
    columns: &[(&str, &str)],
    key_columns: &[&str],
    column_renames: &[(&str, &str)],
    sep: u8,
) -> Result<()> {
    locked_println(format_args!("Iniciando: {}", path.display()));

    let f = File::open(path).with_context(|| format!("abrir {}", path.display()))?;
    let decoder = DecodeReaderBytesBuilder::new()
        .encoding(Some(WINDOWS_1252))
        .build(f);

    let mut rdr = ReaderBuilder::new()
        .delimiter(sep)
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);

    let mut raw_header = csv::StringRecord::new();
    if !rdr
        .read_record(&mut raw_header)
        .context("ler cabeçalho")?
    {
        anyhow::bail!("CSV sem cabeçalho");
    }

    let mut col_indexes = HashMap::with_capacity(raw_header.len());
    for (i, col) in raw_header.iter().enumerate() {
        let name = normalize_header_cell(col, column_renames);
        col_indexes.insert(name, i);
    }

    let target_cols: Vec<&str> = columns.iter().map(|(n, _)| *n).collect();
    let batch_size = PG_MAX_PARAMS / target_cols.len().max(1);
    let batch_size = batch_size.max(1);

    let mut batch: Vec<Vec<Option<String>>> = Vec::with_capacity(batch_size);

    let flush_batch = |conn: &mut PooledPgConn, batch: &mut Vec<Vec<Option<String>>>| -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        execute_batch(
            conn,
            table_name,
            target_cols.as_slice(),
            key_columns,
            batch,
        )
        .context("inserir lote")?;
        batch.clear();
        Ok(())
    };

    let mut record = csv::StringRecord::new();
    loop {
        match rdr.read_record(&mut record) {
            Ok(false) => break,
            Ok(true) => {
                batch.push(row_values(&record, &target_cols, &col_indexes));
                if batch.len() >= batch_size {
                    flush_batch(conn, &mut batch).context("inserir lote")?;
                }
            }
            Err(e) => {
                locked_eprintln(format_args!("Aviso CSV em {}: {}", path.display(), e));
                continue;
            }
        }
    }

    flush_batch(conn, &mut batch).context("inserir lote final")?;
    locked_println(format_args!("Finalizado: {}", path.display()));
    Ok(())
}

pub fn ensure_index(conn: &mut PooledPgConn, table: &str, name: &str, cols: &str) {
    let cols_quoted = cols
        .split(',')
        .map(|s| {
            let t = s.trim();
            format!(r#""{t}""#)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"CREATE INDEX IF NOT EXISTS "{}" ON "{}" ({})"#,
        name, table, cols_quoted
    );
    if let Err(e) = conn.execute(&sql, &[]) {
        eprintln!("Aviso índice {name}: {e}");
    }
}
