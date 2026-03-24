//! Conexão MySQL, descoberta de CSVs e importação em lote (equivalente aos scripts Go).

use anyhow::{anyhow, Context, Result};
use csv::ReaderBuilder;
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use mysql::prelude::*;
use mysql::{OptsBuilder, Pool, PoolConstraints, PoolOpts, PooledConn, Value};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use walkdir::WalkDir;

/// Limite de placeholders por statement no MySQL.
pub const MYSQL_MAX_PLACEHOLDERS: usize = 65_535;

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

/// Igual ao Go: `DB_USER` ou `DB_USERNAME`.
pub fn db_user() -> String {
    std::env::var("DB_USER")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("DB_USERNAME").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "root".to_string())
}

/// Pool TCP (sem socket Unix), charset implícito do cliente.
pub fn mysql_pool(max_open: usize) -> Result<Pool> {
    let host = get_env("DB_HOST", "127.0.0.1");
    let port: u16 = get_env("DB_PORT", "3306")
        .parse()
        .context("DB_PORT inválido")?;
    let database = get_env("DB_DATABASE", "eleicoes");
    let user = db_user();
    let password = get_env("DB_PASSWORD", "");

    let constraints = PoolConstraints::new(0, max_open.max(1))
        .ok_or_else(|| anyhow!("pool max_open inválido"))?;
    let pool_opts = PoolOpts::default().with_constraints(constraints);

    let opts = OptsBuilder::new()
        .prefer_socket(false)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .pool_opts(pool_opts);

    Pool::new(opts).context("criar pool MySQL")
}

/// `rel` é relativo à raiz do repositório (`bweb`, `dados`). Funciona com CWD na raiz ou em `rust/`.
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
    let root = root.canonicalize().with_context(|| format!("path inválido: {}", root.display()))?;
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

pub fn quote_columns(cols: &[&str]) -> Vec<String> {
    cols.iter().map(|c| format!("`{c}`")).collect()
}

pub fn setup_table(
    conn: &mut PooledConn,
    table_name: &str,
    columns: &[(&str, &str)],
    key_columns: &[&str],
    unique_index_name: &str,
) -> Result<()> {
    let mut parts: Vec<String> = vec!["`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT".into()];
    for (name, len) in columns {
        parts.push(format!("`{name}` VARCHAR({len}) DEFAULT NULL"));
    }
    parts.push("PRIMARY KEY (`id`)".into());
    let keys = quote_columns(key_columns).join(",");
    parts.push(format!("UNIQUE KEY `{unique_index_name}` ({keys})"));

    let create = format!(
        "CREATE TABLE IF NOT EXISTS `{table_name}` (\n  {}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
        parts.join(",\n  ")
    );
    conn.exec_drop(create, ())
        .with_context(|| format!("criar tabela {table_name}"))?;
    locked_println(format_args!("Tabela `{table_name}` verificada/criada."));

    let idx_cols = quote_columns(key_columns).join(", ");
    let alter = format!(
        "ALTER TABLE `{table_name}` ADD UNIQUE INDEX {unique_index_name} ({idx_cols})"
    );
    match conn.exec_drop(alter, ()) {
        Ok(()) => locked_println(format_args!(
            "Índice único ({unique_index_name}) criado com sucesso."
        )),
        Err(e) => {
            let s = e.to_string();
            if s.contains("1061") || s.contains("Duplicate") {
                locked_println(format_args!(
                    "Índice único ({unique_index_name}) já existe."
                ));
            } else {
                return Err(e).with_context(|| format!("criar índice em {table_name}"));
            }
        }
    }
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

pub fn execute_batch(
    conn: &mut PooledConn,
    base_query: &str,
    placeholder: &str,
    batch: &[Vec<Option<String>>],
) -> mysql::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut sql = String::with_capacity(base_query.len() + (placeholder.len() + 1) * batch.len());
    sql.push_str(base_query);
    for (i, _) in batch.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(placeholder);
    }
    let cols = batch[0].len();
    let mut values: Vec<Value> = Vec::with_capacity(batch.len() * cols);
    for row in batch {
        for cell in row {
            values.push(match cell {
                None => Value::NULL,
                Some(s) if s.is_empty() => Value::NULL,
                Some(s) => Value::from(s.clone()),
            });
        }
    }
    conn.exec_drop(sql, values)
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

pub fn run_csv_import(pool: &Pool, p: CsvImportParams) -> Result<()> {
    let root = resolve_data_dir(p.data_subdir);
    if !root.is_dir() {
        anyhow::bail!("Diretório não encontrado: {}", root.display());
    }

    let mut conn = pool.get_conn().context("conexão inicial")?;
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
            let columns = columns.clone();
            let column_renames = column_renames.clone();
            std::thread::spawn(move || -> Vec<(PathBuf, anyhow::Error)> {
                let mut errors = Vec::new();
                let mut conn = match pool.get_conn() {
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
    conn: &mut PooledConn,
    path: &Path,
    table_name: &str,
    columns: &[(&str, &str)],
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
    let batch_size = MYSQL_MAX_PLACEHOLDERS / target_cols.len().max(1);
    let batch_size = batch_size.max(1);

    let col_list = quote_columns(target_cols.as_slice()).join(",");
    let placeholder = format!(
        "({})",
        std::iter::repeat("?")
            .take(target_cols.len())
            .collect::<Vec<_>>()
            .join(",")
    );
    let base_query = format!("INSERT IGNORE INTO `{table_name}` ({col_list}) VALUES ");

    let mut batch: Vec<Vec<Option<String>>> = Vec::with_capacity(batch_size);

    let flush_batch = |conn: &mut PooledConn, batch: &mut Vec<Vec<Option<String>>>| -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        execute_batch(conn, &base_query, &placeholder, batch)
            .map_err(|e| anyhow::Error::new(e))
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

pub fn ensure_index(conn: &mut PooledConn, table: &str, name: &str, cols: &str) {
    let sql = format!("CREATE INDEX {name} ON {table} ({cols})");
    match conn.query_drop(&sql) {
        Err(e) if e.to_string().contains("1061") => {}
        Err(e) => eprintln!("Aviso índice {name}: {e}"),
        Ok(()) => {}
    }
}
