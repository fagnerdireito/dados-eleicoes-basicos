use anyhow::Result;
use eleicoes_etl_postgres::common::{load_dotenv_quiet, postgres_pool, run_csv_import, CsvImportParams};

const MAX_WORKERS: usize = 2;

static VAGAS_COLUMNS: &[(&str, &str)] = &[
    ("DT_GERACAO", "10"),
    ("HH_GERACAO", "8"),
    ("ANO_ELEICAO", "4"),
    ("CD_TIPO_ELEICAO", "1"),
    ("NM_TIPO_ELEICAO", "20"),
    ("CD_ELEICAO", "4"),
    ("DS_ELEICAO", "40"),
    ("DT_ELEICAO", "10"),
    ("DT_POSSE", "10"),
    ("SG_UF", "2"),
    ("SG_UE", "5"),
    ("NM_UE", "40"),
    ("CD_CARGO", "2"),
    ("DS_CARGO", "20"),
    ("QT_VAGA", "5"),
];

static KEY_COLUMNS: &[&str] = &["ANO_ELEICAO", "CD_ELEICAO", "SG_UE", "CD_CARGO"];

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = postgres_pool(MAX_WORKERS + 2)?;
    let _ = pool.get()?;

    run_csv_import(
        &pool,
        CsvImportParams {
            table_name: "consulta_vagas",
            columns: VAGAS_COLUMNS,
            key_columns: KEY_COLUMNS,
            unique_index_name: "idx_unique_consulta_vagas",
            data_subdir: "dados",
            dir_prefix: Some("consulta_vagas_"),
            column_renames: &[],
            max_workers: MAX_WORKERS,
            csv_separator: b';',
        },
    )?;

    Ok(())
}
