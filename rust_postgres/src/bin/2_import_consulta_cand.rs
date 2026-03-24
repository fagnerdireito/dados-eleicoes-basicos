use anyhow::Result;
use eleicoes_etl_postgres::common::{load_dotenv_quiet, postgres_pool, run_csv_import, CsvImportParams};

const MAX_WORKERS: usize = 2;

static CAND_COLUMNS: &[(&str, &str)] = &[
    ("DT_GERACAO", "10"),
    ("HH_GERACAO", "8"),
    ("ANO_ELEICAO", "4"),
    ("CD_TIPO_ELEICAO", "1"),
    ("NM_TIPO_ELEICAO", "20"),
    ("NR_TURNO", "1"),
    ("CD_ELEICAO", "4"),
    ("DS_ELEICAO", "40"),
    ("DT_ELEICAO", "10"),
    ("TP_ABRANGENCIA", "10"),
    ("SG_UF", "2"),
    ("SG_UE", "5"),
    ("NM_UE", "40"),
    ("CD_CARGO", "2"),
    ("DS_CARGO", "20"),
    ("SQ_CANDIDATO", "15"),
    ("NR_CANDIDATO", "5"),
    ("NM_CANDIDATO", "70"),
    ("NM_URNA_CANDIDATO", "40"),
    ("NM_SOCIAL_CANDIDATO", "40"),
    ("NR_CPF_CANDIDATO", "15"),
    ("DS_EMAIL", "100"),
    ("CD_SITUACAO_CANDIDATURA", "2"),
    ("DS_SITUACAO_CANDIDATURA", "20"),
    ("TP_AGREMIACAO", "20"),
    ("NR_PARTIDO", "5"),
    ("SG_PARTIDO", "15"),
    ("NM_PARTIDO", "50"),
    ("NR_FEDERACAO", "5"),
    ("NM_FEDERACAO", "50"),
    ("SG_FEDERACAO", "20"),
    ("DS_COMPOSICAO_FEDERACAO", "20"),
    ("SQ_COLIGACAO", "15"),
    ("NM_COLIGACAO", "100"),
    ("DS_COMPOSICAO_COLIGACAO", "255"),
    ("SG_UF_NASCIMENTO", "15"),
    ("DT_NASCIMENTO", "10"),
    ("NR_TITULO_ELEITORAL_CANDIDATO", "15"),
    ("CD_GENERO", "2"),
    ("DS_GENERO", "15"),
    ("CD_GRAU_INSTRUCAO", "2"),
    ("DS_GRAU_INSTRUCAO", "30"),
    ("CD_ESTADO_CIVIL", "2"),
    ("DS_ESTADO_CIVIL", "30"),
    ("CD_COR_RACA", "2"),
    ("DS_COR_RACA", "15"),
    ("CD_OCUPACAO", "5"),
    ("DS_OCUPACAO", "80"),
    ("CD_SIT_TOT_TURNO", "2"),
    ("DS_SIT_TOT_TURNO", "20"),
];

static KEY_COLUMNS: &[&str] = &["ANO_ELEICAO", "SQ_CANDIDATO"];

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = postgres_pool(MAX_WORKERS + 2)?;
    let _ = pool.get()?;

    run_csv_import(
        &pool,
        CsvImportParams {
            table_name: "consulta_cand",
            columns: CAND_COLUMNS,
            key_columns: KEY_COLUMNS,
            unique_index_name: "idx_unique_consulta_cand",
            data_subdir: "dados",
            dir_prefix: Some("consulta_cand_"),
            column_renames: &[],
            max_workers: MAX_WORKERS,
            csv_separator: b';',
        },
    )?;

    Ok(())
}
