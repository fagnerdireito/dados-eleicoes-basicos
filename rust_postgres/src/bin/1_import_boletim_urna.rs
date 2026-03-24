use anyhow::Result;
use eleicoes_etl_postgres::common::{load_dotenv_quiet, postgres_pool, run_csv_import, CsvImportParams};

const MAX_WORKERS: usize = 1;

static COLUMN_MAPPING: &[(&str, &str)] = &[
    ("C_ELEICAO", "CD_ELEICAO"),
    ("DS_AGREGADAS", "DS_SECOES_AGREGADAS"),
    ("QT_ELEITORES_BIOMETRIA_NH", "QT_ELEI_BIOM_SEM_HABILITACAO"),
];

static BU_COLUMNS: &[(&str, &str)] = &[
    ("DT_GERACAO", "10"),
    ("HH_GERACAO", "8"),
    ("ANO_ELEICAO", "4"),
    ("CD_TIPO_ELEICAO", "1"),
    ("NM_TIPO_ELEICAO", "17"),
    ("CD_PLEITO", "3"),
    ("DT_PLEITO", "19"),
    ("NR_TURNO", "1"),
    ("CD_ELEICAO", "3"),
    ("DS_ELEICAO", "30"),
    ("SG_UF", "2"),
    ("CD_MUNICIPIO", "5"),
    ("NM_MUNICIPIO", "22"),
    ("NR_ZONA", "3"),
    ("NR_SECAO", "3"),
    ("NR_LOCAL_VOTACAO", "4"),
    ("CD_CARGO_PERGUNTA", "2"),
    ("DS_CARGO_PERGUNTA", "17"),
    ("NR_PARTIDO", "2"),
    ("SG_PARTIDO", "13"),
    ("NM_PARTIDO", "46"),
    ("DT_BU_RECEBIDO", "19"),
    ("QT_APTOS", "3"),
    ("QT_COMPARECIMENTO", "3"),
    ("QT_ABSTENCOES", "3"),
    ("CD_TIPO_URNA", "1"),
    ("DS_TIPO_URNA", "7"),
    ("CD_TIPO_VOTAVEL", "1"),
    ("DS_TIPO_VOTAVEL", "7"),
    ("NR_VOTAVEL", "5"),
    ("NM_VOTAVEL", "28"),
    ("QT_VOTOS", "3"),
    ("NR_URNA_EFETIVADA", "7"),
    ("CD_CARGA_1_URNA_EFETIVADA", "24"),
    ("CD_CARGA_2_URNA_EFETIVADA", "7"),
    ("CD_FLASHCARD_URNA_EFETIVADA", "8"),
    ("DT_CARGA_URNA_EFETIVADA", "19"),
    ("DS_CARGO_PERGUNTA_SECAO", "8"),
    ("DS_SECOES_AGREGADAS", "15"),
    ("DT_ABERTURA", "19"),
    ("DT_ENCERRAMENTO", "19"),
    ("QT_ELEI_BIOM_SEM_HABILITACAO", "2"),
    ("DT_EMISSAO_BU", "19"),
    ("NR_JUNTA_APURADORA", "2"),
    ("NR_TURMA_APURADORA", "2"),
];

static KEY_COLUMNS: &[&str] = &[
    "CD_PLEITO",
    "CD_MUNICIPIO",
    "NR_ZONA",
    "NR_SECAO",
    "CD_CARGO_PERGUNTA",
    "NR_VOTAVEL",
];

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = postgres_pool(MAX_WORKERS + 2)?;
    let _ = pool.get()?; // ping implícito ao obter conexão

    run_csv_import(
        &pool,
        CsvImportParams {
            table_name: "boletim_de_urna",
            columns: BU_COLUMNS,
            key_columns: KEY_COLUMNS,
            unique_index_name: "idx_unique_bu",
            data_subdir: "bweb",
            dir_prefix: None,
            column_renames: COLUMN_MAPPING,
            max_workers: MAX_WORKERS,
            csv_separator: b';',
        },
    )?;

    Ok(())
}
