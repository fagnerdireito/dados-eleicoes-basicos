//! Cria a tabela `estados` (UF + DF) e insere as 27 unidades federativas no PostgreSQL.
//! Colunas: `sigla` (2 letras, ex.: AC, SP), `NM_ESTADO` (nome completo).

use anyhow::{Context, Result};
use eleicoes_etl_postgres::common::{load_dotenv_quiet, postgres_pool};

/// (sigla UF, nome completo) — 26 estados + Distrito Federal.
const UFS: &[(&str, &str)] = &[
    ("AC", "Acre"),
    ("AL", "Alagoas"),
    ("AM", "Amazonas"),
    ("AP", "Amapá"),
    ("BA", "Bahia"),
    ("CE", "Ceará"),
    ("DF", "Distrito Federal"),
    ("ES", "Espírito Santo"),
    ("GO", "Goiás"),
    ("MA", "Maranhão"),
    ("MG", "Minas Gerais"),
    ("MS", "Mato Grosso do Sul"),
    ("MT", "Mato Grosso"),
    ("PA", "Pará"),
    ("PB", "Paraíba"),
    ("PE", "Pernambuco"),
    ("PI", "Piauí"),
    ("PR", "Paraná"),
    ("RJ", "Rio de Janeiro"),
    ("RN", "Rio Grande do Norte"),
    ("RO", "Rondônia"),
    ("RR", "Roraima"),
    ("RS", "Rio Grande do Sul"),
    ("SC", "Santa Catarina"),
    ("SE", "Sergipe"),
    ("SP", "São Paulo"),
    ("TO", "Tocantins"),
];

fn main() -> Result<()> {
    load_dotenv_quiet();

    let pool = postgres_pool(1)?;
    let mut conn = pool.get().context("conectar ao PostgreSQL")?;

    conn.execute(
        r#"CREATE TABLE IF NOT EXISTS public.estados (
    id BIGSERIAL PRIMARY KEY,
    sigla VARCHAR(2) NOT NULL,
    "NM_ESTADO" VARCHAR(100) NOT NULL,
    CONSTRAINT estados_sigla_key UNIQUE (sigla)
)"#,
        &[],
    )
    .context("criar tabela estados")?;

    let _ = conn.execute(
        r#"DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'estados'
                  AND column_name = 'SG_UF'
            ) THEN
                ALTER TABLE public.estados RENAME COLUMN "SG_UF" TO sigla;
            END IF;
        END $$"#,
        &[],
    );

    println!(r#"Tabela estados verificada/criada (coluna sigla = UF)."#);

    let insert = r#"INSERT INTO public.estados (sigla, "NM_ESTADO") VALUES ($1, $2)
        ON CONFLICT (sigla) DO UPDATE SET "NM_ESTADO" = EXCLUDED."NM_ESTADO""#;

    for (sg, nm) in UFS {
        conn.execute(insert, &[&*sg, &*nm])
            .with_context(|| format!("inserir UF {sg}"))?;
    }

    println!("{} unidades federativas inseridas/atualizadas.", UFS.len());
    Ok(())
}
