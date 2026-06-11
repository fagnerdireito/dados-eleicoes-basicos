-- NAO EXECUTADO PELO APP.
-- Sugestoes para revisao manual do administrador do PostgreSQL.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bu_dashboard_scope
ON boletim_de_urna (
    "ANO_ELEICAO", "CD_ELEICAO", "NR_TURNO", "SG_UF",
    "CD_MUNICIPIO", "CD_CARGO_PERGUNTA", "NR_VOTAVEL"
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bu_dashboard_location
ON boletim_de_urna (
    "ANO_ELEICAO", "CD_ELEICAO", "NR_TURNO", "SG_UF",
    "CD_MUNICIPIO", "CD_CARGO_PERGUNTA", "NR_LOCAL_VOTACAO"
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_local_votacao_join
ON local_votacao (
    "AA_ELEICAO", "NR_TURNO", "SG_UF", "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO"
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_perfil_eleitorado_scope
ON perfil_eleitorado ("ANO_ELEICAO", "SG_UF");
