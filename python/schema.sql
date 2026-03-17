SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS votos_secao;
DROP TABLE IF EXISTS votos_consolidados;
DROP TABLE IF EXISTS vagas;
DROP TABLE IF EXISTS candidatos_detalhes;
DROP TABLE IF EXISTS candidatos;
DROP TABLE IF EXISTS secoes;
DROP TABLE IF EXISTS zonas;
DROP TABLE IF EXISTS municipios;
DROP TABLE IF EXISTS estados;
DROP TABLE IF EXISTS partidos;
DROP TABLE IF EXISTS cargos;
DROP TABLE IF EXISTS eleicoes;
DROP TABLE IF EXISTS arquivos_processados;

SET FOREIGN_KEY_CHECKS = 1;

-- Tabela de Eleições (Dimensão Temporal)
CREATE TABLE eleicoes (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    ano INT NOT NULL,
    turno INT NOT NULL,
    tipo_eleicao INT, -- 1=Geral, 2=Municipal (simplificado)
    dt_pleito DATE,
    ds_eleicao VARCHAR(255),
    cd_eleicao VARCHAR(10),
    UNIQUE KEY uk_eleicao (ano, turno, cd_eleicao)
);

-- Tabela de Estados (Dimensão Geográfica)
CREATE TABLE estados (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sigla CHAR(2) NOT NULL UNIQUE,
    nome VARCHAR(100)
);

-- Tabela de Municípios (Dimensão Geográfica)
CREATE TABLE municipios (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    estado_id BIGINT UNSIGNED NOT NULL,
    codigo_tse VARCHAR(10) NOT NULL,
    nome VARCHAR(255) NOT NULL,
    UNIQUE KEY uk_municipio_tse (estado_id, codigo_tse),
    FOREIGN KEY (estado_id) REFERENCES estados(id)
);

-- Tabela de Zonas (Dimensão Geográfica)
CREATE TABLE zonas (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    municipio_id BIGINT UNSIGNED NOT NULL,
    nr_zona VARCHAR(10) NOT NULL,
    UNIQUE KEY uk_zona (municipio_id, nr_zona),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id)
);

-- Tabela de Seções (Dimensão Geográfica)
CREATE TABLE secoes (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    zona_id BIGINT UNSIGNED NOT NULL,
    nr_secao VARCHAR(10) NOT NULL,
    nr_local_votacao VARCHAR(20),
    UNIQUE KEY uk_secao (zona_id, nr_secao),
    FOREIGN KEY (zona_id) REFERENCES zonas(id)
);

-- Tabela de Cargos (Dimensão Política)
CREATE TABLE cargos (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    codigo VARCHAR(10) NOT NULL UNIQUE,
    descricao VARCHAR(100) NOT NULL
);

-- Tabela de Partidos (Dimensão Política)
CREATE TABLE partidos (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    numero VARCHAR(5) NOT NULL UNIQUE,
    sigla VARCHAR(20) NOT NULL,
    nome VARCHAR(255)
);

-- Tabela de Candidatos (Dimensão Política)
CREATE TABLE candidatos (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT UNSIGNED NOT NULL,
    municipio_id BIGINT UNSIGNED, -- Adicionado para diferenciar candidatos a Prefeito entre municípios
    cargo_id BIGINT UNSIGNED NOT NULL,
    partido_id BIGINT UNSIGNED, -- Pode ser nulo para votos brancos/nulos
    nr_votavel VARCHAR(10) NOT NULL,
    nome VARCHAR(255),
    UNIQUE KEY uk_candidato (eleicao_id, municipio_id, cargo_id, nr_votavel),
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (partido_id) REFERENCES partidos(id)
);

-- Tabela de Controle de Arquivos
CREATE TABLE arquivos_processados (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    path VARCHAR(500) NOT NULL UNIQUE,
    hash_md5 VARCHAR(32),
    dt_processamento DATETIME DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20), -- 'PROCESSADO', 'ERRO'
    linhas INT
);

-- Fato Principal: Votos por Seção
CREATE TABLE votos_secao (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT UNSIGNED NOT NULL,
    secao_id BIGINT UNSIGNED NOT NULL,
    cargo_id BIGINT UNSIGNED NOT NULL,
    candidato_id BIGINT UNSIGNED NOT NULL,
    qt_votos INT DEFAULT 0,
    qt_aptos INT DEFAULT 0,
    qt_comparecimento INT DEFAULT 0,
    qt_abstencoes INT DEFAULT 0,
    arquivo_origem_id BIGINT UNSIGNED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_votos_secao (eleicao_id, secao_id, cargo_id, candidato_id),
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (secao_id) REFERENCES secoes(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (candidato_id) REFERENCES candidatos(id),
    FOREIGN KEY (arquivo_origem_id) REFERENCES arquivos_processados(id)
);

-- Tabela Agregada: Votos Consolidados (para o sistema web)
CREATE TABLE votos_consolidados (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT UNSIGNED NOT NULL,
    municipio_id BIGINT UNSIGNED NOT NULL,
    cargo_id BIGINT UNSIGNED NOT NULL,
    candidato_id BIGINT UNSIGNED NOT NULL,
    total_votos INT DEFAULT 0,
    total_validos INT DEFAULT 0, -- Se for candidato nominal
    total_brancos INT DEFAULT 0, -- Se for voto branco
    total_nulos INT DEFAULT 0,   -- Se for voto nulo
    percentual DECIMAL(5,2),     -- Opcional, calculado na hora ou pré-calculado
    UNIQUE KEY uk_votos_consolidados (eleicao_id, municipio_id, cargo_id, candidato_id),
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (candidato_id) REFERENCES candidatos(id)
);

-- Tabela de Vagas (Quantidade de vagas por cargo e município)
CREATE TABLE vagas (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT UNSIGNED NOT NULL,
    municipio_id BIGINT UNSIGNED NOT NULL,
    cargo_id BIGINT UNSIGNED NOT NULL,
    quantidade INT NOT NULL,
    UNIQUE KEY uk_vaga (eleicao_id, municipio_id, cargo_id),
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id)
);

-- Tabela Detalhada de Candidatos (Consulta Candidatos)
CREATE TABLE candidatos_detalhes (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT UNSIGNED NOT NULL,
    municipio_id BIGINT UNSIGNED,
    cargo_id BIGINT UNSIGNED NOT NULL,
    partido_id BIGINT UNSIGNED,
    sq_candidato VARCHAR(20) NOT NULL,
    nr_candidato VARCHAR(10) NOT NULL,
    nm_candidato VARCHAR(255),
    nm_urna_candidato VARCHAR(255),
    nm_social_candidato VARCHAR(255),
    nr_cpf_candidato VARCHAR(20),
    ds_email VARCHAR(255),
    cd_situacao_candidatura INT,
    ds_situacao_candidatura VARCHAR(100),
    tp_agremiacao VARCHAR(100),
    nr_partido VARCHAR(10),
    sg_partido VARCHAR(20),
    nm_partido VARCHAR(255),
    nr_federacao VARCHAR(20),
    nm_federacao VARCHAR(255),
    sg_federacao VARCHAR(20),
    ds_composicao_federacao TEXT,
    sq_coligacao VARCHAR(20),
    nm_coligacao VARCHAR(255),
    ds_composicao_coligacao TEXT,
    sg_uf_nascimento CHAR(2),
    dt_nascimento DATE,
    nr_titulo_eleitoral_candidato VARCHAR(20),
    cd_genero INT,
    ds_genero VARCHAR(50),
    cd_grau_instrucao INT,
    ds_grau_instrucao VARCHAR(100),
    cd_estado_civil INT,
    ds_estado_civil VARCHAR(100),
    cd_cor_raca INT,
    ds_cor_raca VARCHAR(50),
    cd_ocupacao INT,
    ds_ocupacao VARCHAR(255),
    cd_sit_tot_turno INT,
    ds_sit_tot_turno VARCHAR(100),
    UNIQUE KEY uk_sq_candidato (sq_candidato, eleicao_id),
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (partido_id) REFERENCES partidos(id)
);

-- Índices para performance
CREATE INDEX idx_votos_secao_agregacao ON votos_secao (eleicao_id, cargo_id, secao_id);
CREATE INDEX idx_votos_consolidados_busca ON votos_consolidados (eleicao_id, municipio_id, cargo_id);
CREATE INDEX idx_candidato_eleicao ON candidatos (eleicao_id);
