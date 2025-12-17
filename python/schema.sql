-- Tabela de Eleições (Dimensão Temporal)
CREATE TABLE IF NOT EXISTS eleicoes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ano INT NOT NULL,
    turno INT NOT NULL,
    tipo_eleicao INT, -- 1=Geral, 2=Municipal (simplificado)
    dt_pleito DATE,
    ds_eleicao VARCHAR(255),
    cd_eleicao VARCHAR(10),
    UNIQUE KEY uk_eleicao (ano, turno, cd_eleicao)
);

-- Tabela de Estados (Dimensão Geográfica)
CREATE TABLE IF NOT EXISTS estados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sigla CHAR(2) NOT NULL UNIQUE,
    nome VARCHAR(100)
);

-- Tabela de Municípios (Dimensão Geográfica)
CREATE TABLE IF NOT EXISTS municipios (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    estado_id INT NOT NULL,
    codigo_tse VARCHAR(10) NOT NULL,
    nome VARCHAR(255) NOT NULL,
    UNIQUE KEY uk_municipio_tse (estado_id, codigo_tse),
    FOREIGN KEY (estado_id) REFERENCES estados(id)
);

-- Tabela de Zonas (Dimensão Geográfica)
CREATE TABLE IF NOT EXISTS zonas (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    municipio_id BIGINT NOT NULL,
    nr_zona VARCHAR(10) NOT NULL,
    UNIQUE KEY uk_zona (municipio_id, nr_zona),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id)
);

-- Tabela de Seções (Dimensão Geográfica)
CREATE TABLE IF NOT EXISTS secoes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    zona_id BIGINT NOT NULL,
    nr_secao VARCHAR(10) NOT NULL,
    nr_local_votacao VARCHAR(20),
    UNIQUE KEY uk_secao (zona_id, nr_secao),
    FOREIGN KEY (zona_id) REFERENCES zonas(id)
);

-- Tabela de Cargos (Dimensão Política)
CREATE TABLE IF NOT EXISTS cargos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    codigo VARCHAR(10) NOT NULL UNIQUE,
    descricao VARCHAR(100) NOT NULL
);

-- Tabela de Partidos (Dimensão Política)
CREATE TABLE IF NOT EXISTS partidos (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    numero VARCHAR(5) NOT NULL UNIQUE,
    sigla VARCHAR(20) NOT NULL,
    nome VARCHAR(255)
);

-- Tabela de Candidatos (Dimensão Política)
CREATE TABLE IF NOT EXISTS candidatos (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT NOT NULL,
    cargo_id INT NOT NULL,
    partido_id BIGINT, -- Pode ser nulo para votos brancos/nulos
    nr_votavel VARCHAR(10) NOT NULL,
    nome VARCHAR(255),
    UNIQUE KEY uk_candidato (eleicao_id, cargo_id, nr_votavel),
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (partido_id) REFERENCES partidos(id)
);

-- Tabela de Controle de Arquivos
CREATE TABLE IF NOT EXISTS arquivos_processados (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    path VARCHAR(500) NOT NULL UNIQUE,
    hash_md5 VARCHAR(32),
    dt_processamento DATETIME DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20), -- 'PROCESSADO', 'ERRO'
    linhas INT
);

-- Fato Principal: Votos por Seção
CREATE TABLE IF NOT EXISTS votos_secao (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT NOT NULL,
    secao_id BIGINT NOT NULL,
    cargo_id INT NOT NULL,
    candidato_id BIGINT NOT NULL,
    qt_votos INT DEFAULT 0,
    qt_aptos INT DEFAULT 0,
    qt_comparecimento INT DEFAULT 0,
    qt_abstencoes INT DEFAULT 0,
    arquivo_origem_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (secao_id) REFERENCES secoes(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (candidato_id) REFERENCES candidatos(id),
    FOREIGN KEY (arquivo_origem_id) REFERENCES arquivos_processados(id)
);

-- Tabela Agregada: Votos Consolidados (para o sistema web)
CREATE TABLE IF NOT EXISTS votos_consolidados (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    eleicao_id BIGINT NOT NULL,
    municipio_id BIGINT NOT NULL,
    cargo_id INT NOT NULL,
    candidato_id BIGINT NOT NULL,
    total_votos INT DEFAULT 0,
    total_validos INT DEFAULT 0, -- Se for candidato nominal
    total_brancos INT DEFAULT 0, -- Se for voto branco
    total_nulos INT DEFAULT 0,   -- Se for voto nulo
    percentual DECIMAL(5,2),     -- Opcional, calculado na hora ou pré-calculado
    FOREIGN KEY (eleicao_id) REFERENCES eleicoes(id),
    FOREIGN KEY (municipio_id) REFERENCES municipios(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos(id),
    FOREIGN KEY (candidato_id) REFERENCES candidatos(id)
);

-- Índices para performance
CREATE INDEX idx_votos_secao_agregacao ON votos_secao (eleicao_id, cargo_id, secao_id);
CREATE INDEX idx_votos_consolidados_busca ON votos_consolidados (eleicao_id, municipio_id, cargo_id);
