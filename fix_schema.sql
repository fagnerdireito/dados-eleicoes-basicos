SET FOREIGN_KEY_CHECKS = 0;

-- Limpar tabelas de dados que dependem de candidatos
TRUNCATE TABLE votos_secao;
TRUNCATE TABLE votos_consolidados;
TRUNCATE TABLE candidatos;

-- Adicionar índice para eleicao_id para que uk_candidato possa ser dropado
CREATE INDEX idx_candidato_eleicao ON candidatos (eleicao_id);

-- Ajustar o índice único da tabela candidatos
ALTER TABLE candidatos DROP INDEX uk_candidato;
ALTER TABLE candidatos ADD UNIQUE KEY uk_candidato (eleicao_id, municipio_id, cargo_id, nr_votavel);

-- Resetar status de processamento para permitir re-execução
UPDATE arquivos_processados SET status = 'ERROR';

SET FOREIGN_KEY_CHECKS = 1;
