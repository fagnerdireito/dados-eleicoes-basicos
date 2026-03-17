SET FOREIGN_KEY_CHECKS = 0;

-- Limpar tabelas de dados que podem ter duplicatas
TRUNCATE TABLE votos_secao;
TRUNCATE TABLE votos_consolidados;

-- Adicionar índices únicos para permitir idempotência (INSERT IGNORE ou ON DUPLICATE KEY UPDATE)
ALTER TABLE votos_secao ADD UNIQUE KEY uk_votos_secao (eleicao_id, secao_id, cargo_id, candidato_id);
ALTER TABLE votos_consolidados ADD UNIQUE KEY uk_votos_consolidados (eleicao_id, municipio_id, cargo_id, candidato_id);

-- Garantir que a tabela de candidatos também seja limpa para refletir o novo schema sem erros residuais
TRUNCATE TABLE candidatos;

-- Resetar status de processamento
UPDATE arquivos_processados SET status = 'ERROR';

SET FOREIGN_KEY_CHECKS = 1;
