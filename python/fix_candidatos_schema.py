"""
Script para corrigir o schema da tabela candidatos e reprocessar os dados.
Problema: NR_VOTAVEL para Prefeito é o número do partido (22, 44, 13...)
e se repete em todos os municípios. Sem municipio_id na chave única,
apenas o primeiro município processado ficava registrado.
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'election_data_pipeline'))

from sqlalchemy import create_engine, text
from election_data_pipeline.config.settings import settings

engine = create_engine(settings.DATABASE_URL)

steps = [
    ("Removendo FK votos_consolidados -> candidatos",
     "ALTER TABLE votos_consolidados DROP FOREIGN KEY votos_consolidados_ibfk_4"),
    ("Removendo FK votos_secao -> candidatos",
     "ALTER TABLE votos_secao DROP FOREIGN KEY votos_secao_ibfk_4"),
    ("Removendo index uk_candidato antigo",
     "ALTER TABLE candidatos DROP INDEX uk_candidato"),
    ("Adicionando novo uk_candidato com municipio_id",
     "ALTER TABLE candidatos ADD UNIQUE KEY uk_candidato (eleicao_id, municipio_id, cargo_id, nr_votavel)"),
    ("Adicionando FK candidatos -> municipios",
     "ALTER TABLE candidatos ADD FOREIGN KEY fk_candidato_municipio (municipio_id) REFERENCES municipios(id)"),
    ("Recriando FK votos_consolidados -> candidatos",
     "ALTER TABLE votos_consolidados ADD FOREIGN KEY (candidato_id) REFERENCES candidatos(id)"),
    ("Recriando FK votos_secao -> candidatos",
     "ALTER TABLE votos_secao ADD FOREIGN KEY (candidato_id) REFERENCES candidatos(id)"),
    ("Limpando votos_secao",
     "DELETE FROM votos_secao"),
    ("Limpando votos_consolidados",
     "DELETE FROM votos_consolidados"),
    ("Limpando candidatos (dados com nomes errados)",
     "DELETE FROM candidatos"),
    ("Marcando arquivo GO para reprocessamento",
     "UPDATE arquivos_processados SET status = 'ERROR' WHERE path LIKE '%bweb_1t_GO%'"),
]

with engine.connect() as conn:
    for desc, sql in steps:
        try:
            print(f"  >> {desc}...")
            conn.execute(text(sql))
            conn.commit()
            print(f"     OK")
        except Exception as e:
            print(f"     AVISO: {e}")

print("\nSchema corrigido. Iniciando pipeline...\n")

# Rodar o pipeline
from election_data_pipeline.src.utils.file_parser import FileParser
from election_data_pipeline.src.extractors.csv_extractor import CSVExtractor
from election_data_pipeline.src.loaders.mysql_loader import MySQLLoader
from election_data_pipeline.src.transformers.normalizer import Transformer
from election_data_pipeline.src.transformers.cleaner import Cleaner
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = '/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/bweb'

loader = MySQLLoader()
transformer = Transformer(loader)

files = list(FileParser.find_csv_files(DATA_DIR))
logger.info(f"Encontrados {len(files)} arquivos CSV.")

for file_path, metadata in files:
    logger.info(f"Processando {file_path} (Turno: {metadata['turno']}, UF: {metadata['uf']})")

    file_id, already_processed = loader.register_file(file_path)
    if already_processed:
        logger.info(f"Pulando arquivo já processado: {file_path}")
        continue

    total_lines = 0
    extractor = CSVExtractor(file_path)

    try:
        for chunk in tqdm(extractor.extract_chunks(), desc="Chunks"):
            chunk = Cleaner.clean_chunk(chunk)
            transformer.process_chunk(chunk, metadata)
            total_lines += len(chunk)

        loader.update_file_status(file_id, 'PROCESSED', total_lines)
        logger.info(f"Concluído: {file_path} ({total_lines} linhas)")

    except Exception as e:
        loader.update_file_status(file_id, 'ERROR', total_lines)
        logger.error(f"Erro em {file_path}: {e}", exc_info=True)
