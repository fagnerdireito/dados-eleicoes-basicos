import sys
import os
import argparse
import logging
from tqdm import tqdm

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.settings import settings
from src.utils.file_parser import FileParser
from src.extractors.csv_extractor import CSVExtractor
from src.loaders.mysql_loader import MySQLLoader
from src.transformers.normalizer import Transformer
from src.transformers.cleaner import Cleaner
from src.transformers.aggregator import Aggregator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Election Data ETL Pipeline')
    parser.add_argument('--data_dir', default=settings.DATA_DIR, help='Directory containing election data')
    args = parser.parse_args()

    logger.info(f"Starting ETL pipeline. Data directory: {args.data_dir}")
    
    loader = MySQLLoader()
    transformer = Transformer(loader)
    
    # 1. Discovery
    files = list(FileParser.find_csv_files(args.data_dir))
    logger.info(f"Found {len(files)} CSV files to process.")
    
    for file_path, metadata in files:
        logger.info(f"Processing {file_path} (Turno: {metadata['turno']}, UF: {metadata['uf']})")
        
        file_id = loader.register_file(file_path)
        total_lines = 0
        
        extractor = CSVExtractor(file_path)
        
        try:
            for chunk in tqdm(extractor.extract_chunks(), desc="Processing Chunks"):
                # Clean
                chunk = Cleaner.clean_chunk(chunk)
                
                # Transform and Load (Normalized + Consolidated)
                transformer.process_chunk(chunk, metadata)
                
                total_lines += len(chunk)
                
            loader.update_file_status(file_id, 'PROCESSED', total_lines)
            logger.info(f"Successfully processed {file_path}")
            
        except Exception as e:
            loader.update_file_status(file_id, 'ERROR', total_lines)
            logger.error(f"Failed to process {file_path}: {e}")

if __name__ == '__main__':
    main()
