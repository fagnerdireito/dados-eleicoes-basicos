import pandas as pd
import logging
from config.settings import settings

logger = logging.getLogger(__name__)

class CSVExtractor:
    def __init__(self, file_path, encoding=settings.ENCODING, separator=settings.CSV_SEPARATOR):
        self.file_path = file_path
        self.encoding = encoding
        self.separator = separator

    def extract_chunks(self, chunk_size=settings.CHUNK_SIZE):
        """
        Yields chunks of the CSV file as DataFrames.
        """
        try:
            # First, peek at the columns to ensure validity or validation if needed
            # For now, we trust the schema but handle encoding issues
            
            # Columns to ensure are read as strings to preserve leading zeros
            dtype_options = {
                'NR_ZONA': str,
                'NR_SECAO': str,
                'NR_LOCAL_VOTACAO': str,
                'CD_MUNICIPIO': str,
                'NR_PARTIDO': str,
                'NR_VOTAVEL': str,
                'CD_CARGO_PERGUNTA': str,
                'CD_ELEICAO': str
            }

            for chunk in pd.read_csv(
                self.file_path, 
                sep=self.separator, 
                encoding=self.encoding, 
                chunksize=chunk_size,
                dtype=dtype_options,
                on_bad_lines='warn'
            ):
                yield chunk
        except Exception as e:
            logger.error(f"Error reading file {self.file_path}: {e}")
            raise
