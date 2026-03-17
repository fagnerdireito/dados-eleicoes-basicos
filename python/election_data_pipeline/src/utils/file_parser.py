import re
import os
from datetime import datetime

class FileParser:
    # Pattern: bweb_{turno}t_{UF}_{DDMMAAAAHHMM}
    DIR_PATTERN = re.compile(r'bweb_(\d+)t_([A-Z]{2})_(\d{12})')
    # Pattern: consulta_vagas_{ANO}_{UF}.csv
    VAGAS_PATTERN = re.compile(r'consulta_vagas_(\d{4})_([A-Z]{2})\.csv')
    # Pattern: consulta_cand_{ANO}_{UF}.csv
    CANDIDATOS_PATTERN = re.compile(r'consulta_cand_(\d{4})_([A-Z]{2}|BRASIL)\.csv')
    
    @staticmethod
    def parse_directory_name(dirname):
        """
        Parses the directory name to extract metadata.
        Returns a dict with turno, uf, and generation_date, or None if no match.
        """
        match = FileParser.DIR_PATTERN.match(dirname)
        if match:
            turno, uf, date_str = match.groups()
            generation_date = datetime.strptime(date_str, '%d%m%Y%H%M')
            return {
                'type': 'bweb',
                'turno': int(turno),
                'uf': uf,
                'generation_date': generation_date,
                'ano': generation_date.year
            }
        return None

    @staticmethod
    def parse_vagas_filename(filename):
        """
        Parses the vacancy filename to extract metadata.
        """
        match = FileParser.VAGAS_PATTERN.match(filename)
        if match:
            ano, uf = match.groups()
            return {
                'type': 'vagas',
                'ano': int(ano),
                'uf': uf,
                'turno': 1 # Vagas are generic for the election, usually associated with 1st turn info
            }
        return None

    @staticmethod
    def parse_candidatos_filename(filename):
        """
        Parses the candidate filename to extract metadata.
        """
        match = FileParser.CANDIDATOS_PATTERN.match(filename)
        if match:
            ano, uf = match.groups()
            return {
                'type': 'candidatos',
                'ano': int(ano),
                'uf': uf,
                'turno': 1 # Candidates are usually global for the election
            }
        return None

    @staticmethod
    def find_csv_files(base_path):
        """
        Walks through the base path and finds valid election CSV files.
        Yields (file_path, metadata)
        """
        for root, dirs, files in os.walk(base_path):
            dir_name = os.path.basename(root)
            
            # Check for BWEB directories
            metadata = FileParser.parse_directory_name(dir_name)
            if metadata:
                for file in files:
                    if file.endswith('.csv') and file.startswith(dir_name):
                        yield os.path.join(root, file), metadata
                continue

            # Check for Vagas or Candidatos files in ANY directory
            for file in files:
                vagas_metadata = FileParser.parse_vagas_filename(file)
                if vagas_metadata:
                    yield os.path.join(root, file), vagas_metadata
                    continue
                
                cand_metadata = FileParser.parse_candidatos_filename(file)
                if cand_metadata:
                    yield os.path.join(root, file), cand_metadata
