import re
import os
from datetime import datetime

class FileParser:
    # Pattern: bweb_{turno}t_{UF}_{DDMMAAAAHHMM}
    DIR_PATTERN = re.compile(r'bweb_(\d+)t_([A-Z]{2})_(\d{12})')
    
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
                'turno': int(turno),
                'uf': uf,
                'generation_date': generation_date,
                'ano': generation_date.year # Infer election year from generation date roughly, but CSV is better source
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
            metadata = FileParser.parse_directory_name(dir_name)
            
            if metadata:
                for file in files:
                    if file.endswith('.csv') and file.startswith(dir_name):
                        yield os.path.join(root, file), metadata
