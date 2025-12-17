import os
from dotenv import load_dotenv

# Try loading .env from python directory specifically if not found
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
load_dotenv(env_path)

class Settings:
    # Database
    DB_CONNECTION = os.getenv('DB_CONNECTION', 'mysql')
    DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
    DB_PORT = os.getenv('DB_PORT', '3306')
    DB_DATABASE = os.getenv('DB_DATABASE', 'eleicoes')
    DB_USERNAME = os.getenv('DB_USERNAME', 'root')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')

    # Load .env explicitly if needed or rely on python-dotenv loaded at top
    # The load_dotenv() at top might look for .env in CWD, let's ensure it looks in python folder if running from root
    
    # Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.getenv('DATA_DIR', os.path.join(os.path.dirname(BASE_DIR), '..')) # Default to parent of python folder (root)
    
    # ETL
    CHUNK_SIZE = 50000
    ENCODING = 'latin1'
    CSV_SEPARATOR = ';'

    @property
    def DATABASE_URL(self):
        return f"{self.DB_CONNECTION}+mysqlconnector://{self.DB_USERNAME}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DATABASE}"

settings = Settings()
