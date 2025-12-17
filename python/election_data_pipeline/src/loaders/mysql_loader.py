from sqlalchemy import create_engine, text
from config.settings import settings
import logging

logger = logging.getLogger(__name__)

class MySQLLoader:
    def __init__(self):
        self.engine = create_engine(settings.DATABASE_URL)

    def execute_query(self, query, params=None):
        with self.engine.connect() as conn:
            # If query is string, wrap in text(), else use as is
            if isinstance(query, str):
                stmt = text(query)
            else:
                stmt = query
                
            result = conn.execute(stmt, params or {})
            conn.commit()
            return result

    def load_df(self, df, table_name, if_exists='append', chunksize=1000):
        try:
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists, 
                index=False, 
                chunksize=chunksize,
                method='multi' # More efficient for MySQL
            )
            logger.info(f"Loaded {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            raise
