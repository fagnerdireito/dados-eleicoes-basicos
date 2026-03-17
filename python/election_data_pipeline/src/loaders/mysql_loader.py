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

    def register_file(self, path):
        """
        Registra o arquivo na tabela de controle.
        Retorna uma tupla (file_id, already_processed).
        already_processed=True indica que o arquivo já foi processado com sucesso e deve ser pulado.
        """
        try:
            with self.engine.connect() as conn:
                # Check if exists
                res = conn.execute(text("SELECT id, status FROM arquivos_processados WHERE path=:path"), {'path': path}).first()
                if res:
                    file_id, status = res[0], res[1]
                    if status == 'PROCESSED':
                        return file_id, True
                    # ERROR or PROCESSING: allow retry
                    conn.execute(text("UPDATE arquivos_processados SET status='PROCESSING', linhas=0 WHERE id=:id"), {'id': file_id})
                    conn.commit()
                    return file_id, False

                # Insert new record
                conn.execute(text("INSERT INTO arquivos_processados (path, status, linhas) VALUES (:path, 'PROCESSING', 0)"),
                             {'path': path})
                conn.commit()

                res = conn.execute(text("SELECT id FROM arquivos_processados WHERE path=:path"), {'path': path}).first()
                return res[0], False
        except Exception as e:
            logger.error(f"Error registering file {path}: {e}")
            return None, False

    def update_file_status(self, file_id, status, lines=0):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("UPDATE arquivos_processados SET status=:status, linhas=:lines WHERE id=:id"), 
                           {'status': status, 'lines': lines, 'id': file_id})
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating file {file_id}: {e}")

    def load_df(self, df, table_name, if_exists='append', chunksize=1000):
        def mysql_on_duplicate_key_update(table, conn, keys, data_iter):
            from sqlalchemy.dialects.mysql import insert
            from sqlalchemy import table as sql_table, column as sql_column

            # Create a list of columns to be updated
            update_cols = {c.name: c for c in table.table.columns if c.name not in table.table.primary_key}

            # Prepare the data
            data = [dict(zip(keys, row)) for row in data_iter]

            # Construct the insert statement
            stmt = insert(table.table).values(data)
            
            # Add ON DUPLICATE KEY UPDATE clause
            on_duplicate_key_stmt = stmt.on_duplicate_key_update(
                {c.name: c for c in stmt.inserted if c.name in update_cols}
            )

            conn.execute(on_duplicate_key_stmt)

        try:
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists, 
                index=False, 
                chunksize=chunksize,
                method=mysql_on_duplicate_key_update
            )
            logger.info(f"Loaded {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            raise
