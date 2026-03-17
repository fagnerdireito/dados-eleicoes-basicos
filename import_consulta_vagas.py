"""
Importa e unifica todos os CSVs de consulta_vagas_{ano} em uma única tabela MySQL.
Padrão: dados/consulta_vagas_*/consulta_vagas_*.csv
Encoding dos CSV: latin1.
"""
import os
import glob
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, text, types
from dotenv import load_dotenv

load_dotenv()

# Configuração do Banco de Dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '3306'),
    'database': os.getenv('DB_DATABASE', 'eleicoes'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Colunas que definem a unicidade do registro (evita duplicidade)
# Para vagas, a combinação de Eleição, Unidade Eleitoral (Cidade/Estado) e Cargo define o registro.
KEY_COLUMNS = ['ANO_ELEICAO', 'CD_ELEICAO', 'SG_UE', 'CD_CARGO']

TABLE_NAME = 'consulta_vagas'

# Diretório base dos dados
DADOS_BASE = os.path.join(os.path.dirname(__file__), 'dados')

# Multithread: máx. de threads
MAX_WORKERS = 4
CHUNKSIZE_READ = 50_000
CHUNKSIZE_INSERT = 1_000
MAX_INSERT_RETRIES = 10

_print_lock = threading.Lock()

def get_engine(pool_size=None):
    """Retorna o engine do SQLAlchemy usando mysql-connector-python."""
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    kwargs = {}
    if pool_size is not None:
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max(0, pool_size - 1)
    return create_engine(conn_str, **kwargs)

def clean_column_names(df):
    """Remove aspas duplas dos nomes das colunas e normaliza para maiúsculas."""
    df.columns = [str(c).strip('"').strip().upper() for c in df.columns]
    return df

def insert_ignore(table, conn, keys, data_iter):
    """Método customizado para o pandas.to_sql realizar INSERT IGNORE no MySQL."""
    from sqlalchemy.dialects.mysql import insert

    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    stmt = insert(table.table).values(data)
    stmt = stmt.prefix_with('IGNORE')
    conn.execute(stmt)

def setup_table_and_indexes(engine, df_sample):
    """Cria a tabela com tipos VARCHAR, adiciona chave primária e índice único."""
    dtype_map = {}
    for col in df_sample.columns:
        if col in KEY_COLUMNS:
            dtype_map[col] = types.VARCHAR(50)
        else:
            dtype_map[col] = types.VARCHAR(255)

    with engine.begin() as conn:
        # Cria a tabela apenas se não existir
        df_sample.head(0).to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists='append',
            index=False,
            dtype=dtype_map
        )

    # Garante coluna de chave primária auto incremental
    pk_sql = f"""
    ALTER TABLE {TABLE_NAME}
    ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
    """
    with engine.begin() as conn:
        try:
            conn.execute(text(pk_sql))
            print("Chave primária (id) criada com sucesso em consulta_vagas.")
        except Exception as e:
            # 1060: Duplicate column name, 1068: Multiple primary key defined
            if (
                "1060" in str(e)
                or "Duplicate column name" in str(e)
                or "1068" in str(e)
                or "Multiple primary key" in str(e)
            ):
                print("Chave primária (id) em consulta_vagas já existe ou já há uma PRIMARY KEY.")
            else:
                print(f"Erro ao criar chave primária em {TABLE_NAME}: {e}")

    idx_sql = f"""
    ALTER TABLE {TABLE_NAME}
    ADD UNIQUE INDEX idx_unique_consulta_vagas
    ({', '.join(KEY_COLUMNS)});
    """
    with engine.begin() as conn:
        try:
            conn.execute(text(idx_sql))
            print("Índice único (idx_unique_consulta_vagas) criado com sucesso.")
        except Exception as e:
            if '1061' in str(e) or 'Duplicate key name' in str(e) or 'Duplicate key' in str(e):
                print("Índice único (idx_unique_consulta_vagas) já existe.")
            else:
                print(f"Erro ao configurar índices: {e}")

def get_csv_files():
    """Lista todos os CSV em dados/consulta_vagas_*."""
    pattern = os.path.join(DADOS_BASE, 'consulta_vagas_*', '*.csv')
    files = glob.glob(pattern)
    return sorted(files)

def process_one_file(engine, file_path):
    """Processa um único arquivo CSV."""
    with _print_lock:
        print(f"Iniciando: {file_path}")
    try:
        reader = pd.read_csv(
            file_path,
            sep=';',
            encoding='latin1',
            quotechar='"',
            dtype=str,
            chunksize=CHUNKSIZE_READ
        )
        for chunk in reader:
            chunk = clean_column_names(chunk)
            for attempt in range(MAX_INSERT_RETRIES):
                try:
                    chunk.to_sql(
                        name=TABLE_NAME,
                        con=engine,
                        if_exists='append',
                        index=False,
                        method=insert_ignore,
                        chunksize=CHUNKSIZE_INSERT
                    )
                    break
                except Exception as e:
                    if attempt < MAX_INSERT_RETRIES - 1 and ("1213" in str(e) or "1205" in str(e)):
                        time.sleep(2 * (attempt + 1))
                    else:
                        raise
        with _print_lock:
            print(f"Finalizado: {file_path}")
        return file_path, None
    except Exception as e:
        with _print_lock:
            print(f"Erro ao processar {file_path}: {e}")
        return file_path, e

def process_and_import():
    csv_files = get_csv_files()
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {DADOS_BASE}/consulta_vagas_*/")
        return

    n_workers = min(MAX_WORKERS, len(csv_files))
    engine = get_engine(pool_size=n_workers + 2)

    print(f"Encontrados {len(csv_files)} arquivo(s) CSV. Usando {n_workers} thread(s).")

    # Thread principal: cria tabela e índice
    first_file = csv_files[0]
    first_chunk = next(
        pd.read_csv(
            first_file,
            sep=';',
            encoding='latin1',
            quotechar='"',
            dtype=str,
            chunksize=CHUNKSIZE_READ
        ).__iter__()
    )
    first_chunk = clean_column_names(first_chunk)
    setup_table_and_indexes(engine, first_chunk)

    # Workers
    errors = []
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(process_one_file, engine, path): path
            for path in csv_files
        }
        for future in as_completed(futures):
            _path, err = future.result()
            if err is not None:
                errors.append((_path, err))

    # Reprocessar falhas de lock sequencialmente
    retry_paths = [p for p, e in errors if "1213" in str(e) or "1205" in str(e)]
    other_errors = [(p, e) for p, e in errors if p not in retry_paths]
    if retry_paths:
        print(f"\nReprocessando {len(retry_paths)} arquivo(s) em modo sequencial...")
        engine_serial = get_engine(pool_size=1)
        for p in retry_paths:
            _, err = process_one_file(engine_serial, p)
            if err is not None:
                other_errors.append((p, err))
        errors = other_errors

    if errors:
        print(f"\nProcessamento concluído com {len(errors)} erro(s):")
        for p, e in errors:
            print(f"  - {os.path.basename(p)}: {e}")
    else:
        print("\nProcessamento concluído com sucesso.")

if __name__ == "__main__":
    process_and_import()
