import os
import glob
import threading
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

# Mapeamento de cabeçalhos (De 2022 Para 2024)
COLUMN_MAPPING = {
    'C_ELEICAO': 'CD_ELEICAO',
    'DS_AGREGADAS': 'DS_SECOES_AGREGADAS',
    'QT_ELEITORES_BIOMETRIA_NH': 'QT_ELEI_BIOM_SEM_HABILITACAO'
}

# Colunas que definem a unicidade do Boletim de Urna
KEY_COLUMNS = ['CD_PLEITO', 'CD_MUNICIPIO', 'NR_ZONA', 'NR_SECAO', 'CD_CARGO_PERGUNTA', 'NR_VOTAVEL']

TABLE_NAME = 'boletim_urna'

# Multithread: um arquivo por thread (sem overlap); duplicidade evitada por índice único + INSERT IGNORE
MAX_WORKERS = 2
CHUNKSIZE_READ = 10_000
CHUNKSIZE_INSERT = 2_000

_print_lock = threading.Lock()


from urllib.parse import quote_plus

def get_engine(pool_size=None):
    """Retorna o engine do SQLAlchemy usando mysql-connector-python."""
    pwd = quote_plus(DB_CONFIG['password']) if DB_CONFIG['password'] else ''
    conn_str = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{pwd}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    kwargs = {}
    if pool_size is not None:
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max(0, pool_size - 1)
    return create_engine(conn_str, **kwargs)

def clean_column_names(df):
    """Remove aspas duplas dos nomes das colunas e aplica o mapeamento."""
    df.columns = [c.strip('"').upper() for c in df.columns]
    return df.rename(columns=COLUMN_MAPPING)

def insert_ignore(table, conn, keys, data_iter):
    """
    Método customizado para o pandas.to_sql realizar INSERT IGNORE no MySQL.
    """
    from sqlalchemy.dialects.mysql import insert
    
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    stmt = insert(table.table).values(data)
    stmt = stmt.prefix_with('IGNORE')
    conn.execute(stmt)

def setup_table_and_indexes(engine, df_sample):
    """Cria a tabela com tipos VARCHAR otimizados e adiciona o índice único."""
    # 1. Define tipos VARCHAR otimizados para evitar erro 'Specified key was too long'
    # Colunas de índice: VARCHAR(50) (6 x 50 x 4 bytes < 3072 limit)
    # Outras colunas: VARCHAR(255)
    dtype_map = {}
    for col in df_sample.columns:
        if col in KEY_COLUMNS:
            dtype_map[col] = types.VARCHAR(50)
        else:
            dtype_map[col] = types.VARCHAR(255)
    
    # 2. Cria a estrutura da tabela apenas se não existir
    with engine.begin() as conn:
        df_sample.head(0).to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists='append',
            index=False,
            dtype=dtype_map
        )
    
    # 3. Adiciona o UNIQUE INDEX
    idx_sql = f"""
    ALTER TABLE {TABLE_NAME}
    ADD UNIQUE INDEX idx_unique_bu
    ({', '.join(KEY_COLUMNS)});
    """
    
    with engine.begin() as conn:
        try:
            conn.execute(text(idx_sql))
            print("Índice único (idx_unique_bu) criado com sucesso.")
        except Exception as e:
            if '1061' in str(e) or 'Duplicate key name' in str(e) or 'Duplicate key' in str(e):
                print("O índice único (idx_unique_bu) já existe.")
            else:
                print(f"Erro ao configurar índices: {e}")

def process_one_file(engine, file_path):
    """
    Processa um único arquivo CSV (uma thread por arquivo = sem overlap).
    Duplicidade evitada pelo índice único + INSERT IGNORE.
    """
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
            chunk.to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists='append',
                index=False,
                method=insert_ignore,
                chunksize=CHUNKSIZE_INSERT
            )
        with _print_lock:
            print(f"Finalizado: {file_path}")
        return file_path, None
    except Exception as e:
        with _print_lock:
            print(f"Erro ao processar {file_path}: {e}")
        return file_path, e


def process_and_import():
    csv_files = sorted(glob.glob('bweb/**/*.csv', recursive=True))
    if not csv_files:
        print("Nenhum arquivo CSV encontrado em 'bweb/'.")
        return

    n_workers = min(MAX_WORKERS, len(csv_files))
    engine = get_engine(pool_size=n_workers + 2)

    print(f"Encontrados {len(csv_files)} arquivo(s) CSV. Usando {n_workers} thread(s).")

    # Thread principal: cria tabela e índice uma vez (usa primeiro chunk do primeiro arquivo)
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

    # Workers: cada um processa um arquivo
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

    if errors:
        print(f"\nProcessamento concluído com {len(errors)} erro(s).")
    else:
        print("\nProcessamento concluído com sucesso.")

if __name__ == "__main__":
    process_and_import()
