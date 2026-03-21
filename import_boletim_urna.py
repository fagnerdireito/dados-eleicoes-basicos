import os
import glob
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, text

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

TABLE_NAME = 'boletim_de_urna'

# Tamanhos VARCHAR conforme layout oficial / tabela alvo
BU_COLUMN_LENGTHS = [
    ('DT_GERACAO', 10),
    ('HH_GERACAO', 8),
    ('ANO_ELEICAO', 4),
    ('CD_TIPO_ELEICAO', 1),
    ('NM_TIPO_ELEICAO', 17),
    ('CD_PLEITO', 3),
    ('DT_PLEITO', 19),
    ('NR_TURNO', 1),
    ('CD_ELEICAO', 3),
    ('DS_ELEICAO', 30),
    ('SG_UF', 2),
    ('CD_MUNICIPIO', 5),
    ('NM_MUNICIPIO', 22),
    ('NR_ZONA', 3),
    ('NR_SECAO', 3),
    ('NR_LOCAL_VOTACAO', 4),
    ('CD_CARGO_PERGUNTA', 2),
    ('DS_CARGO_PERGUNTA', 17),
    ('NR_PARTIDO', 2),
    ('SG_PARTIDO', 13),
    ('NM_PARTIDO', 46),
    ('DT_BU_RECEBIDO', 19),
    ('QT_APTOS', 3),
    ('QT_COMPARECIMENTO', 3),
    ('QT_ABSTENCOES', 3),
    ('CD_TIPO_URNA', 1),
    ('DS_TIPO_URNA', 7),
    ('CD_TIPO_VOTAVEL', 1),
    ('DS_TIPO_VOTAVEL', 7),
    ('NR_VOTAVEL', 5),
    ('NM_VOTAVEL', 28),
    ('QT_VOTOS', 3),
    ('NR_URNA_EFETIVADA', 7),
    ('CD_CARGA_1_URNA_EFETIVADA', 24),
    ('CD_CARGA_2_URNA_EFETIVADA', 7),
    ('CD_FLASHCARD_URNA_EFETIVADA', 8),
    ('DT_CARGA_URNA_EFETIVADA', 19),
    ('DS_CARGO_PERGUNTA_SECAO', 8),
    ('DS_SECOES_AGREGADAS', 15),
    ('DT_ABERTURA', 19),
    ('DT_ENCERRAMENTO', 19),
    ('QT_ELEI_BIOM_SEM_HABILITACAO', 2),
    ('DT_EMISSAO_BU', 19),
    ('NR_JUNTA_APURADORA', 2),
    ('NR_TURMA_APURADORA', 2),
]

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

def build_create_table_sql():
    """DDL com id AUTO_INCREMENT, VARCHARs oficiais e UNIQUE em chave natural."""
    cols = ['`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT']
    for name, length in BU_COLUMN_LENGTHS:
        cols.append(f'`{name}` VARCHAR({length}) DEFAULT NULL')
    cols.append('PRIMARY KEY (`id`)')
    cols.append(
        'UNIQUE KEY `idx_unique_bu` '
        '(`CD_PLEITO`,`CD_MUNICIPIO`,`NR_ZONA`,`NR_SECAO`,`CD_CARGO_PERGUNTA`,`NR_VOTAVEL`)'
    )
    body = ',\n  '.join(cols)
    return (
        f'CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (\n  {body}\n) '
        'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci'
    )


def setup_table_and_indexes(engine):
    """Cria a tabela com id e VARCHARs fixos; garante índice único na chave natural."""
    with engine.begin() as conn:
        conn.execute(text(build_create_table_sql()))
        print(f"Tabela `{TABLE_NAME}` verificada/criada (com coluna `id`).")

    idx_sql = f"""
    ALTER TABLE `{TABLE_NAME}`
    ADD UNIQUE INDEX idx_unique_bu
    ({', '.join(f'`{c}`' for c in KEY_COLUMNS)});
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

    setup_table_and_indexes(engine)

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
