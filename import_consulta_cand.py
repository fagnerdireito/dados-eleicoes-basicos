"""
Importa e unifica todos os CSVs de consulta_cand_{ano} em uma única tabela MySQL.
Padrão: dados/consulta_cand_*/consulta_cand_*.csv
Encoding dos CSV: latin1.
Usa multithread: um arquivo por thread, sem overlap; duplicidade evitada por índice único + INSERT IGNORE.
"""
"""
Importa e unifica todos os CSVs de consulta_cand_{ano} em uma única tabela MySQL.
Ajustado para reduzir deadlocks durante INSERT IGNORE concorrente.
"""

import json
import os
import glob
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text, types
from sqlalchemy.exc import OperationalError, InternalError
from dotenv import load_dotenv

load_dotenv()

# #region agent log
DEBUG_LOG = "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/.cursor/debug-f2c28d.log"

def _debug_log(message, data, hypothesis_id, location=""):
    try:
        payload = {
            "sessionId": "f2c28d",
            "message": message,
            "data": data,
            "hypothesisId": hypothesis_id,
            "timestamp": int(time.time() * 1000),
        }
        if location:
            payload["location"] = location
        with open(DEBUG_LOG, "a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass
# #endregion

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "3306"),
    "database": os.getenv("DB_DATABASE", "eleicoes"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
}

KEY_COLUMNS = ["ANO_ELEICAO", "SQ_CANDIDATO"]
TABLE_NAME = "consulta_cand"
DADOS_BASE = os.path.join(os.path.dirname(__file__), "dados")

MAX_WORKERS = 2              # ↓ reduza concorrência de escrita
CHUNKSIZE_READ = 20_000      # ↓ chunk menor para reduzir memória e duração do processamento
CHUNKSIZE_INSERT = 250       # ↓ lote menor = menos tempo segurando lock
MAX_INSERT_RETRIES = 12

# trava só para prints
_print_lock = threading.Lock()

# trava de escrita no banco
# se quiser máximo de estabilidade, mantenha ativa
# se quiser testar mais concorrência, comente o "with _db_write_lock"
_db_write_lock = threading.Lock()


def get_engine(pool_size=1):
    pwd = quote_plus(DB_CONFIG["password"]) if DB_CONFIG["password"] else ""
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{pwd}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    return create_engine(
        conn_str,
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=3600,
        isolation_level="READ COMMITTED",  # ajuda a reduzir contenção
        echo=False,
    )


def is_retryable_db_error(exc: Exception) -> bool:
    s = str(exc)
    return (
        "1213" in s  # deadlock
        or "1205" in s  # lock wait timeout
        or "40001" in s  # serialization failure
    )


def clean_column_names(df):
    df.columns = [str(c).strip('"').strip().upper() for c in df.columns]
    return df


def normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)

    # remove colunas totalmente vazias
    df = df.dropna(axis=1, how="all")

    # padroniza NaN -> None para o MySQL
    df = df.where(pd.notnull(df), None)

    # remove duplicados dentro do próprio chunk para evitar disputa desnecessária
    if all(col in df.columns for col in KEY_COLUMNS):
        df = df.drop_duplicates(subset=KEY_COLUMNS, keep="first")

    return df


def insert_ignore(table, conn, keys, data_iter):
    from sqlalchemy.dialects.mysql import insert

    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return 0

    stmt = insert(table.table).values(data)
    stmt = stmt.prefix_with("IGNORE")
    result = conn.execute(stmt)
    return result.rowcount


def setup_table_and_indexes(engine, df_sample):
    dtype_map = {}
    for col in df_sample.columns:
        if col in KEY_COLUMNS:
            dtype_map[col] = types.VARCHAR(50)
        else:
            dtype_map[col] = types.VARCHAR(255)

    with engine.begin() as conn:
        df_sample.head(0).to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists="append",
            index=False,
            dtype=dtype_map,
        )

    id_sql = f"""
    ALTER TABLE {TABLE_NAME}
    ADD COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
    """

    with engine.begin() as conn:
        try:
            conn.execute(text(id_sql))
            print("Coluna 'id' criada.")
        except Exception as e:
            if "1060" in str(e) or "Duplicate column name" in str(e):
                print("Coluna 'id' já existe.")
            else:
                print(f"Erro ao adicionar coluna id: {e}")

    idx_sql = f"""
    ALTER TABLE {TABLE_NAME}
    ADD UNIQUE INDEX idx_unique_consulta_cand
    ({', '.join(KEY_COLUMNS)});
    """

    with engine.begin() as conn:
        try:
            conn.execute(text(idx_sql))
            print("Índice único criado.")
        except Exception as e:
            if "1061" in str(e) or "Duplicate key name" in str(e) or "Duplicate key" in str(e):
                print("Índice único já existe.")
            else:
                print(f"Erro ao configurar índice: {e}")


def get_csv_files():
    pattern = os.path.join(DADOS_BASE, "consulta_cand_*", "*.csv")
    return sorted(glob.glob(pattern))


def insert_chunk_with_retry(engine, chunk: pd.DataFrame, file_path: str, chunk_no: int):
    """
    Insere 1 chunk com retry, backoff exponencial e jitter.
    Usa transação curta por chunk.
    """
    for attempt in range(1, MAX_INSERT_RETRIES + 1):
        try:
            # Funil de escrita para matar o deadlock na raiz.
            with _db_write_lock:
                with engine.begin() as conn:
                    chunk.to_sql(
                        name=TABLE_NAME,
                        con=conn,
                        if_exists="append",
                        index=False,
                        method=insert_ignore,
                        chunksize=CHUNKSIZE_INSERT,
                    )
            return

        except (OperationalError, InternalError, Exception) as e:
            if not is_retryable_db_error(e) or attempt == MAX_INSERT_RETRIES:
                raise

            sleep_s = min(8, (0.4 * (2 ** (attempt - 1)))) + random.uniform(0, 0.8)

            with _print_lock:
                print(
                    f"[retry {attempt}/{MAX_INSERT_RETRIES}] "
                    f"{os.path.basename(file_path)} chunk={chunk_no} "
                    f"erro={str(e).splitlines()[0]} "
                    f"aguardando {sleep_s:.2f}s"
                )

            time.sleep(sleep_s)


def process_one_file(file_path):
    """
    Cada thread cria sua própria engine/conexão.
    Isso evita compartilhamento estranho de conexão entre threads.
    """
    engine = get_engine(pool_size=1)

    with _print_lock:
        print(f"Iniciando: {file_path}")

    try:
        reader = pd.read_csv(
            file_path,
            sep=";",
            encoding="latin1",
            quotechar='"',
            dtype=str,
            chunksize=CHUNKSIZE_READ,
            low_memory=False,
        )

        for chunk_no, chunk in enumerate(reader, start=1):
            chunk = normalize_chunk(chunk)

            if chunk.empty:
                continue

            insert_chunk_with_retry(engine, chunk, file_path, chunk_no)

        with _print_lock:
            print(f"Finalizado: {file_path}")

        return file_path, None

    except Exception as e:
        _debug_log(
            "process_one_file exception",
            {
                "file_path": file_path,
                "exception_type": type(e).__name__,
                "exception_msg": str(e),
            },
            "H1_H3_H5",
            "import_consulta_cand.py:process_one_file_except",
        )

        with _print_lock:
            print(f"Erro ao processar {file_path}: {e}")

        return file_path, e

    finally:
        engine.dispose()


def process_and_import():
    csv_files = get_csv_files()
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {DADOS_BASE}/consulta_cand_*/")
        return

    n_workers = min(MAX_WORKERS, len(csv_files))
    setup_engine = get_engine(pool_size=1)

    print(f"Encontrados {len(csv_files)} arquivo(s). Usando {n_workers} thread(s).")

    first_file = csv_files[0]
    first_chunk = next(
        pd.read_csv(
            first_file,
            sep=";",
            encoding="latin1",
            quotechar='"',
            dtype=str,
            chunksize=CHUNKSIZE_READ,
            low_memory=False,
        ).__iter__()
    )
    first_chunk = normalize_chunk(first_chunk)
    setup_table_and_indexes(setup_engine, first_chunk)
    setup_engine.dispose()

    errors = []

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(process_one_file, path): path for path in csv_files}

        for future in as_completed(futures):
            path, err = future.result()
            if err is not None:
                errors.append((path, err))

    if errors:
        print(f"\nProcessamento concluído com {len(errors)} erro(s):")
        for p, e in errors:
            short = str(e).split("\n")[0] if str(e) else type(e).__name__
            print(f"  - {os.path.basename(p)}: {short}")
    else:
        print("\nProcessamento concluído com sucesso.")


if __name__ == "__main__":
    process_and_import()