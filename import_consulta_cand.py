"""
Importa e unifica todos os CSVs de consulta_cand_{ano} em uma única tabela MySQL.
Padrão: dados/consulta_cand_*/consulta_cand_*.csv
Encoding dos CSV: latin1.
"""
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text, types

# Configuração do Banco de Dados (desenvolvimento - .env)
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'eleicoes',
    'user': 'root',
    'password': ''
}

# Colunas que definem a unicidade do registro (evita duplicidade)
KEY_COLUMNS = ['ANO_ELEICAO', 'SQ_CANDIDATO']

TABLE_NAME = 'consulta_cand'

# Diretório base dos dados
DADOS_BASE = os.path.join(os.path.dirname(__file__), 'dados')


def get_engine():
    """Retorna o engine do SQLAlchemy usando mysql-connector-python."""
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)


def clean_column_names(df):
    """Remove aspas duplas dos nomes das colunas e normaliza para maiúsculas."""
    df.columns = [str(c).strip('"').strip().upper() for c in df.columns]
    return df


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
    """Cria a tabela com tipos VARCHAR e adiciona o índice único."""
    # Colunas de índice: VARCHAR(50) para respeitar limite do MySQL
    dtype_map = {}
    for col in df_sample.columns:
        if col in KEY_COLUMNS:
            dtype_map[col] = types.VARCHAR(50)
        else:
            dtype_map[col] = types.VARCHAR(255)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        print(f"Tabela '{TABLE_NAME}' criada (estrutura resetada).")

    df_sample.head(0).to_sql(
        name=TABLE_NAME,
        con=engine,
        if_exists='append',
        index=False,
        dtype=dtype_map
    )

    idx_sql = f"""
    ALTER TABLE {TABLE_NAME}
    ADD UNIQUE INDEX idx_unique_consulta_cand
    ({', '.join(KEY_COLUMNS)});
    """
    with engine.begin() as conn:
        try:
            conn.execute(text(idx_sql))
            print("Índice único (idx_unique_consulta_cand) criado com sucesso.")
        except Exception as e:
            if '1061' in str(e) or 'Duplicate key name' in str(e):
                print("Índice único (idx_unique_consulta_cand) já existe.")
            else:
                print(f"Erro ao configurar índices: {e}")


def get_csv_files():
    """Lista todos os CSV em dados/consulta_cand_* (qualquer ano)."""
    pattern = os.path.join(DADOS_BASE, 'consulta_cand_*', '*.csv')
    files = glob.glob(pattern)
    return sorted(files)


def process_and_import():
    engine = get_engine()
    csv_files = get_csv_files()

    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {DADOS_BASE}/consulta_cand_*/")
        return

    print(f"Encontrados {len(csv_files)} arquivo(s) CSV.")
    first_run = True

    for file_path in csv_files:
        print(f"Iniciando: {file_path}")

        try:
            reader = pd.read_csv(
                file_path,
                sep=';',
                encoding='latin1',
                quotechar='"',
                dtype=str,
                chunksize=50000
            )

            for chunk in reader:
                chunk = clean_column_names(chunk)

                if first_run:
                    setup_table_and_indexes(engine, chunk)
                    first_run = False

                chunk.to_sql(
                    name=TABLE_NAME,
                    con=engine,
                    if_exists='append',
                    index=False,
                    method=insert_ignore,
                    chunksize=5000
                )

            print(f"Finalizado: {file_path}")

        except Exception as e:
            print(f"Erro ao processar {file_path}: {e}")

    print("\nProcessamento concluído com sucesso.")


if __name__ == "__main__":
    process_and_import()
