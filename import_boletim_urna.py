import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text, types

# Configuração do Banco de Dados
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'eleicoes',
    'user': 'root',
    'password': ''
}

# Mapeamento de cabeçalhos (De 2022 Para 2024)
COLUMN_MAPPING = {
    'C_ELEICAO': 'CD_ELEICAO',
    'DS_AGREGADAS': 'DS_SECOES_AGREGADAS',
    'QT_ELEITORES_BIOMETRIA_NH': 'QT_ELEI_BIOM_SEM_HABILITACAO'
}

# Colunas que definem a unicidade do Boletim de Urna
KEY_COLUMNS = ['CD_PLEITO', 'CD_MUNICIPIO', 'NR_ZONA', 'NR_SECAO', 'CD_CARGO_PERGUNTA', 'NR_VOTAVEL']

def get_engine():
    """Retorna o engine do SQLAlchemy usando mysql-connector-python."""
    conn_str = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(conn_str)

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
    table_name = 'boletim_urna'
    
    # 1. Define tipos VARCHAR otimizados para evitar erro 'Specified key was too long'
    # Colunas de índice: VARCHAR(50) (6 x 50 x 4 bytes < 3072 limit)
    # Outras colunas: VARCHAR(255)
    dtype_map = {}
    for col in df_sample.columns:
        if col in KEY_COLUMNS:
            dtype_map[col] = types.VARCHAR(50)
        else:
            dtype_map[col] = types.VARCHAR(255)
    
    # 2. Reinicia a tabela se necessário
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        print(f"Tabela '{table_name}' resetada para otimização de tamanho de colunas.")

    # 3. Cria a estrutura da tabela
    df_sample.head(0).to_sql(
        name=table_name, 
        con=engine, 
        if_exists='append', 
        index=False,
        dtype=dtype_map
    )
    
    # 4. Adiciona o UNIQUE INDEX
    idx_sql = f"""
    ALTER TABLE {table_name} 
    ADD UNIQUE INDEX idx_unique_bu 
    ({', '.join(KEY_COLUMNS)});
    """
    
    with engine.begin() as conn:
        try:
            conn.execute(text(idx_sql))
            print("Índice único (idx_unique_bu) criado com sucesso.")
        except Exception as e:
            if '1061' in str(e) or 'Duplicate key name' in str(e):
                print("O índice único (idx_unique_bu) já existe.")
            else:
                print(f"Erro ao configurar índices: {e}")

def process_and_import():
    engine = get_engine()
    table_name = 'boletim_urna'
    
    csv_files = glob.glob('bweb/**/*.csv', recursive=True)
    
    if not csv_files:
        print("Nenhum arquivo CSV encontrado em 'bweb/'.")
        return

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
                
                # Realiza a inserção usando INSERT IGNORE
                chunk.to_sql(
                    name=table_name, 
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
