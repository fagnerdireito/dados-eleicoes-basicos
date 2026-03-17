import os
import glob
import pandas as pd
from sqlalchemy import create_engine

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

def get_engine():
    """Retorna o engine do SQLAlchemy usando mysql-connector-python."""
    conn_str = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(conn_str)

def clean_column_names(df):
    """Remove aspas duplas dos nomes das colunas e aplica o mapeamento."""
    df.columns = [c.strip('"').upper() for c in df.columns]
    return df.rename(columns=COLUMN_MAPPING)

def process_and_import():
    engine = get_engine()
    table_name = 'boletim_urna'
    
    # Localiza todos os CSVs recursivamente na pasta bweb
    csv_files = glob.glob('bweb/**/*.csv', recursive=True)
    
    if not csv_files:
        print("Nenhum arquivo CSV encontrado em 'bweb/'.")
        return

    first_file = True
    
    for file_path in csv_files:
        print(f"Iniciando processamento: {file_path}")
        
        # Leitura em chunks para otimizar memória (50k linhas por vez)
        try:
            reader = pd.read_csv(
                file_path, 
                sep=';', 
                encoding='latin1', 
                quotechar='"',
                dtype=str, # Mantém como string para evitar perda de zeros à esquerda (ex: NR_SECAO)
                chunksize=50000
            )
            
            for chunk in reader:
                # Limpeza e unificação das colunas
                chunk = clean_column_names(chunk)
                
                # Na primeira inserção do primeiro arquivo, recria a tabela.
                # Nas demais, apenas acrescenta os dados (append).
                if_exists_mode = 'replace' if first_file else 'append'
                
                chunk.to_sql(
                    name=table_name, 
                    con=engine, 
                    if_exists=if_exists_mode, 
                    index=False,
                    chunksize=10000 # Inserção no MySQL em lotes menores
                )
                
                if first_file:
                    print(f"Tabela '{table_name}' inicializada com o cabeçalho de: {file_path}")
                    first_file = False
                    
            print(f"Concluído: {file_path}")
            
        except Exception as e:
            print(f"Erro ao processar {file_path}: {e}")

    print("\nImportação concluída com sucesso para todos os arquivos.")

if __name__ == "__main__":
    process_and_import()
