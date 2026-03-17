"""
Cria uma TABELA no MySQL para consolidar o total de votos por partido,
por município, por turno e por ano, com base na tabela 'boletim_urna'.

Tabela criada: votos_partido
"""
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do Banco de Dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '3306'),
    'database': os.getenv('DB_DATABASE', 'eleicoes'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '')
}

TABLE_NAME = 'votos_partido'


def get_engine():
    """Retorna o engine do SQLAlchemy."""
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)


def create_table():
    engine = get_engine()

    # Consulta para popular a tabela com votos agrupados por partido e cargo
    select_sql = f"""
    SELECT
        bu.NR_PARTIDO,
        bu.SG_PARTIDO,
        bu.NM_PARTIDO,
        SUM(CAST(bu.QT_VOTOS AS UNSIGNED)) AS total_votos,
        bu.ANO_ELEICAO,
        bu.NM_MUNICIPIO,
        bu.CD_MUNICIPIO,
        bu.CD_ELEICAO,
        bu.NR_TURNO,
        bu.SG_UF,
        MAX(bu.DS_CARGO_PERGUNTA) AS DS_CARGO_PERGUNTA,
        bu.CD_CARGO_PERGUNTA
    FROM
        boletim_urna AS bu
    GROUP BY
        bu.ANO_ELEICAO,
        bu.CD_MUNICIPIO,
        bu.NM_MUNICIPIO,
        bu.CD_ELEICAO,
        bu.NR_TURNO,
        bu.SG_UF,
        bu.CD_CARGO_PERGUNTA,
        bu.NR_PARTIDO,
        bu.SG_PARTIDO,
        bu.NM_PARTIDO
    ORDER BY
        total_votos DESC
    """

    sql = f"""
    DROP TABLE IF EXISTS {TABLE_NAME};
    CREATE TABLE {TABLE_NAME} AS
    {select_sql};
    ALTER TABLE {TABLE_NAME}
        ADD COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
    """

    print(f"Criando/Atualizando a tabela '{TABLE_NAME}'...")
    try:
        with engine.begin() as conn:
            for statement in sql.split(';'):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
        print(f"Tabela '{TABLE_NAME}' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela '{TABLE_NAME}': {e}")


if __name__ == "__main__":
    create_table()
