"""
Cria uma TABELA no MySQL para consolidar o total de votos por candidato,
por município, por turno e por ano, com base nas tabelas 'boletim_urna' e 'consulta_cand'.

Tabela criada: votos_candidatos
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

TABLE_NAME = 'votos_candidatos'


from urllib.parse import quote_plus

def get_engine():
    """Retorna o engine do SQLAlchemy."""
    pwd = quote_plus(DB_CONFIG['password']) if DB_CONFIG['password'] else ''
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{pwd}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)


def create_table():
    engine = get_engine()

    # Consulta base (mesma da VIEW), usada para popular a tabela
    select_sql = f"""
    SELECT
        MAX(cc.NM_URNA_CANDIDATO) AS NM_URNA_CANDIDATO,
        bu.NM_VOTAVEL,
        SUM(CAST(bu.QT_VOTOS AS UNSIGNED)) AS total_votos,
        bu.ANO_ELEICAO,
        bu.NM_MUNICIPIO,
        bu.CD_MUNICIPIO,
        bu.CD_ELEICAO,
        bu.NR_TURNO,
        bu.SG_UF,
        MAX(bu.DS_CARGO_PERGUNTA) AS DS_CARGO_PERGUNTA,
        MAX(cc.SG_PARTIDO) AS SG_PARTIDO,
        MAX(cc.DS_SIT_TOT_TURNO) AS SITUACAO_ELEICAO
    FROM
        boletim_urna AS bu
    LEFT JOIN
        consulta_cand AS cc
        ON bu.ANO_ELEICAO = cc.ANO_ELEICAO
        AND bu.NR_VOTAVEL = cc.NR_CANDIDATO
        AND bu.SG_UF = cc.SG_UF
        AND bu.CD_CARGO_PERGUNTA = cc.CD_CARGO
        AND bu.CD_MUNICIPIO = cc.SG_UE
    GROUP BY
        bu.ANO_ELEICAO,
        bu.CD_MUNICIPIO,
        bu.NM_MUNICIPIO,
        bu.CD_ELEICAO,
        bu.NR_TURNO,
        bu.SG_UF,
        bu.CD_CARGO_PERGUNTA,
        bu.NR_VOTAVEL,
        bu.NM_VOTAVEL
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

