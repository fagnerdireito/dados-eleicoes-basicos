"""
Cria uma TABELA no MySQL para consolidar o total de votos por partido,
por município, por turno e por ano, com base na tabela 'boletim_urna'.

Tabela criada: votos_partido
"""
"""
Cria a tabela votos_partido no MySQL consolidando votos por partido,
por município, por turno e por ano, com base na tabela boletim_urna.

Ajustado para evitar erro 1206 (The total number of locks exceeds the lock table size)
ao quebrar a carga em etapas menores.
"""

import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

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
    pwd = quote_plus(DB_CONFIG['password']) if DB_CONFIG['password'] else ''
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{pwd}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=3600,
        isolation_level="READ COMMITTED",
    )


def ensure_source_indexes(conn):
    """
    Cria índices úteis na tabela de origem, se ainda não existirem.
    """
    print("Garantindo índices na tabela de origem...")

    statements = [
        """
        CREATE INDEX idx_bu_partido_agrupamento
        ON boletim_urna (
            ANO_ELEICAO,
            CD_MUNICIPIO,
            CD_ELEICAO,
            NR_TURNO,
            SG_UF,
            CD_CARGO_PERGUNTA,
            NR_PARTIDO
        )
        """,
        """
        CREATE INDEX idx_bu_partido_aux
        ON boletim_urna (
            ANO_ELEICAO,
            SG_PARTIDO,
            NM_PARTIDO
        )
        """
    ]

    for stmt in statements:
        try:
            conn.execute(text(stmt))
        except Exception as e:
            msg = str(e).lower()
            if 'duplicate key name' in msg or '1061' in msg:
                pass
            else:
                print(f"Aviso ao criar índice de origem: {e}")


def recreate_target_table(conn):
    print(f"Recriando a tabela '{TABLE_NAME}'...")

    conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))

    conn.execute(text(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            NR_PARTIDO VARCHAR(50) NULL,
            SG_PARTIDO VARCHAR(50) NULL,
            NM_PARTIDO VARCHAR(255) NULL,
            total_votos BIGINT UNSIGNED NOT NULL,
            ANO_ELEICAO VARCHAR(10) NULL,
            NM_MUNICIPIO VARCHAR(255) NULL,
            CD_MUNICIPIO VARCHAR(50) NULL,
            CD_ELEICAO VARCHAR(50) NULL,
            NR_TURNO VARCHAR(10) NULL,
            SG_UF VARCHAR(10) NULL,
            DS_CARGO_PERGUNTA VARCHAR(255) NULL,
            CD_CARGO_PERGUNTA VARCHAR(50) NULL
        ) ENGINE=InnoDB
    """))


def get_anos(conn):
    rows = conn.execute(text("""
        SELECT DISTINCT ANO_ELEICAO
        FROM boletim_urna
        WHERE ANO_ELEICAO IS NOT NULL
        ORDER BY ANO_ELEICAO
    """)).fetchall()

    return [row[0] for row in rows]


def insert_for_year(conn, ano):
    print(f"Inserindo dados do ano {ano}...")

    conn.execute(text(f"""
        INSERT INTO {TABLE_NAME} (
            NR_PARTIDO,
            SG_PARTIDO,
            NM_PARTIDO,
            total_votos,
            ANO_ELEICAO,
            NM_MUNICIPIO,
            CD_MUNICIPIO,
            CD_ELEICAO,
            NR_TURNO,
            SG_UF,
            DS_CARGO_PERGUNTA,
            CD_CARGO_PERGUNTA
        )
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
        FROM boletim_urna AS bu
        WHERE bu.ANO_ELEICAO = :ano
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
    """), {"ano": ano})


def create_target_indexes(conn):
    print(f"Criando índices na tabela '{TABLE_NAME}'...")

    statements = [
        f"CREATE INDEX idx_{TABLE_NAME}_ano ON {TABLE_NAME} (ANO_ELEICAO)",
        f"CREATE INDEX idx_{TABLE_NAME}_municipio ON {TABLE_NAME} (CD_MUNICIPIO)",
        f"CREATE INDEX idx_{TABLE_NAME}_uf ON {TABLE_NAME} (SG_UF)",
        f"CREATE INDEX idx_{TABLE_NAME}_turno ON {TABLE_NAME} (NR_TURNO)",
        f"CREATE INDEX idx_{TABLE_NAME}_partido ON {TABLE_NAME} (NR_PARTIDO)",
        f"CREATE INDEX idx_{TABLE_NAME}_cargo ON {TABLE_NAME} (CD_CARGO_PERGUNTA)",
        f"CREATE INDEX idx_{TABLE_NAME}_votos ON {TABLE_NAME} (total_votos)",
        f"CREATE INDEX idx_{TABLE_NAME}_ano_municipio ON {TABLE_NAME} (ANO_ELEICAO, CD_MUNICIPIO)",
        f"CREATE INDEX idx_{TABLE_NAME}_ano_uf_partido ON {TABLE_NAME} (ANO_ELEICAO, SG_UF, NR_PARTIDO)",
    ]

    for stmt in statements:
        try:
            conn.execute(text(stmt))
        except Exception as e:
            msg = str(e).lower()
            if 'duplicate key name' in msg or '1061' in msg:
                pass
            else:
                print(f"Aviso ao criar índice final: {e}")


def create_table():
    engine = get_engine()

    try:
        with engine.begin() as conn:
            ensure_source_indexes(conn)
            recreate_target_table(conn)

        with engine.connect() as conn:
            anos = get_anos(conn)

        print(f"Anos encontrados: {anos}")

        for ano in anos:
            with engine.begin() as conn:
                insert_for_year(conn, ano)

        with engine.begin() as conn:
            create_target_indexes(conn)

        print(f"Tabela '{TABLE_NAME}' criada com sucesso.")

    except Exception as e:
        print(f"Erro ao criar a tabela '{TABLE_NAME}': {e}")


if __name__ == "__main__":
    create_table()