"""
Cria uma VIEW no MySQL para consolidar o total de votos por candidato, por município, por turno e por ano.
Utiliza as tabelas 'boletim_urna' e 'consulta_cand'.
"""
from sqlalchemy import create_engine, text

# Configuração do Banco de Dados
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'eleicoes',
    'user': 'root',
    'password': ''
}

VIEW_NAME = 'view_votos_candidatos_municipio'

def get_engine():
    """Retorna o engine do SQLAlchemy."""
    conn_str = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)

def create_view():
    engine = get_engine()
    
    # Query para criar a VIEW
    # Nota: Usamos CAST(... AS CHAR) para garantir a compatibilidade de tipos no JOIN entre CD_MUNICIPIO e SG_UE
    view_sql = f"""
    CREATE OR REPLACE VIEW {VIEW_NAME} AS
    SELECT
        bu.id,
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
        total_votos DESC;
    """

    print(f"Criando/Atualizando a VIEW '{VIEW_NAME}'...")
    try:
        with engine.begin() as conn:
            conn.execute(text(view_sql))
        print(f"VIEW '{VIEW_NAME}' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a VIEW: {e}")

if __name__ == "__main__":
    create_view()
