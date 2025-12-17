import pandas as pd
import logging
from datetime import datetime
from config.settings import settings
from src.loaders.mysql_loader import MySQLLoader
from sqlalchemy import text

logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, loader: MySQLLoader):
        self.loader = loader
        # Caches to avoid repeated DB lookups
        self.cache_estados = {} # sigla -> id
        self.cache_municipios = {} # (estado_id, codigo_tse) -> id
        self.cache_zonas = {} # (municipio_id, nr_zona) -> id
        self.cache_secoes = {} # (zona_id, nr_secao) -> id
        self.cache_partidos = {} # numero -> id
        self.cache_cargos = {} # codigo -> id
        self.cache_candidatos = {} # (eleicao_id, cargo_id, nr_votavel) -> id
        self.cache_eleicoes = {} # (ano, turno, cd_eleicao) -> id

    def get_or_create_eleicao(self, metadata, row):
        key = (metadata['ano'], metadata['turno'], str(row['CD_ELEICAO']))
        if key in self.cache_eleicoes:
            return self.cache_eleicoes[key]
        
        # Check DB
        query = text("SELECT id FROM eleicoes WHERE ano=:ano AND turno=:turno AND cd_eleicao=:cd_eleicao")
        result = self.loader.execute_query(query, {'ano': metadata['ano'], 'turno': metadata['turno'], 'cd_eleicao': str(row['CD_ELEICAO'])})
        row_db = result.first()
        
        if row_db:
            self.cache_eleicoes[key] = row_db[0]
            return row_db[0]
        
        # Insert
        insert = text("""
            INSERT INTO eleicoes (ano, turno, tipo_eleicao, dt_pleito, ds_eleicao, cd_eleicao)
            VALUES (:ano, :turno, :tipo, :dt_pleito, :ds_eleicao, :cd_eleicao)
        """)
        # Infer type and date from row/metadata
        # Defaulting tipo=1 (Geral) for now, logic can be improved
        dt_pleito = datetime.strptime(row['DT_GERACAO'], '%d/%m/%Y').date() if 'DT_GERACAO' in row else None
        
        self.loader.execute_query(insert, {
            'ano': metadata['ano'],
            'turno': metadata['turno'],
            'tipo': 1, # Placeholder logic
            'dt_pleito': dt_pleito,
            'ds_eleicao': row.get('DS_ELEICAO', ''),
            'cd_eleicao': str(row['CD_ELEICAO'])
        })
        
        # Get ID back
        return self.get_or_create_eleicao(metadata, row)

    def process_chunk(self, chunk, metadata):
        """
        Main method to process a chunk and load it into normalized tables.
        """
        if len(chunk) == 0:
            return

        # 1. Get Election ID (assuming consistent election per chunk/file)
        # We might have multiple elections in one file (e.g. Federal vs State)? Usually yes, CD_ELEICAO varies.
        # But for now let's assume we handle row by row or group by CD_ELEICAO.
        # Grouping by CD_ELEICAO is safer.
        
        for cd_eleicao, group in chunk.groupby('CD_ELEICAO'):
            # Get Election ID
            first_row = group.iloc[0]
            eleicao_id = self.get_or_create_eleicao(metadata, first_row)
            
            # Prepare data for bulk insert into 'votos_consolidados' (Simplified for now as requested by user to "activate load")
            # To do it properly we need all dimensions. 
            # Given the constraints, I'll implement a "Fast Load" directly to `votos_consolidados` using raw values where possible
            # or creating simple dimensions on the fly.
            
            # Actually, `votos_consolidados` links to IDs. We must create dimensions.
            # This is complex for a single step. 
            
            # STRATEGY: 
            # 1. Create temporary dataframe with mapped IDs
            # 2. Insert into dimensions (ignoring duplicates)
            # 3. Insert into fact
            
            # A. Estados
            # Assuming all rows are same UF from metadata
            # ...
            
            # Let's simplify: User wants "carga". I will focus on `votos_consolidados` for the web system first as per instructions.
            # But the instructions say "primeiro consolidacao...".
            
            # Let's implement a robust row-by-row (slow but safe) or bulk approach.
            # Bulk approach for Dimensions:
            
            # 1. Unique Municipios
            municipios = group[['SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO']].drop_duplicates()
            for _, row in municipios.iterrows():
                self.ensure_municipio(row)
                
            # 2. Unique Cargos
            cargos = group[['CD_CARGO_PERGUNTA', 'DS_CARGO_PERGUNTA']].drop_duplicates()
            for _, row in cargos.iterrows():
                self.ensure_cargo(row)
                
            # 3. Unique Partidos
            partidos = group[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']].drop_duplicates()
            for _, row in partidos.iterrows():
                self.ensure_partido(row)

            # 4. Unique Candidatos
            # Candidatos depend on Election, Cargo, Partido
            candidatos = group[['CD_CARGO_PERGUNTA', 'NR_PARTIDO', 'NR_VOTAVEL', 'NM_VOTAVEL']].drop_duplicates()
            for _, row in candidatos.iterrows():
                self.ensure_candidato(eleicao_id, row)

            # 5. Insert Votos Consolidados (Aggregated by Muni, Cargo, Candidato)
            # Group by these fields and sum votes
            agg_df = group.groupby(['CD_MUNICIPIO', 'CD_CARGO_PERGUNTA', 'NR_VOTAVEL']).agg({
                'QT_VOTOS': 'sum'
            }).reset_index()
            
            # Map back to IDs and insert
            self.bulk_insert_consolidados(eleicao_id, agg_df, metadata['uf'])


    def ensure_municipio(self, row):
        # Implementation of get_or_create for Municipio
        # Cache key: (SG_UF, CD_MUNICIPIO) -- actually State ID + CD_MUNICIPIO
        # First ensure State
        uf = row['SG_UF']
        if uf not in self.cache_estados:
            # Check/Insert State
            res = self.loader.execute_query(text("SELECT id FROM estados WHERE sigla=:sigla"), {'sigla': uf}).first()
            if res:
                self.cache_estados[uf] = res[0]
            else:
                self.loader.execute_query(text("INSERT INTO estados (sigla) VALUES (:sigla)"), {'sigla': uf})
                self.cache_estados[uf] = self.loader.execute_query(text("SELECT id FROM estados WHERE sigla=:sigla"), {'sigla': uf}).first()[0]
        
        estado_id = self.cache_estados[uf]
        cd_mun = str(row['CD_MUNICIPIO'])
        
        if (estado_id, cd_mun) not in self.cache_municipios:
             res = self.loader.execute_query(text("SELECT id FROM municipios WHERE estado_id=:eid AND codigo_tse=:cd"), 
                                           {'eid': estado_id, 'cd': cd_mun}).first()
             if res:
                 self.cache_municipios[(estado_id, cd_mun)] = res[0]
             else:
                 self.loader.execute_query(text("INSERT INTO municipios (estado_id, codigo_tse, nome) VALUES (:eid, :cd, :nm)"), 
                                         {'eid': estado_id, 'cd': cd_mun, 'nm': row['NM_MUNICIPIO']})
                 self.cache_municipios[(estado_id, cd_mun)] = self.loader.execute_query(text("SELECT id FROM municipios WHERE estado_id=:eid AND codigo_tse=:cd"), 
                                           {'eid': estado_id, 'cd': cd_mun}).first()[0]

    def ensure_cargo(self, row):
        cd = str(row['CD_CARGO_PERGUNTA'])
        if cd not in self.cache_cargos:
            res = self.loader.execute_query(text("SELECT id FROM cargos WHERE codigo=:cd"), {'cd': cd}).first()
            if res:
                self.cache_cargos[cd] = res[0]
            else:
                self.loader.execute_query(text("INSERT INTO cargos (codigo, descricao) VALUES (:cd, :ds)"), {'cd': cd, 'ds': row['DS_CARGO_PERGUNTA']})
                self.cache_cargos[cd] = self.loader.execute_query(text("SELECT id FROM cargos WHERE codigo=:cd"), {'cd': cd}).first()[0]

    def ensure_partido(self, row):
        nr = str(row['NR_PARTIDO'])
        if nr == '-1' or nr == '#NULO#': return # Skip invalid
        
        if nr not in self.cache_partidos:
            res = self.loader.execute_query(text("SELECT id FROM partidos WHERE numero=:nr"), {'nr': nr}).first()
            if res:
                self.cache_partidos[nr] = res[0]
            else:
                self.loader.execute_query(text("INSERT INTO partidos (numero, sigla, nome) VALUES (:nr, :sg, :nm)"), 
                                        {'nr': nr, 'sg': row['SG_PARTIDO'], 'nm': row['NM_PARTIDO']})
                self.cache_partidos[nr] = self.loader.execute_query(text("SELECT id FROM partidos WHERE numero=:nr"), {'nr': nr}).first()[0]

    def ensure_candidato(self, eleicao_id, row):
        cargo_id = self.cache_cargos.get(str(row['CD_CARGO_PERGUNTA']))
        partido_id = self.cache_partidos.get(str(row['NR_PARTIDO']))
        nr_votavel = str(row['NR_VOTAVEL'])
        
        key = (eleicao_id, cargo_id, nr_votavel)
        if key not in self.cache_candidatos:
             res = self.loader.execute_query(text("SELECT id FROM candidatos WHERE eleicao_id=:eid AND cargo_id=:cid AND nr_votavel=:nr"), 
                                           {'eid': eleicao_id, 'cid': cargo_id, 'nr': nr_votavel}).first()
             if res:
                 self.cache_candidatos[key] = res[0]
             else:
                 self.loader.execute_query(text("INSERT INTO candidatos (eleicao_id, cargo_id, partido_id, nr_votavel, nome) VALUES (:eid, :cid, :pid, :nr, :nm)"), 
                                         {'eid': eleicao_id, 'cid': cargo_id, 'pid': partido_id, 'nr': nr_votavel, 'nm': row['NM_VOTAVEL']})
                 self.cache_candidatos[key] = self.loader.execute_query(text("SELECT id FROM candidatos WHERE eleicao_id=:eid AND cargo_id=:cid AND nr_votavel=:nr"), 
                                           {'eid': eleicao_id, 'cid': cargo_id, 'nr': nr_votavel}).first()[0]

    def bulk_insert_consolidados(self, eleicao_id, agg_df, uf_metadata):
        data_to_insert = []
        
        # Get State ID for lookups
        if uf_metadata not in self.cache_estados:
             # Should be there if we ran ensure_municipio, but safe check
             res = self.loader.execute_query(text("SELECT id FROM estados WHERE sigla=:sigla"), {'sigla': uf_metadata}).first()
             if res: self.cache_estados[uf_metadata] = res[0]
             
        estado_id = self.cache_estados.get(uf_metadata)
        if not estado_id: return

        for _, row in agg_df.iterrows():
            cd_mun = str(row['CD_MUNICIPIO'])
            cd_cargo = str(row['CD_CARGO_PERGUNTA'])
            nr_votavel = str(row['NR_VOTAVEL'])
            
            mun_id = self.cache_municipios.get((estado_id, cd_mun))
            cargo_id = self.cache_cargos.get(cd_cargo)
            # Candidato Key: (eleicao_id, cargo_id, nr_votavel)
            candidato_id = self.cache_candidatos.get((eleicao_id, cargo_id, nr_votavel))
            
            if mun_id and cargo_id and candidato_id:
                data_to_insert.append({
                    'eleicao_id': eleicao_id,
                    'municipio_id': mun_id,
                    'cargo_id': cargo_id,
                    'candidato_id': candidato_id,
                    'total_votos': int(row['QT_VOTOS'])
                })
        
        if data_to_insert:
            df_insert = pd.DataFrame(data_to_insert)
            # Use loader to load (append mode)
            self.loader.load_df(df_insert, 'votos_consolidados')
