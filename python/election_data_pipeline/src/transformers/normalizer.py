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

        query = text("SELECT id FROM eleicoes WHERE ano=:ano AND turno=:turno AND cd_eleicao=:cd_eleicao")
        result = self.loader.execute_query(query, {'ano': metadata['ano'], 'turno': metadata['turno'], 'cd_eleicao': str(row['CD_ELEICAO'])})
        row_db = result.first()

        if row_db:
            self.cache_eleicoes[key] = row_db[0]
            return row_db[0]

        insert = text("""
            INSERT INTO eleicoes (ano, turno, tipo_eleicao, dt_pleito, ds_eleicao, cd_eleicao)
            VALUES (:ano, :turno, :tipo, :dt_pleito, :ds_eleicao, :cd_eleicao)
        """)
        dt_pleito = datetime.strptime(row['DT_GERACAO'], '%d/%m/%Y').date() if 'DT_GERACAO' in row else None

        self.loader.execute_query(insert, {
            'ano': metadata['ano'],
            'turno': metadata['turno'],
            'tipo': 1,
            'dt_pleito': dt_pleito,
            'ds_eleicao': row.get('DS_ELEICAO', ''),
            'cd_eleicao': str(row['CD_ELEICAO'])
        })

        return self.get_or_create_eleicao(metadata, row)

    def process_chunk(self, chunk, metadata):
        """
        Main method to process a chunk and load it into normalized tables.
        """
        if len(chunk) == 0:
            return

        for cd_eleicao, group in chunk.groupby('CD_ELEICAO'):
            try:
                first_row = group.iloc[0]
                eleicao_id = self.get_or_create_eleicao(metadata, first_row)

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

                # 4. Unique Candidatos (por município, pois NR_VOTAVEL se repete entre municípios para Prefeito)
                candidatos = group[['CD_MUNICIPIO', 'CD_CARGO_PERGUNTA', 'NR_PARTIDO', 'NR_VOTAVEL', 'NM_VOTAVEL']].drop_duplicates()
                for _, row in candidatos.iterrows():
                    self.ensure_candidato(eleicao_id, row)

                # 5. Insert Votos Consolidados (Aggregated by Muni, Cargo, Candidato)
                agg_df = group.groupby(['CD_MUNICIPIO', 'CD_CARGO_PERGUNTA', 'NR_VOTAVEL']).agg({
                    'QT_VOTOS': 'sum'
                }).reset_index()
                self.bulk_insert_consolidados(eleicao_id, agg_df, metadata['uf'])

                # 6. Normalization of Zonas, Secoes
                secoes_df = group[['CD_MUNICIPIO', 'NR_ZONA', 'NR_SECAO', 'NR_LOCAL_VOTACAO', 'QT_APTOS', 'QT_COMPARECIMENTO', 'QT_ABSTENCOES']].drop_duplicates(['CD_MUNICIPIO', 'NR_ZONA', 'NR_SECAO'])
                for _, row in secoes_df.iterrows():
                    self.ensure_zona_secao(row, metadata['uf'])

                # 7. Bulk Insert Votos Secao
                self.bulk_insert_votos_secao(eleicao_id, group, metadata['uf'])

            except Exception as e:
                logger.error(f"Erro ao processar grupo CD_ELEICAO={cd_eleicao}: {e}", exc_info=True)
                # Continua para o próximo grupo sem interromper o chunk inteiro

    def ensure_zona_secao(self, row, uf):
        estado_id = self.cache_estados.get(uf)
        if not estado_id:
            return

        cd_mun = str(row['CD_MUNICIPIO'])
        mun_id = self.cache_municipios.get((estado_id, cd_mun))
        if not mun_id:
            return

        nr_zona = str(row['NR_ZONA'])
        if (mun_id, nr_zona) not in self.cache_zonas:
            res = self.loader.execute_query(
                text("SELECT id FROM zonas WHERE municipio_id=:mid AND nr_zona=:nr"),
                {'mid': mun_id, 'nr': nr_zona}
            ).first()
            if res:
                self.cache_zonas[(mun_id, nr_zona)] = res[0]
            else:
                self.loader.execute_query(
                    text("INSERT INTO zonas (municipio_id, nr_zona) VALUES (:mid, :nr)"),
                    {'mid': mun_id, 'nr': nr_zona}
                )
                self.cache_zonas[(mun_id, nr_zona)] = self.loader.execute_query(
                    text("SELECT id FROM zonas WHERE municipio_id=:mid AND nr_zona=:nr"),
                    {'mid': mun_id, 'nr': nr_zona}
                ).first()[0]

        zona_id = self.cache_zonas[(mun_id, nr_zona)]

        nr_secao = str(row['NR_SECAO'])
        if (zona_id, nr_secao) not in self.cache_secoes:
            res = self.loader.execute_query(
                text("SELECT id FROM secoes WHERE zona_id=:zid AND nr_secao=:nr"),
                {'zid': zona_id, 'nr': nr_secao}
            ).first()
            if res:
                self.cache_secoes[(zona_id, nr_secao)] = res[0]
            else:
                self.loader.execute_query(
                    text("INSERT INTO secoes (zona_id, nr_secao, nr_local_votacao) VALUES (:zid, :nr, :loc)"),
                    {'zid': zona_id, 'nr': nr_secao, 'loc': str(row.get('NR_LOCAL_VOTACAO', ''))}
                )
                self.cache_secoes[(zona_id, nr_secao)] = self.loader.execute_query(
                    text("SELECT id FROM secoes WHERE zona_id=:zid AND nr_secao=:nr"),
                    {'zid': zona_id, 'nr': nr_secao}
                ).first()[0]

    def bulk_insert_votos_secao(self, eleicao_id, group_df, uf):
        data = []
        estado_id = self.cache_estados.get(uf)
        if not estado_id:
            return

        skipped = 0
        for _, row in group_df.iterrows():
            cd_mun = str(row['CD_MUNICIPIO'])
            mun_id = self.cache_municipios.get((estado_id, cd_mun))
            if not mun_id:
                skipped += 1
                continue

            nr_zona = str(row['NR_ZONA'])
            zona_id = self.cache_zonas.get((mun_id, nr_zona))
            if not zona_id:
                skipped += 1
                continue

            nr_secao = str(row['NR_SECAO'])
            secao_id = self.cache_secoes.get((zona_id, nr_secao))
            if not secao_id:
                skipped += 1
                continue

            cd_cargo = str(row['CD_CARGO_PERGUNTA'])
            cargo_id = self.cache_cargos.get(cd_cargo)
            if not cargo_id:
                skipped += 1
                continue

            nr_votavel = str(row['NR_VOTAVEL'])
            candidato_id = self.cache_candidatos.get((eleicao_id, mun_id, cargo_id, nr_votavel))
            if not candidato_id:
                skipped += 1
                continue

            data.append({
                'eleicao_id': eleicao_id,
                'secao_id': secao_id,
                'cargo_id': cargo_id,
                'candidato_id': candidato_id,
                'qt_votos': int(row['QT_VOTOS']),
                'qt_aptos': int(row['QT_APTOS']) if pd.notna(row.get('QT_APTOS')) else 0,
                'qt_comparecimento': int(row['QT_COMPARECIMENTO']) if pd.notna(row.get('QT_COMPARECIMENTO')) else 0,
                'qt_abstencoes': int(row['QT_ABSTENCOES']) if pd.notna(row.get('QT_ABSTENCOES')) else 0
            })

        if skipped > 0:
            logger.warning(f"bulk_insert_votos_secao: {skipped} linhas ignoradas por falta de referência no cache (eleicao_id={eleicao_id}, uf={uf})")

        if data:
            df_insert = pd.DataFrame(data)
            self.loader.load_df(df_insert, 'votos_secao')

    def ensure_municipio(self, row):
        uf = row['SG_UF']
        if pd.isna(uf) if not isinstance(uf, str) else not uf:
            logger.warning(f"ensure_municipio: SG_UF inválido para CD_MUNICIPIO={row.get('CD_MUNICIPIO')}")
            return

        if uf not in self.cache_estados:
            res = self.loader.execute_query(text("SELECT id FROM estados WHERE sigla=:sigla"), {'sigla': uf}).first()
            if res:
                self.cache_estados[uf] = res[0]
            else:
                self.loader.execute_query(text("INSERT INTO estados (sigla) VALUES (:sigla)"), {'sigla': uf})
                self.cache_estados[uf] = self.loader.execute_query(
                    text("SELECT id FROM estados WHERE sigla=:sigla"), {'sigla': uf}
                ).first()[0]

        estado_id = self.cache_estados[uf]
        cd_mun = str(row['CD_MUNICIPIO'])

        if (estado_id, cd_mun) not in self.cache_municipios:
            res = self.loader.execute_query(
                text("SELECT id FROM municipios WHERE estado_id=:eid AND codigo_tse=:cd"),
                {'eid': estado_id, 'cd': cd_mun}
            ).first()
            if res:
                self.cache_municipios[(estado_id, cd_mun)] = res[0]
            else:
                self.loader.execute_query(
                    text("INSERT INTO municipios (estado_id, codigo_tse, nome) VALUES (:eid, :cd, :nm)"),
                    {'eid': estado_id, 'cd': cd_mun, 'nm': row['NM_MUNICIPIO']}
                )
                self.cache_municipios[(estado_id, cd_mun)] = self.loader.execute_query(
                    text("SELECT id FROM municipios WHERE estado_id=:eid AND codigo_tse=:cd"),
                    {'eid': estado_id, 'cd': cd_mun}
                ).first()[0]

    def ensure_cargo(self, row):
        cd = str(row['CD_CARGO_PERGUNTA'])
        if not cd or cd == 'None':
            return

        if cd not in self.cache_cargos:
            res = self.loader.execute_query(text("SELECT id FROM cargos WHERE codigo=:cd"), {'cd': cd}).first()
            if res:
                self.cache_cargos[cd] = res[0]
            else:
                self.loader.execute_query(
                    text("INSERT INTO cargos (codigo, descricao) VALUES (:cd, :ds)"),
                    {'cd': cd, 'ds': row['DS_CARGO_PERGUNTA']}
                )
                self.cache_cargos[cd] = self.loader.execute_query(
                    text("SELECT id FROM cargos WHERE codigo=:cd"), {'cd': cd}
                ).first()[0]

    def ensure_partido(self, row):
        nr_raw = row['NR_PARTIDO']
        # Trata None/NaN que vem do cleaner (converte #NULO# → None)
        if nr_raw is None or (not isinstance(nr_raw, str) and pd.isna(nr_raw)):
            return
        nr = str(nr_raw).strip()
        if nr in ('-1', '#NULO#', 'None', ''):
            return

        if nr not in self.cache_partidos:
            res = self.loader.execute_query(text("SELECT id FROM partidos WHERE numero=:nr"), {'nr': nr}).first()
            if res:
                self.cache_partidos[nr] = res[0]
            else:
                self.loader.execute_query(
                    text("INSERT INTO partidos (numero, sigla, nome) VALUES (:nr, :sg, :nm)"),
                    {'nr': nr, 'sg': row['SG_PARTIDO'], 'nm': row['NM_PARTIDO']}
                )
                self.cache_partidos[nr] = self.loader.execute_query(
                    text("SELECT id FROM partidos WHERE numero=:nr"), {'nr': nr}
                ).first()[0]

    def ensure_candidato(self, eleicao_id, row):
        try:
            cd_cargo_raw = row['CD_CARGO_PERGUNTA']
            if cd_cargo_raw is None or (not isinstance(cd_cargo_raw, str) and pd.isna(cd_cargo_raw)):
                logger.warning(f"ensure_candidato: CD_CARGO_PERGUNTA ausente para NR_VOTAVEL={row.get('NR_VOTAVEL')}")
                return

            cargo_id = self.cache_cargos.get(str(cd_cargo_raw))
            if not cargo_id:
                logger.warning(f"ensure_candidato: cargo_id não encontrado no cache para CD_CARGO={cd_cargo_raw}, NR_VOTAVEL={row.get('NR_VOTAVEL')}")
                return

            # Resolver municipio_id (necessário para diferenciar candidatos a Prefeito entre municípios)
            cd_mun = str(row['CD_MUNICIPIO'])
            estado_ids = list(self.cache_estados.values())
            municipio_id = None
            for eid in estado_ids:
                municipio_id = self.cache_municipios.get((eid, cd_mun))
                if municipio_id:
                    break

            nr_partido_raw = row['NR_PARTIDO']
            nr_partido = str(nr_partido_raw).strip() if nr_partido_raw is not None else 'None'
            partido_id = self.cache_partidos.get(nr_partido)

            nr_votavel = str(row['NR_VOTAVEL'])
            key = (eleicao_id, municipio_id, cargo_id, nr_votavel)

            if key not in self.cache_candidatos:
                res = self.loader.execute_query(
                    text("SELECT id FROM candidatos WHERE eleicao_id=:eid AND municipio_id=:mid AND cargo_id=:cid AND nr_votavel=:nr"),
                    {'eid': eleicao_id, 'mid': municipio_id, 'cid': cargo_id, 'nr': nr_votavel}
                ).first()
                if res:
                    self.cache_candidatos[key] = res[0]
                else:
                    self.loader.execute_query(
                        text("INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (:eid, :mid, :cid, :pid, :nr, :nm)"),
                        {'eid': eleicao_id, 'mid': municipio_id, 'cid': cargo_id, 'pid': partido_id, 'nr': nr_votavel, 'nm': row['NM_VOTAVEL']}
                    )
                    self.cache_candidatos[key] = self.loader.execute_query(
                        text("SELECT id FROM candidatos WHERE eleicao_id=:eid AND municipio_id=:mid AND cargo_id=:cid AND nr_votavel=:nr"),
                        {'eid': eleicao_id, 'mid': municipio_id, 'cid': cargo_id, 'nr': nr_votavel}
                    ).first()[0]
        except Exception as e:
            logger.error(f"ensure_candidato: erro ao processar NR_VOTAVEL={row.get('NR_VOTAVEL')}, NM_VOTAVEL={row.get('NM_VOTAVEL')}: {e}", exc_info=True)

    def bulk_insert_consolidados(self, eleicao_id, agg_df, uf_metadata):
        data_to_insert = []

        if uf_metadata not in self.cache_estados:
            res = self.loader.execute_query(
                text("SELECT id FROM estados WHERE sigla=:sigla"), {'sigla': uf_metadata}
            ).first()
            if res:
                self.cache_estados[uf_metadata] = res[0]

        estado_id = self.cache_estados.get(uf_metadata)
        if not estado_id:
            return

        skipped = 0
        for _, row in agg_df.iterrows():
            cd_mun = str(row['CD_MUNICIPIO'])
            cd_cargo = str(row['CD_CARGO_PERGUNTA'])
            nr_votavel = str(row['NR_VOTAVEL'])

            mun_id = self.cache_municipios.get((estado_id, cd_mun))
            cargo_id = self.cache_cargos.get(cd_cargo)
            candidato_id = self.cache_candidatos.get((eleicao_id, mun_id, cargo_id, nr_votavel))

            if mun_id and cargo_id and candidato_id:
                data_to_insert.append({
                    'eleicao_id': eleicao_id,
                    'municipio_id': mun_id,
                    'cargo_id': cargo_id,
                    'candidato_id': candidato_id,
                    'total_votos': int(row['QT_VOTOS'])
                })
            else:
                skipped += 1
                logger.warning(
                    f"bulk_insert_consolidados: linha ignorada — "
                    f"CD_MUNICIPIO={cd_mun} (mun_id={mun_id}), "
                    f"CD_CARGO={cd_cargo} (cargo_id={cargo_id}), "
                    f"NR_VOTAVEL={nr_votavel} (candidato_id={candidato_id})"
                )

        if skipped > 0:
            logger.warning(f"bulk_insert_consolidados: total de {skipped} linhas ignoradas (eleicao_id={eleicao_id})")

        if data_to_insert:
            df_insert = pd.DataFrame(data_to_insert)
            self.loader.load_df(df_insert, 'votos_consolidados')
