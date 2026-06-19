'use cache';

// Consultas usadas pelos filtros globais (ano/UF/município/cargo/candidato).
// O "use cache" a nível de arquivo faz cada função reutilizar o resultado
// quando recebe os mesmos argumentos, evitando novas idas ao banco para o
// mesmo filtro. O perfil padrão de cacheLife (definido em next.config.ts)
// mantém os dados eleitorais em cache por muito tempo.
import { query } from '@/lib/db';

export async function checkUsaCatalogoFiltros() {
  const res = await query(`
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'catalogo_boletim'
    LIMIT 1
  `);
  return res.rows.length > 0;
}

export async function listarAnos() {
  const usaCatalogo = await checkUsaCatalogoFiltros();
  let res;
  if (usaCatalogo) {
    res = await query("SELECT DISTINCT ano::int AS ano FROM catalogo_boletim ORDER BY 1");
  } else {
    res = await query('SELECT DISTINCT "ANO_ELEICAO"::int AS ano FROM boletim_de_urna ORDER BY 1');
  }
  return res.rows.map(r => r.ano);
}

export async function listarUfs(ano: number) {
  const usaCatalogo = await checkUsaCatalogoFiltros();
  let res;
  if (usaCatalogo) {
    res = await query(
      "SELECT DISTINCT sg_uf AS uf FROM catalogo_boletim WHERE ano = $1 ORDER BY 1",
      [ano]
    );
  } else {
    res = await query(
      'SELECT DISTINCT "SG_UF" AS uf FROM boletim_de_urna WHERE "ANO_ELEICAO" = $1 ORDER BY 1',
      [ano.toString()]
    );
  }
  return res.rows.map(r => r.uf);
}

export async function listarMunicipios(ano: number, uf: string) {
  const usaCatalogo = await checkUsaCatalogoFiltros();
  let res;
  if (usaCatalogo) {
    res = await query(
      "SELECT DISTINCT cd_municipio AS cd, nm_municipio AS nm FROM catalogo_boletim WHERE ano = $1 AND sg_uf = $2 ORDER BY 2",
      [ano, uf]
    );
  } else {
    res = await query(
      'SELECT DISTINCT "CD_MUNICIPIO" AS cd, "NM_MUNICIPIO" AS nm FROM boletim_de_urna WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2 ORDER BY 2',
      [ano.toString(), uf]
    );
  }
  return res.rows;
}

export async function listarCargos(ano: number, uf: string, cdMunicipio?: string) {
  const usaCatalogo = await checkUsaCatalogoFiltros();
  let res;
  if (usaCatalogo) {
    let sql = "SELECT DISTINCT cd_cargo AS cd, ds_cargo AS ds FROM catalogo_boletim WHERE ano = $1 AND sg_uf = $2";
    const params: any[] = [ano, uf];
    if (cdMunicipio) {
      sql += " AND cd_municipio = $3";
      params.push(cdMunicipio);
    }
    sql += " ORDER BY 2";
    res = await query(sql, params);
  } else {
    let sql = 'SELECT DISTINCT "CD_CARGO_PERGUNTA" AS cd, "DS_CARGO_PERGUNTA" AS ds FROM boletim_de_urna WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2';
    const params: any[] = [ano.toString(), uf];
    if (cdMunicipio) {
      sql += ' AND "CD_MUNICIPIO" = $3';
      params.push(cdMunicipio);
    }
    sql += " ORDER BY 1";
    res = await query(sql, params);
  }
  return res.rows;
}

export async function listarCandidatos(ano: number, uf: string, cdCargo: string, cdMunicipio?: string, limit = 200) {
  const usaCatalogo = await checkUsaCatalogoFiltros();
  let res;
  if (usaCatalogo) {
    let sql = "SELECT nr_votavel AS nr, MAX(nm_votavel) AS nm, MAX(sg_partido) AS sg_partido, SUM(total_votos) AS votos FROM catalogo_boletim WHERE ano = $1 AND sg_uf = $2 AND cd_cargo = $3";
    const params: any[] = [ano, uf, cdCargo];
    if (cdMunicipio) {
      sql += " AND cd_municipio = $4";
      params.push(cdMunicipio);
    }
    sql += " GROUP BY nr_votavel ORDER BY MAX(nm_votavel) ASC";
    res = await query(sql, params);
  } else {
    let sql = 'SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm, MAX("SG_PARTIDO") AS sg_partido, SUM("QT_VOTOS"::bigint) AS votos FROM boletim_de_urna WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2 AND "CD_CARGO_PERGUNTA" = $3 AND COALESCE("DS_TIPO_VOTAVEL", \'\') NOT IN (\'Branco\', \'Nulo\')';
    const params: any[] = [ano.toString(), uf, cdCargo];
    if (cdMunicipio) {
      sql += ' AND "CD_MUNICIPIO" = $4';
      params.push(cdMunicipio);
    }
    sql += ` GROUP BY 1 ORDER BY MAX("NM_VOTAVEL") ASC LIMIT ${limit}`;
    res = await query(sql, params);
  }
  return res.rows;
}
