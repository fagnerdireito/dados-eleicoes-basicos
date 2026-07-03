import { query } from '@/lib/db';
import { checkTableExists } from '@/app/actions-tab3';
import { LV_JOIN, LV_JOIN_CACHE_SALT } from '@/lib/local-votacao-join';

// Expressão do nome do local. Com a tabela local_votacao, resolve o nome real
// via join por zona/seção (mesma lógica das abas 9 e 10). Sem ela, cai no
// rótulo "Local <número>". O número do local (NR_LOCAL_VOTACAO) NÃO é único no
// município — ele se repete entre zonas — por isso o local é identificado pelo
// NOME, não pelo número.
const LOCAL_NOME_EXPR_JOIN =
  "COALESCE(NULLIF(TRIM(lv.\"NM_LOCAL_VOTACAO\"), ''), 'Local ' || b.\"NR_LOCAL_VOTACAO\")";
const LOCAL_NOME_EXPR_FALLBACK = "'Local ' || b.\"NR_LOCAL_VOTACAO\"";

export async function locaisDoMunicipio(ano: number, uf: string, cdMunicipio: string, cdCargo: string) {
  return locaisDoMunicipioCached(ano, uf, cdMunicipio, cdCargo, LV_JOIN_CACHE_SALT);
}

async function locaisDoMunicipioCached(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  _cacheSalt: string,
) {
  'use cache';

  const hasLocalVotacao = await checkTableExists('local_votacao');
  const nomeExpr = hasLocalVotacao ? LOCAL_NOME_EXPR_JOIN : LOCAL_NOME_EXPR_FALLBACK;
  const join = hasLocalVotacao ? LV_JOIN : '';

  const sql = `
    SELECT ${nomeExpr} AS nome,
           MAX(b."NR_LOCAL_VOTACAO") AS nr_local,
           MAX(b."DS_CARGO_PERGUNTA") AS cargo,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    ${join}
    WHERE b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2
      AND b."CD_MUNICIPIO" = $3 AND b."CD_CARGO_PERGUNTA" = $4
    GROUP BY nome
    ORDER BY nome
  `;
  const res = await query(sql, [ano.toString(), uf, cdMunicipio, cdCargo]);
  return res.rows.map((r) => ({
    nome: r.nome,
    nr_local: r.nr_local,
    cargo: r.cargo,
    votos: parseInt(r.votos, 10),
  }));
}

export async function topCandidatosNoLocal(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nomeLocal: string,
  top: number = 10,
) {
  return topCandidatosNoLocalCached(ano, uf, cdMunicipio, cdCargo, nomeLocal, top, LV_JOIN_CACHE_SALT);
}

async function topCandidatosNoLocalCached(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nomeLocal: string,
  top: number,
  _cacheSalt: string,
) {
  'use cache';

  const hasLocalVotacao = await checkTableExists('local_votacao');
  const nomeExpr = hasLocalVotacao ? LOCAL_NOME_EXPR_JOIN : LOCAL_NOME_EXPR_FALLBACK;
  const join = hasLocalVotacao ? LV_JOIN : '';

  // Filtra pelo NOME do local (agrega todas as zonas/seções daquele local).
  const base = `b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2 AND b."CD_MUNICIPIO" = $3 AND b."CD_CARGO_PERGUNTA" = $4 AND ${nomeExpr} = $5`;
  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nomeLocal];

  const sqlRanking = `
    WITH agg AS (
      SELECT b."NR_VOTAVEL" AS nr, MAX(b."NM_VOTAVEL") AS nm,
             MAX(b."SG_PARTIDO") AS partido, SUM(b."QT_VOTOS"::bigint) AS votos
      FROM boletim_de_urna b
      ${join}
      WHERE ${base} AND COALESCE(b."DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
      GROUP BY 1
    ),
    tot AS (SELECT SUM(votos) AS t FROM agg)
    SELECT nr, nm, partido, votos,
           votos::float / NULLIF((SELECT t FROM tot), 0) * 100 AS pct
    FROM agg ORDER BY votos DESC LIMIT $6
  `;
  const resRanking = await query(sqlRanking, [...params, top]);

  const sqlTotais = `
    SELECT
      COALESCE(SUM(CASE WHEN b."DS_TIPO_VOTAVEL" NOT IN ('Branco', 'Nulo') THEN b."QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
      COALESCE(SUM(CASE WHEN b."DS_TIPO_VOTAVEL" = 'Branco' THEN b."QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
      COALESCE(SUM(CASE WHEN b."DS_TIPO_VOTAVEL" = 'Nulo' THEN b."QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
    FROM boletim_de_urna b
    ${join}
    WHERE ${base}
  `;
  const resTotais = await query(sqlTotais, params);
  const totais = resTotais.rows[0];

  return {
    ranking: resRanking.rows.map((r) => ({
      nr: r.nr,
      nm: r.nm,
      partido: r.partido,
      votos: parseInt(r.votos, 10),
      pct: parseFloat(r.pct),
    })),
    validos: parseInt(totais.validos, 10),
    brancos: parseInt(totais.brancos, 10),
    nulos: parseInt(totais.nulos, 10),
  };
}
