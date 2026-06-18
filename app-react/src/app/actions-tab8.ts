import { query } from '@/lib/db';

export async function locaisDoMunicipio(ano: number, uf: string, cdMunicipio: string, cdCargo: string) {
  const sql = `
    SELECT "NR_LOCAL_VOTACAO" AS nr_local,
           MAX("DS_CARGO_PERGUNTA") AS cargo,
           SUM("QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna
    WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2
      AND "CD_MUNICIPIO" = $3 AND "CD_CARGO_PERGUNTA" = $4
    GROUP BY 1 ORDER BY 1
  `;
  const res = await query(sql, [ano.toString(), uf, cdMunicipio, cdCargo]);
  return res.rows.map(r => ({ nr_local: r.nr_local, cargo: r.cargo, votos: parseInt(r.votos, 10) }));
}

export async function nomeLocal(uf: string, cdMunicipio: string, nrLocal: string) {
  const sql = `
    SELECT MAX("NM_LOCAL_VOTACAO") AS nm
    FROM local_votacao
    WHERE "SG_UF" = $1
      AND "CD_MUNICIPIO" = $2 AND "NR_LOCAL_VOTACAO" = $3
  `;
  const res = await query(sql, [uf, cdMunicipio, nrLocal]);
  return res.rows.length > 0 ? res.rows[0].nm : null;
}

export async function topCandidatosNoLocal(ano: number, uf: string, cdMunicipio: string, cdCargo: string, nrLocal: string, top: number = 10) {
  const base = '"ANO_ELEICAO" = $1 AND "SG_UF" = $2 AND "CD_MUNICIPIO" = $3 AND "CD_CARGO_PERGUNTA" = $4 AND "NR_LOCAL_VOTACAO" = $5';
  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrLocal];

  const sqlRanking = `
    WITH agg AS (
      SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
             MAX("SG_PARTIDO") AS partido, SUM("QT_VOTOS"::bigint) AS votos
      FROM boletim_de_urna
      WHERE ${base} AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
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
      COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" NOT IN ('Branco', 'Nulo') THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
      COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
      COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
    FROM boletim_de_urna WHERE ${base}
  `;
  const resTotais = await query(sqlTotais, params);
  const totais = resTotais.rows[0];

  return {
    ranking: resRanking.rows.map(r => ({
      nr: r.nr,
      nm: r.nm,
      partido: r.partido,
      votos: parseInt(r.votos, 10),
      pct: parseFloat(r.pct)
    })),
    validos: parseInt(totais.validos, 10),
    brancos: parseInt(totais.brancos, 10),
    nulos: parseInt(totais.nulos, 10),
  };
}
