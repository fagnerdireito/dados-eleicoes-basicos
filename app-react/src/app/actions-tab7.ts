'use cache';

import { query } from '@/lib/db';

export async function sinteseTerritorial(ano: number, uf: string, cdMunicipio: string, cdCargo: string) {
  const sql = `
    WITH por_local AS (
      SELECT "NR_LOCAL_VOTACAO" AS lv, "NR_VOTAVEL" AS nr,
             MAX("NM_VOTAVEL") AS nm, MAX("SG_PARTIDO") AS partido,
             SUM("QT_VOTOS"::bigint) AS votos
      FROM boletim_de_urna
      WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2
        AND "CD_MUNICIPIO" = $3 AND "CD_CARGO_PERGUNTA" = $4
        AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
      GROUP BY 1, 2
    ),
    rk AS (
      SELECT lv, nr, nm, partido, votos,
             RANK() OVER (PARTITION BY lv ORDER BY votos DESC) AS pos
      FROM por_local
    )
    SELECT nr, nm, partido, COUNT(DISTINCT lv) AS locais
    FROM rk WHERE pos = 1
    GROUP BY 1, 2, 3
    ORDER BY locais DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo];
  const res = await query(sql, params);

  return res.rows.map(r => ({
    nr: r.nr,
    nm: r.nm,
    partido: r.partido,
    locais: parseInt(r.locais, 10)
  }));
}

export async function sinteseTerritorialUf(ano: number, uf: string, cdCargo: string) {
  const sql = `
    WITH agg AS (
      SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
             MAX("SG_PARTIDO") AS partido, SUM("QT_VOTOS"::bigint) AS votos
      FROM boletim_de_urna
      WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2
        AND "CD_CARGO_PERGUNTA" = $3
        AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
      GROUP BY 1
    ),
    tot AS (SELECT SUM(votos) AS t FROM agg)
    SELECT nr, nm, partido, votos,
           (votos::float / NULLIF((SELECT t FROM tot), 0) * 100) AS pct
    FROM agg ORDER BY votos DESC
  `;

  const params = [ano.toString(), uf, cdCargo];
  const res = await query(sql, params);

  return res.rows.map(r => ({
    nr: r.nr,
    nm: r.nm,
    partido: r.partido,
    votos: parseInt(r.votos, 10),
    pct: parseFloat(r.pct)
  }));
}
