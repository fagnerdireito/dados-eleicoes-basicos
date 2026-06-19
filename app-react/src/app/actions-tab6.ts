'use cache';

import { query } from '@/lib/db';

export async function rankingMunicipio(ano: number, uf: string, cdMunicipio: string, cdCargo: string, top: number = 10) {
  const sql = `
    WITH agg AS (
      SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
             MAX("SG_PARTIDO") AS partido, SUM("QT_VOTOS"::bigint) AS votos
      FROM boletim_de_urna
      WHERE "ANO_ELEICAO" = $1 AND "SG_UF" = $2
        AND "CD_MUNICIPIO" = $3 AND "CD_CARGO_PERGUNTA" = $4
        AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
      GROUP BY 1
    ),
    tot AS (SELECT SUM(votos) AS t FROM agg)
    SELECT nr, nm, partido, votos,
           (votos::float / NULLIF((SELECT t FROM tot), 0) * 100) AS pct
    FROM agg ORDER BY votos DESC LIMIT $5
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, top];
  const res = await query(sql, params);
  
  return res.rows.map(r => ({
    nr: r.nr,
    nm: r.nm,
    partido: r.partido,
    votos: parseInt(r.votos, 10),
    pct: parseFloat(r.pct)
  }));
}
