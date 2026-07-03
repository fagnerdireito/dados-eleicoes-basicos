'use cache';

import { query } from '@/lib/db';

// Esconde registros com nome corrompido (aspas duplas ou ponto-e-vírgula,
// resíduos do parsing do CSV do TSE). Filtro feito no código para evitar
// LIKE no banco, que pode ficar lento em tabelas grandes.
function nomeValido(nm: unknown): boolean {
  if (typeof nm !== 'string') return false;
  return !nm.includes('"') && !nm.includes(';');
}

export async function rankingMunicipio(ano: number, uf: string, cdMunicipio: string, cdCargo: string, top: number = 10) {
  // Busca uma margem extra além do top pedido, pois alguns registros serão
  // descartados pelo filtro de nome aplicado no código abaixo.
  const limiteBusca = top + 20;

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

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, limiteBusca];
  const res = await query(sql, params);

  return res.rows
    .filter(r => nomeValido(r.nm))
    .slice(0, top)
    .map(r => ({
      nr: r.nr,
      nm: r.nm,
      partido: r.partido,
      votos: parseInt(r.votos, 10),
      pct: parseFloat(r.pct)
    }));
}
