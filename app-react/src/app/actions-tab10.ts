'use cache';

import { query } from '@/lib/db';
import { checkTableExists } from '@/app/actions-tab3';

export async function votosPorBairro(ano: number, uf: string, cdMunicipio: string, cdCargo: string, nrVotavel: string) {
  const hasLocalVotacao = await checkTableExists('local_votacao');
  if (!hasLocalVotacao) return [];

  const join = `
    JOIN local_votacao lv
      ON lv."SG_UF" = b."SG_UF"
     AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
     AND lv."NR_ZONA" = b."NR_ZONA"
     AND lv."NR_SECAO" = b."NR_SECAO"
  `;

  const sql = `
    SELECT COALESCE(NULLIF(TRIM(lv."NM_BAIRRO"), ''), '(sem bairro)') AS bairro,
           MAX(b."NM_VOTAVEL") AS nm_votavel,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    ${join}
    WHERE b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2
      AND b."CD_MUNICIPIO" = $3 AND b."CD_CARGO_PERGUNTA" = $4
      AND b."NR_VOTAVEL" = $5
    GROUP BY 1
    ORDER BY votos DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrVotavel];
  const res = await query(sql, params);
  
  return res.rows.map(r => ({
    bairro: r.bairro,
    nm_votavel: r.nm_votavel,
    votos: parseInt(r.votos, 10)
  }));
}

export async function votosPorLocalCandidato(ano: number, uf: string, cdMunicipio: string, cdCargo: string, nrVotavel: string) {
  const hasLocalVotacao = await checkTableExists('local_votacao');
  if (!hasLocalVotacao) return [];

  const join = `
    JOIN local_votacao lv
      ON lv."SG_UF" = b."SG_UF"
     AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
     AND lv."NR_ZONA" = b."NR_ZONA"
     AND lv."NR_SECAO" = b."NR_SECAO"
  `;

  const sql = `
    SELECT lv."NM_LOCAL_VOTACAO" AS local,
           MAX(b."NM_VOTAVEL") AS nm_votavel,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    ${join}
    WHERE b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2
      AND b."CD_MUNICIPIO" = $3 AND b."CD_CARGO_PERGUNTA" = $4
      AND b."NR_VOTAVEL" = $5
    GROUP BY 1
    ORDER BY votos DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrVotavel];
  const res = await query(sql, params);
  
  return res.rows.map(r => ({
    local: r.local,
    nm_votavel: r.nm_votavel,
    votos: parseInt(r.votos, 10)
  }));
}
