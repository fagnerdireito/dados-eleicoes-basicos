import { query } from '@/lib/db';
import { checkTableExists } from '@/app/actions-tab3';
import { LV_JOIN, LV_JOIN_CACHE_SALT } from '@/lib/local-votacao-join';

export async function votosPorBairro(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nrVotavel: string,
) {
  return votosPorBairroCached(ano, uf, cdMunicipio, cdCargo, nrVotavel, LV_JOIN_CACHE_SALT);
}

async function votosPorBairroCached(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nrVotavel: string,
  _cacheSalt: string,
) {
  'use cache';

  const hasLocalVotacao = await checkTableExists('local_votacao');
  if (!hasLocalVotacao) return [];

  const sql = `
    SELECT COALESCE(NULLIF(TRIM(lv."NM_BAIRRO"), ''), '(sem bairro)') AS bairro,
           MAX(b."NM_VOTAVEL") AS nm_votavel,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    ${LV_JOIN}
    WHERE b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2
      AND b."CD_MUNICIPIO" = $3 AND b."CD_CARGO_PERGUNTA" = $4
      AND b."NR_VOTAVEL" = $5
    GROUP BY 1
    ORDER BY votos DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrVotavel];
  const res = await query(sql, params);

  return res.rows.map((r) => ({
    bairro: r.bairro,
    nm_votavel: r.nm_votavel,
    votos: parseInt(r.votos, 10),
  }));
}

export async function votosPorLocalCandidato(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nrVotavel: string,
) {
  return votosPorLocalCandidatoCached(ano, uf, cdMunicipio, cdCargo, nrVotavel, LV_JOIN_CACHE_SALT);
}

async function votosPorLocalCandidatoCached(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nrVotavel: string,
  _cacheSalt: string,
) {
  'use cache';

  const hasLocalVotacao = await checkTableExists('local_votacao');
  if (!hasLocalVotacao) return [];

  const sql = `
    SELECT COALESCE(NULLIF(TRIM(lv."NM_LOCAL_VOTACAO"), ''), 'Local ' || b."NR_LOCAL_VOTACAO") AS local,
           MAX(b."NM_VOTAVEL") AS nm_votavel,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    ${LV_JOIN}
    WHERE b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2
      AND b."CD_MUNICIPIO" = $3 AND b."CD_CARGO_PERGUNTA" = $4
      AND b."NR_VOTAVEL" = $5
    GROUP BY b."NR_LOCAL_VOTACAO", lv."NM_LOCAL_VOTACAO"
    ORDER BY votos DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrVotavel];
  const res = await query(sql, params);

  return res.rows.map((r) => ({
    local: r.local,
    nm_votavel: r.nm_votavel,
    votos: parseInt(r.votos, 10),
  }));
}
