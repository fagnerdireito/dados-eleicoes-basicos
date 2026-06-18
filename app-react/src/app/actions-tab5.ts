import { query } from '@/lib/db';
import { checkTableExists } from '@/app/actions-tab3';

export async function votosCandidatoPorLocal(ano: number, uf: string, cdMunicipio: string, cdCargo: string, nrVotavel: string) {
  const hasTable = await checkTableExists('local_votacao');
  if (!hasTable) return null;

  const join = `
    JOIN local_votacao lv
      ON lv."SG_UF" = b."SG_UF"
     AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
     AND lv."NR_ZONA" = b."NR_ZONA"
     AND lv."NR_SECAO" = b."NR_SECAO"
  `;

  const sql = `
    SELECT lv."NM_LOCAL_VOTACAO" AS nm_local,
           lv."NM_BAIRRO" AS bairro,
           lv."NR_LATITUDE"::float AS lat,
           lv."NR_LONGITUDE"::float AS lng,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    ${join}
    WHERE b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2
      AND b."CD_MUNICIPIO" = $3
      AND b."CD_CARGO_PERGUNTA" = $4 AND b."NR_VOTAVEL" = $5
      AND lv."NR_LATITUDE" NOT IN ('-1', '') AND lv."NR_LATITUDE" IS NOT NULL
    GROUP BY 1, 2, 3, 4
    ORDER BY votos DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrVotavel];
  const res = await query(sql, params);
  
  return res.rows.map(r => ({
    nm_local: r.nm_local,
    bairro: r.bairro,
    lat: r.lat,
    lng: r.lng,
    votos: parseInt(r.votos, 10)
  }));
}
