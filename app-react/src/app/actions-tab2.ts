import { query } from '@/lib/db';

export async function resumoCandidatoMunicipio(ano: number, uf: string, cdMunicipio: string, cdCargo: string, nrVotavel: string) {
  const baseFilter = '"ANO_ELEICAO" = $1 AND "SG_UF" = $2 AND "CD_MUNICIPIO" = $3 AND "CD_CARGO_PERGUNTA" = $4';
  const params = [ano.toString(), uf, cdMunicipio, cdCargo];

  // 1. Rank and Votos do candidato
  const rankQuery = `
    WITH agg AS (
        SELECT "NR_VOTAVEL" AS nr, MAX("NM_VOTAVEL") AS nm,
               SUM("QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna
        WHERE ${baseFilter}
          AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
        GROUP BY 1
    )
    SELECT nr, nm, votos,
           RANK() OVER (ORDER BY votos DESC) AS posicao,
           (SELECT COUNT(*) FROM agg) AS total_cands
    FROM agg ORDER BY votos DESC
  `;
  const rankRes = await query(rankQuery, params);
  const minha = rankRes.rows.find(r => r.nr === nrVotavel);
  const votosCand = minha ? parseInt(minha.votos, 10) : 0;
  const posicao = minha ? parseInt(minha.posicao, 10) : null;
  const totalCands = rankRes.rows.length > 0 ? parseInt(rankRes.rows[0].total_cands, 10) : 0;

  // 2. Composição (válidos / brancos / nulos)
  const compQuery = `
    SELECT
      COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" NOT IN ('Branco', 'Nulo') THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS validos,
      COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Branco' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS brancos,
      COALESCE(SUM(CASE WHEN "DS_TIPO_VOTAVEL" = 'Nulo' THEN "QT_VOTOS"::bigint ELSE 0 END), 0) AS nulos
    FROM boletim_de_urna
    WHERE ${baseFilter}
  `;
  const compRes = await query(compQuery, params);
  const comp = compRes.rows[0];

  // 3. Secoes
  const secoesQuery = `
    SELECT SUM("QT_APTOS"::bigint) AS aptos,
           SUM("QT_COMPARECIMENTO"::bigint) AS comparec,
           SUM("QT_ABSTENCOES"::bigint) AS abstenc
    FROM (
      SELECT DISTINCT "NR_ZONA", "NR_SECAO",
                      "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES"
      FROM boletim_de_urna
      WHERE ${baseFilter}
    ) s
  `;
  const secoesRes = await query(secoesQuery, params);
  const secoes = secoesRes.rows[0];

  // 4. Liderança em locais
  const liderQuery = `
    WITH por_local AS (
      SELECT "NR_LOCAL_VOTACAO" AS lv, "NR_VOTAVEL" AS nr,
             SUM("QT_VOTOS"::bigint) AS votos
      FROM boletim_de_urna
      WHERE ${baseFilter}
        AND COALESCE("DS_TIPO_VOTAVEL", '') NOT IN ('Branco', 'Nulo')
      GROUP BY 1, 2
    ),
    rank_local AS (
      SELECT lv, nr, votos,
             RANK() OVER (PARTITION BY lv ORDER BY votos DESC) AS rk
      FROM por_local
    )
    SELECT COUNT(DISTINCT lv) FILTER (WHERE nr = $5 AND rk = 1) AS lideres,
           COUNT(DISTINCT lv) AS total_locais
    FROM rank_local
  `;
  const liderRes = await query(liderQuery, [...params, nrVotavel]);
  const lider = liderRes.rows[0];

  const aptos = secoes?.aptos ? parseInt(secoes.aptos, 10) : 0;
  const comparec = secoes?.comparec ? parseInt(secoes.comparec, 10) : 0;
  const abstenc = secoes?.abstenc ? parseInt(secoes.abstenc, 10) : 0;
  const validos = comp?.validos ? parseInt(comp.validos, 10) : 0;
  const brancos = comp?.brancos ? parseInt(comp.brancos, 10) : 0;
  const nulos = comp?.nulos ? parseInt(comp.nulos, 10) : 0;

  return {
    nm_candidato: minha ? minha.nm : null,
    votos_cand: votosCand,
    posicao,
    total_cands: totalCands,
    pct_validos: validos ? (votosCand / validos) * 100 : 0.0,
    validos,
    brancos,
    nulos,
    aptos,
    comparec,
    abstenc,
    pct_comparec: aptos ? (comparec / aptos) * 100 : 0.0,
    lideres: lider?.lideres ? parseInt(lider.lideres, 10) : 0,
    total_locais: lider?.total_locais ? parseInt(lider.total_locais, 10) : 0,
  };
}

export async function votosCandidatoPorMunicipio(ano: number, uf: string, cdCargo: string, nrVotavel: string, cdMunicipio?: string) {
  let where = 'b."ANO_ELEICAO" = $1 AND b."SG_UF" = $2 AND b."CD_CARGO_PERGUNTA" = $3 AND b."NR_VOTAVEL" = $4';
  const params: any[] = [ano.toString(), uf, cdCargo, nrVotavel];
  
  if (cdMunicipio) {
    where += ' AND b."CD_MUNICIPIO" = $5';
    params.push(cdMunicipio);
  }

  const sql = `
    SELECT b."CD_MUNICIPIO" AS cd,
           MAX(b."NM_MUNICIPIO") AS nm,
           MAX(m.cd_municipio_ibge) AS cd_ibge,
           SUM(b."QT_VOTOS"::bigint) AS votos
    FROM boletim_de_urna b
    LEFT JOIN municipio_tse_ibge m
      ON m.sg_uf = b."SG_UF"
     AND m.cd_municipio_tse = b."CD_MUNICIPIO"
    WHERE ${where}
    GROUP BY b."CD_MUNICIPIO"
    ORDER BY votos DESC
  `;

  const res = await query(sql, params);
  return res.rows.map(r => ({ ...r, votos: parseInt(r.votos, 10) }));
}
