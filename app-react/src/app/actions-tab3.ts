'use cache';

import { query } from '@/lib/db';

export async function checkTableExists(tableName: string) {
  const res = await query(`
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = $1
    LIMIT 1
  `, [tableName]);
  return res.rows.length > 0;
}

export async function turnoutUf(ano: number, uf: string, cdMunicipio?: string) {
  let where = '"ANO_ELEICAO" = $1 AND "SG_UF" = $2';
  const params: any[] = [ano.toString(), uf];

  if (cdMunicipio) {
    where += ' AND "CD_MUNICIPIO" = $3';
    params.push(cdMunicipio);
  }

  const sql = `
    SELECT SUM("QT_APTOS"::bigint)          AS aptos,
           SUM("QT_COMPARECIMENTO"::bigint) AS comparec,
           SUM("QT_ABSTENCOES"::bigint)     AS abstenc
    FROM (
      SELECT DISTINCT "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO",
                      "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES"
      FROM boletim_de_urna
      WHERE ${where}
    ) s
  `;

  const res = await query(sql, params);
  const row = res.rows[0];

  const aptos = row?.aptos ? parseInt(row.aptos, 10) : 0;
  const comparec = row?.comparec ? parseInt(row.comparec, 10) : 0;
  const abstenc = row?.abstenc ? parseInt(row.abstenc, 10) : 0;

  return {
    aptos,
    comparec,
    abstenc,
    pct_comparec: aptos ? (comparec / aptos) * 100 : 0.0,
    pct_abstenc: aptos ? (abstenc / aptos) * 100 : 0.0,
  };
}

export async function perfilFaixaEtaria(ano: number, uf: string, cdMunicipio?: string) {
  const hasTable = await checkTableExists('perfil_eleitorado');
  if (!hasTable) return null;

  let where = '"ANO_ELEICAO" = $1 AND "SG_UF" = $2';
  const params: any[] = [ano.toString(), uf];

  if (cdMunicipio) {
    where += ' AND "CD_MUNICIPIO" = $3';
    params.push(cdMunicipio);
  }

  const sql = `
    SELECT COALESCE(NULLIF("DS_FAIXA_ETARIA", '#NULO#'), 'Não informado') AS label,
           SUM(NULLIF("QT_ELEITORES_PERFIL", '')::bigint) AS eleitores
    FROM perfil_eleitorado
    WHERE ${where}
    GROUP BY 1
    ORDER BY MIN(NULLIF("CD_FAIXA_ETARIA", '')::int) NULLS LAST
  `;

  const res = await query(sql, params);
  return res.rows.map(r => ({ label: r.label, eleitores: parseInt(r.eleitores, 10) }));
}

export async function perfilEscolaridade(ano: number, uf: string, cdMunicipio?: string) {
  const hasTable = await checkTableExists('perfil_eleitorado');
  if (!hasTable) return null;

  let where = '"ANO_ELEICAO" = $1 AND "SG_UF" = $2';
  const params: any[] = [ano.toString(), uf];

  if (cdMunicipio) {
    where += ' AND "CD_MUNICIPIO" = $3';
    params.push(cdMunicipio);
  }

  const sql = `
    SELECT COALESCE(NULLIF("DS_GRAU_ESCOLARIDADE", '#NULO#'), 'Não informado') AS label,
           SUM(NULLIF("QT_ELEITORES_PERFIL", '')::bigint) AS eleitores
    FROM perfil_eleitorado
    WHERE ${where}
    GROUP BY 1
    ORDER BY eleitores DESC
  `;

  const res = await query(sql, params);
  return res.rows.map(r => ({ label: r.label, eleitores: parseInt(r.eleitores, 10) }));
}
