import { query } from '@/lib/db';
import { checkTableExists } from '@/app/actions-tab3';
import { LV_JOIN, LV_JOIN_CACHE_SALT } from '@/lib/local-votacao-join';

export async function comparativoVotosTerritorio(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nrs: string[],
  dimensao: string,
) {
  return comparativoVotosTerritorioCached(
    ano,
    uf,
    cdMunicipio,
    cdCargo,
    nrs,
    dimensao,
    LV_JOIN_CACHE_SALT,
  );
}

async function comparativoVotosTerritorioCached(
  ano: number,
  uf: string,
  cdMunicipio: string,
  cdCargo: string,
  nrs: string[],
  dimensao: string,
  _cacheSalt: string,
) {
  'use cache';

  if (nrs.length === 0) return [];

  const hasLocalVotacao = await checkTableExists('local_votacao');

  const secaoLocalSelect = hasLocalVotacao
    ? "MAX(COALESCE(NULLIF(TRIM(lv.\"NM_LOCAL_VOTACAO\"), ''), 'Local ' || b.\"NR_LOCAL_VOTACAO\")) AS nm_local,"
    : "MAX('Local ' || b.\"NR_LOCAL_VOTACAO\") AS nm_local,";

  const secaoJoin = hasLocalVotacao ? LV_JOIN : '';
  const secaoGroupExtra = hasLocalVotacao
    ? 'b."NR_ZONA", b."NR_SECAO", lv."NM_LOCAL_VOTACAO", b."NR_LOCAL_VOTACAO"'
    : 'b."NR_ZONA", b."NR_SECAO", b."NR_LOCAL_VOTACAO"';

  const dimCfg: Record<string, any> = {
    zona: {
      territorio_expr: 'b."NR_ZONA"',
      join_clause: '',
      group_extra: 'b."NR_ZONA"',
      local_select: 'NULL::text AS nm_local,',
    },
    secao: {
      territorio_expr: 'b."NR_ZONA" || \' · Seção \' || b."NR_SECAO"',
      join_clause: secaoJoin,
      group_extra: secaoGroupExtra,
      local_select: secaoLocalSelect,
    },
    bairro: {
      territorio_expr: "COALESCE(NULLIF(TRIM(lv.\"NM_BAIRRO\"), ''), '(sem bairro)')",
      join_clause: LV_JOIN,
      group_extra: 'lv."NM_BAIRRO"',
      local_select: 'NULL::text AS nm_local,',
    },
    local: {
      territorio_expr: "COALESCE(NULLIF(TRIM(lv.\"NM_LOCAL_VOTACAO\"), ''), 'Local ' || b.\"NR_LOCAL_VOTACAO\")",
      join_clause: LV_JOIN,
      group_extra: 'lv."NM_LOCAL_VOTACAO", b."NR_LOCAL_VOTACAO"',
      local_select: 'NULL::text AS nm_local,',
    },
  };

  const cfg = dimCfg[dimensao];
  if (!cfg) throw new Error(`Dimensão inválida: ${dimensao}`);

  const baseWhere = [
    'b."ANO_ELEICAO" = $1',
    'b."SG_UF" = $2',
    'b."CD_MUNICIPIO" = $3',
    'b."CD_CARGO_PERGUNTA" = $4',
    "COALESCE(b.\"DS_TIPO_VOTAVEL\", '') NOT IN ('Branco', 'Nulo')",
  ];

  const sql = `
    WITH all_votes AS (
        SELECT ${cfg.territorio_expr} AS territorio,
               ${cfg.local_select}
               b."NR_VOTAVEL" AS nr,
               MAX(b."NM_VOTAVEL") AS nm,
               SUM(b."QT_VOTOS"::bigint) AS votos
        FROM boletim_de_urna b
        ${cfg.join_clause}
        WHERE ${baseWhere.join(' AND ')}
        GROUP BY ${cfg.territorio_expr}, ${cfg.group_extra}, b."NR_VOTAVEL"
    ),
    with_total AS (
        SELECT territorio, nm_local, nr, nm, votos,
               SUM(votos) OVER (PARTITION BY territorio) AS total_territorio
        FROM all_votes
    )
    SELECT territorio, nm_local, nr, nm, votos, total_territorio
    FROM with_total
    WHERE nr = ANY($5::text[])
    ORDER BY territorio, votos DESC
  `;

  const params = [ano.toString(), uf, cdMunicipio, cdCargo, nrs];
  const res = await query(sql, params);

  return res.rows.map((r) => ({
    territorio: r.territorio,
    nm_local: r.nm_local,
    nr: r.nr,
    nm: r.nm,
    votos: parseInt(r.votos, 10),
    total_territorio: parseInt(r.total_territorio, 10),
  }));
}
