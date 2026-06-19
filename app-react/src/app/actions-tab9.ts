'use cache';

import { query } from '@/lib/db';
import { checkTableExists } from '@/app/actions-tab3';

export async function comparativoVotosTerritorio(ano: number, uf: string, cdMunicipio: string, cdCargo: string, nrs: string[], dimensao: string) {
  if (nrs.length === 0) return [];

  const hasLocalVotacao = await checkTableExists('local_votacao');
  
  let secaoLocalSelect = hasLocalVotacao 
    ? "MAX(COALESCE(NULLIF(TRIM(lv.\"NM_LOCAL_VOTACAO\"), ''), 'Local ' || b.\"NR_LOCAL_VOTACAO\")) AS nm_local,"
    : "MAX('Local ' || b.\"NR_LOCAL_VOTACAO\") AS nm_local,";
    
  let lvJoin = `
    JOIN local_votacao lv
      ON lv."SG_UF" = b."SG_UF"
     AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
     AND lv."NR_ZONA" = b."NR_ZONA"
     AND lv."NR_SECAO" = b."NR_SECAO"
  `;

  let secaoJoin = hasLocalVotacao ? lvJoin : "";
  let secaoGroupExtra = hasLocalVotacao 
    ? 'b."NR_ZONA", b."NR_SECAO", lv."NM_LOCAL_VOTACAO", b."NR_LOCAL_VOTACAO"'
    : 'b."NR_ZONA", b."NR_SECAO", b."NR_LOCAL_VOTACAO"';

  const dimCfg: Record<string, any> = {
    "zona": {
      "territorio_expr": 'b."NR_ZONA"',
      "join_clause": "",
      "group_extra": 'b."NR_ZONA"',
      "local_select": "NULL::text AS nm_local,",
    },
    "secao": {
      "territorio_expr": 'b."NR_ZONA" || \' · Seção \' || b."NR_SECAO"',
      "join_clause": secaoJoin,
      "group_extra": secaoGroupExtra,
      "local_select": secaoLocalSelect,
    },
    "bairro": {
      "territorio_expr": "COALESCE(NULLIF(TRIM(lv.\"NM_BAIRRO\"), ''), '(sem bairro)')",
      "join_clause": lvJoin,
      "group_extra": 'lv."NM_BAIRRO"',
      "local_select": "NULL::text AS nm_local,",
    },
    "local": {
      "territorio_expr": "COALESCE(NULLIF(TRIM(lv.\"NM_LOCAL_VOTACAO\"), ''), 'Local ' || b.\"NR_LOCAL_VOTACAO\")",
      "join_clause": lvJoin,
      "group_extra": 'lv."NM_LOCAL_VOTACAO", b."NR_LOCAL_VOTACAO"',
      "local_select": "NULL::text AS nm_local,",
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

  // Pass array to PostgreSQL using ANY($5)
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
  
  return res.rows.map(r => ({
    territorio: r.territorio,
    nm_local: r.nm_local,
    nr: r.nr,
    nm: r.nm,
    votos: parseInt(r.votos, 10),
    total_territorio: parseInt(r.total_territorio, 10)
  }));
}
