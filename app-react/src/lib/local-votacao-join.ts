/** JOIN entre boletim_de_urna (alias b) e local_votacao (alias lv). */
export const LV_JOIN = `
    JOIN local_votacao lv
      ON lv."AA_ELEICAO" = b."ANO_ELEICAO"
     AND lv."NR_TURNO" = b."NR_TURNO"
     AND lv."SG_UF" = b."SG_UF"
     AND lv."CD_MUNICIPIO" = b."CD_MUNICIPIO"
     AND lv."NR_ZONA" = b."NR_ZONA"
     AND lv."NR_SECAO" = b."NR_SECAO"
`;

/** Incrementar ao mudar a lógica SQL para invalidar entradas antigas do "use cache". */
export const LV_JOIN_CACHE_SALT = 'lv-join-v2';
