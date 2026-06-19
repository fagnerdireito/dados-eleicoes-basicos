import { listarMunicipios } from '@/lib/queries-filtros';

// O boletim do TSE não tem flag de "capital", e a capital nem sempre é o maior
// município da UF. Por isso usamos um mapa fixo UF -> nome da capital e
// procuramos o código correspondente entre os municípios da eleição.
const CAPITAIS: Record<string, string> = {
  AC: 'RIO BRANCO',
  AL: 'MACEIO',
  AP: 'MACAPA',
  AM: 'MANAUS',
  BA: 'SALVADOR',
  CE: 'FORTALEZA',
  DF: 'BRASILIA',
  ES: 'VITORIA',
  GO: 'GOIANIA',
  MA: 'SAO LUIS',
  MT: 'CUIABA',
  MS: 'CAMPO GRANDE',
  MG: 'BELO HORIZONTE',
  PA: 'BELEM',
  PB: 'JOAO PESSOA',
  PR: 'CURITIBA',
  PE: 'RECIFE',
  PI: 'TERESINA',
  RJ: 'RIO DE JANEIRO',
  RN: 'NATAL',
  RO: 'PORTO VELHO',
  RR: 'BOA VISTA',
  RS: 'PORTO ALEGRE',
  SC: 'FLORIANOPOLIS',
  SP: 'SAO PAULO',
  SE: 'ARACAJU',
  TO: 'PALMAS',
};

function normalizar(valor: string) {
  return valor
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toUpperCase()
    .trim();
}

// Retorna o código/nome da capital da UF para o ano informado, ou null se a UF
// for desconhecida ou a capital não estiver entre os municípios da eleição.
export async function resolverCapital(
  anoNum: number,
  uf: string,
): Promise<{ cd: string; nm: string } | null> {
  const nomeCapital = CAPITAIS[uf.toUpperCase()];
  if (!nomeCapital) return null;

  const municipios = await listarMunicipios(anoNum, uf);
  const match = municipios.find((m) => normalizar(String(m.nm)) === nomeCapital);
  if (!match) return null;

  return { cd: String(match.cd), nm: String(match.nm) };
}
