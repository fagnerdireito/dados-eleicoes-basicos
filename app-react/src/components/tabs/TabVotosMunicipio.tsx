import { votosCandidatoPorLocal } from '@/app/actions-tab5';
import { votosCandidatoPorMunicipio } from '@/app/actions-tab2';
import { listarCandidatos, listarMunicipios } from '@/app/actions';
import { MapMunicipioDynamicWrapper } from './charts/MapMunicipioDynamic';
import { MapPin, Vote } from 'lucide-react';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

export async function TabVotosMunicipio({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato || !municipio) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Município, Cargo e Candidato nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);

  const municipios = await listarMunicipios(anoNum, uf);
  const municipioNome = municipios.find((m) => String(m.cd) === municipio)?.nm ?? municipio;

  const candidatos = await listarCandidatos(anoNum, uf, cargo, municipio);
  const c = candidatos.find((item) => String(item.nr) === candidato);
  const candidatoLabel = c ? `${c.nm}${c.sg_partido ? ` (${c.sg_partido})` : ''}` : candidato;

  const munVotos = await votosCandidatoPorMunicipio(anoNum, uf, cargo, candidato, municipio);
  const cdIbge = munVotos[0]?.cd_ibge?.toString();

  const data = await votosCandidatoPorLocal(anoNum, uf, municipio, cargo, candidato);

  if (!data) {
    return <div className="rounded-lg bg-blue-50 p-4 text-blue-800">Tabela `local_votacao` não encontrada.</div>;
  }

  if (data.length === 0) {
    return <div className="rounded-lg bg-blue-50 p-4 text-blue-800">Sem coordenadas válidas para os locais deste município.</div>;
  }

  const totalVotos = data.reduce((acc, curr) => acc + curr.votos, 0);

  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="flex flex-wrap items-center gap-3 text-2xl font-bold text-[#0b2545]">
          Onde estão os votos no município
          <span className="recorte-pill chart-print-bg inline-flex items-center gap-2 rounded-full bg-indigo-600/70 px-4 py-1.5 text-base font-semibold text-white">
            {municipioNome}
            <MapPin className="h-4 w-4 shrink-0" strokeWidth={2.25} />
          </span>
        </h2>
        <p className="text-gray-500">
          Votação de {candidatoLabel} por local em {municipioNome} ({ano})
        </p>
      </div>

      <div className="votos-municipio-layout flex flex-col gap-6 md:flex-row md:items-start print:flex-col print:gap-4">
        <div className="votos-municipio-map w-full md:w-2/3">
          <MapMunicipioDynamicWrapper uf={uf} cdIbge={cdIbge} municipioNome={municipioNome} data={data} />
          <p className="mt-2 text-sm text-gray-400 print:hidden">
            Mapa coroplético do município com marcadores nos locais de votação (tamanho conforme os votos). Use a roda do mouse ou os botões para zoom; arraste para mover.
          </p>
        </div>

        <div className="votos-municipio-list flex w-full flex-col rounded-lg border border-gray-100 bg-white p-4 shadow-lg chart-print-bg md:w-1/3 print:mt-10 print:w-full print:shadow-none">
          <h4 className="mb-3 font-semibold text-[#0b2545]">Top locais (votos do candidato)</h4>
          <span className="recorte-pill chart-print-bg mb-3 inline-flex w-fit items-center gap-2 rounded-full bg-indigo-600/70 px-4 py-1.5 text-sm font-semibold text-white">
            Total de votos no município · {fmtInt(totalVotos)}
            <Vote className="h-4 w-4 shrink-0" strokeWidth={2.25} />
          </span>
          <div className="max-h-[520px] overflow-y-auto pr-2 print:max-h-none print:overflow-visible print:pr-0">
            {data.map((r, i) => (
              <div
                key={i}
                className="flex items-center justify-between border-b border-gray-100 py-2 text-sm last:border-0"
              >
                <div className="truncate pr-2">
                  <span className="mr-2 text-gray-400">{i + 1}.</span>
                  {r.nm_local}
                </div>
                <div className="whitespace-nowrap font-semibold text-[#0b2545]">
                  {fmtInt(r.votos)}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
