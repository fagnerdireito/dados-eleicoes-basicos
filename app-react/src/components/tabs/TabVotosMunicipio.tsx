import { votosCandidatoPorLocal } from '@/app/actions-tab5';
import { MapMunicipioDynamicWrapper } from './charts/MapMunicipioDynamic';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

export async function TabVotosMunicipio({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato || !municipio) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Município, Cargo e Candidato nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);
  const data = await votosCandidatoPorLocal(anoNum, uf, municipio, cargo, candidato);

  if (!data) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Tabela `local_votacao` não encontrada.</div>;
  }

  if (data.length === 0) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Sem coordenadas válidas para os locais deste município.</div>;
  }

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Onde estão os votos no município</h2>
        <p className="text-gray-500">Votação de {candidato} por local — {municipio}</p>
      </div>

      <div className="flex flex-col md:flex-row gap-6">
        <div className="w-full md:w-2/3">
          <MapMunicipioDynamicWrapper data={data} />
          <p className="text-sm text-gray-400 mt-2">Cada bolha é um local de votação (posição por GPS, tamanho conforme os votos).</p>
        </div>

        <div className="w-full md:w-1/3 flex flex-col">
          <h4 className="font-semibold text-[#0b2545] mb-4">Top locais (votos do candidato)</h4>
          <div className="max-h-[520px] overflow-y-auto pr-2">
            {data.map((r, i) => (
              <div key={i} className="flex justify-between items-center py-2 border-b border-gray-100 last:border-0 text-sm">
                <div className="truncate pr-2">
                  <span className="text-gray-400 mr-2">{i + 1}.</span> 
                  {r.nm_local}
                </div>
                <div className="font-semibold whitespace-nowrap text-[#0b2545]">
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
