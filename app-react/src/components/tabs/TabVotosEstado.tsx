import { votosCandidatoPorMunicipio } from '@/app/actions-tab2';
import { MapEstadoDynamicWrapper } from './charts/MapEstadoDynamic';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

export async function TabVotosEstado({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Cargo e Candidato nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);
  
  // Note: we don't filter by municipio here because we want to see the state map
  const data = await votosCandidatoPorMunicipio(anoNum, uf, cargo, candidato);

  if (data.length === 0) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Nenhum voto do candidato encontrado na UF selecionada.</div>;
  }

  const totalVotos = data.reduce((acc, curr) => acc + curr.votos, 0);

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Onde estão os votos no estado</h2>
        <p className="text-gray-500">Distribuição geográfica dos votos de {candidato} em {uf} ({ano})</p>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg border w-fit">
        <div className="text-sm text-gray-500">Total de votos no estado</div>
        <div className="text-2xl font-bold text-[#0b2545]">{fmtInt(totalVotos)}</div>
      </div>

      <MapEstadoDynamicWrapper uf={uf} data={data} />
    </div>
  );
}
