import { votosCandidatoPorMunicipio } from '@/app/actions-tab2';
import { listarCandidatos } from '@/app/actions';
import { MapEstadoDynamicWrapper } from './charts/MapEstadoDynamic';

export async function TabVotosEstado({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Cargo e Candidato nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);

  const candidatos = await listarCandidatos(anoNum, uf, cargo);
  const c = candidatos.find((item) => String(item.nr) === candidato);
  const candidatoLabel = c ? `${c.nm}${c.sg_partido ? ` (${c.sg_partido})` : ''}` : candidato;

  const data = await votosCandidatoPorMunicipio(anoNum, uf, cargo, candidato);

  if (data.length === 0) {
    return <div className="rounded-lg bg-blue-50 p-4 text-blue-800">Nenhum voto do candidato encontrado na UF selecionada.</div>;
  }

  const totalVotos = data.reduce((acc, curr) => acc + curr.votos, 0);

  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Onde estão os votos no estado</h2>
        <p className="text-gray-500">Distribuição geográfica dos votos de {candidatoLabel} em {uf} ({ano})</p>
      </div>

      <MapEstadoDynamicWrapper uf={uf} data={data} totalVotos={totalVotos} />
    </div>
  );
}
