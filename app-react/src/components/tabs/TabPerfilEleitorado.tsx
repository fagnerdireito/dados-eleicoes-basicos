import { turnoutUf, perfilFaixaEtaria, perfilEscolaridade } from '@/app/actions-tab3';
import { BarChartHorizontal } from './charts/BarChartHorizontal';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { style: 'percent', minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(val / 100);
}

export async function TabPerfilEleitorado({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio } = searchParams;

  if (!ano || !uf) {
    return <div className="text-gray-500 p-4">Selecione Ano e UF nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);
  const recorteKpi = municipio ? "município" : "UF";
  const recorteTitle = municipio ? `${municipio}/${uf}` : `estado ${uf}`;

  const turnout = await turnoutUf(anoNum, uf, municipio);
  const ages = await perfilFaixaEtaria(anoNum, uf, municipio);
  const education = await perfilEscolaridade(anoNum, uf, municipio);

  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Perfil do eleitorado ({recorteTitle})</h2>
        <p className="text-gray-500">Comparecimento do pleito e composição cadastral do eleitorado</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        <div className="lg:col-span-1 flex flex-col gap-4">
          <div className="bg-gray-50 p-4 rounded-lg border">
            <div className="text-gray-500 text-sm mb-1">Comparecimento ({recorteKpi})</div>
            <div className="text-3xl font-bold text-[#0b2545]">{fmtPct(turnout.pct_comparec)}</div>
            <div className="text-sm text-gray-500 mt-2">{fmtInt(turnout.comparec)} eleitores</div>
          </div>
          
          <div className="bg-gray-50 p-4 rounded-lg border">
            <div className="text-gray-500 text-sm mb-1">Abstenção ({recorteKpi})</div>
            <div className="text-3xl font-bold text-[#0b2545]">{fmtPct(turnout.pct_abstenc)}</div>
            <div className="text-sm text-gray-500 mt-2">{fmtInt(turnout.abstenc)} eleitores</div>
          </div>
        </div>

        <div className="lg:col-span-3 grid grid-cols-1 md:grid-cols-2 gap-6">
          {!ages && !education ? (
            <div className="md:col-span-2 bg-blue-50 text-blue-800 p-4 rounded">
              Tabela `perfil_eleitorado` não encontrada no banco ou sem dados.
            </div>
          ) : (
            <>
              <div>
                <h4 className="font-semibold text-[#0b2545] mb-4">Eleitorado por faixa etária</h4>
                {ages && ages.length > 0 ? (
                  <BarChartHorizontal data={ages} height={600} />
                ) : (
                  <div className="text-sm text-gray-500">Sem dados para esse recorte.</div>
                )}
              </div>
              
              <div>
                <h4 className="font-semibold text-[#0b2545] mb-4">Eleitorado por escolaridade</h4>
                {education && education.length > 0 ? (
                  <BarChartHorizontal data={education} height={460} />
                ) : (
                  <div className="text-sm text-gray-500">Sem dados para esse recorte.</div>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
