import { turnoutUf, perfilFaixaEtaria, perfilEscolaridade } from '@/app/actions-tab3';
import { listarMunicipios } from '@/app/actions';
import { BarChartHorizontal } from './charts/BarChartHorizontal';
import { MapPin, UserCheck, UserMinus } from 'lucide-react';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { style: 'percent', minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(val / 100);
}

const UF_NOMES: Record<string, string> = {
  AC: 'Acre', AL: 'Alagoas', AP: 'Amapá', AM: 'Amazonas', BA: 'Bahia', CE: 'Ceará',
  DF: 'Distrito Federal', ES: 'Espírito Santo', GO: 'Goiás', MA: 'Maranhão', MT: 'Mato Grosso',
  MS: 'Mato Grosso do Sul', MG: 'Minas Gerais', PA: 'Pará', PB: 'Paraíba', PR: 'Paraná',
  PE: 'Pernambuco', PI: 'Piauí', RJ: 'Rio de Janeiro', RN: 'Rio Grande do Norte',
  RS: 'Rio Grande do Sul', RO: 'Rondônia', RR: 'Roraima', SC: 'Santa Catarina',
  SP: 'São Paulo', SE: 'Sergipe', TO: 'Tocantins',
};

export async function TabPerfilEleitorado({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio } = searchParams;

  if (!ano || !uf) {
    return <div className="text-gray-500 p-4">Selecione Ano e UF nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);
  const recorteKpi = municipio ? "município" : "UF";

  let recorteTitle = UF_NOMES[uf] ?? uf;
  if (municipio) {
    const municipios = await listarMunicipios(anoNum, uf);
    recorteTitle = municipios.find((m) => String(m.cd) === municipio)?.nm ?? recorteTitle;
  }

  const turnout = await turnoutUf(anoNum, uf, municipio);
  const ages = await perfilFaixaEtaria(anoNum, uf, municipio);
  const education = await perfilEscolaridade(anoNum, uf, municipio);

  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="flex flex-wrap items-center gap-3 text-2xl font-bold text-[#0b2545]">
          Perfil do eleitorado
          <span className="recorte-pill chart-print-bg inline-flex items-center gap-2 rounded-full bg-indigo-600/70 px-4 py-1.5 text-base font-semibold text-white">
            {recorteTitle}
            <MapPin className="h-4 w-4 shrink-0" strokeWidth={2.25} />
          </span>
        </h2>
        <p className="text-gray-500">Comparecimento do pleito e composição cadastral do eleitorado</p>
      </div>

      <div className="perfil-eleitorado-grid grid grid-cols-1 gap-6 lg:grid-cols-4 print:grid-cols-1 print:gap-4">
        <div className="perfil-kpi-row flex flex-col gap-4 lg:col-span-1 print:flex-row print:gap-4">
          <div className="grid w-full grid-cols-[auto_1fr] items-center gap-3 rounded-lg bg-white p-4 shadow-lg chart-print-bg print:min-w-0 print:flex-1">
            <div className="flex h-12 w-12 shrink-0 items-center justify-center self-center rounded-2xl bg-emerald-100 text-emerald-600 shadow-sm chart-print-bg">
              <UserCheck className="h-5 w-5" strokeWidth={2.25} />
            </div>
            <div className="min-w-0 flex-1">
              <div className="mb-1 text-xs uppercase text-gray-500">Comparecimento ({recorteKpi})</div>
              <div className="text-3xl font-bold text-[#0b2545]">{fmtPct(turnout.pct_comparec)}</div>
              <div className="mt-2 text-sm text-gray-500">{fmtInt(turnout.comparec)} eleitores</div>
            </div>
          </div>

          <div className="grid w-full grid-cols-[auto_1fr] items-center gap-3 rounded-lg bg-white p-4 shadow-lg chart-print-bg print:min-w-0 print:flex-1">
            <div className="flex h-12 w-12 shrink-0 items-center justify-center self-center rounded-2xl bg-orange-100 text-orange-600 shadow-sm chart-print-bg">
              <UserMinus className="h-5 w-5" strokeWidth={2.25} />
            </div>
            <div className="min-w-0 flex-1">
              <div className="mb-1 text-xs uppercase text-gray-500">Abstenção ({recorteKpi})</div>
              <div className="text-3xl font-bold text-[#0b2545]">{fmtPct(turnout.pct_abstenc)}</div>
              <div className="mt-2 text-sm text-gray-500">{fmtInt(turnout.abstenc)} eleitores</div>
            </div>
          </div>
        </div>

        <div className="flex w-full flex-col gap-6 lg:col-span-3 lg:flex-row print:flex-row print:gap-4">
          {!ages && !education ? (
            <div className="md:col-span-2 bg-blue-50 text-blue-800 p-4 rounded">
              Tabela `perfil_eleitorado` não encontrada no banco ou sem dados.
            </div>
          ) : (
            <>
              <div className="perfil-chart-panel w-full flex-1 rounded-lg bg-white px-4 py-4 shadow-xl chart-print-bg print:min-w-0 print:py-2 print:shadow-none">
                <h4 className="font-semibold text-[#0b2545] mb-4 print:mb-2">Eleitorado por faixa etária</h4>
                {ages && ages.length > 0 ? (
                  <BarChartHorizontal data={ages} height={600} className="perfil-chart-age" />
                ) : (
                  <div className="text-sm text-gray-500">Sem dados para esse recorte.</div>
                )}
              </div>
              
              <div className="perfil-chart-panel w-full flex-1 rounded-lg bg-white px-4 py-4 shadow-xl chart-print-bg print:min-w-0 print:py-2 print:shadow-none">
                <h4 className="font-semibold text-[#0b2545] mb-4 print:mb-2">Eleitorado por escolaridade</h4>
                {education && education.length > 0 ? (
                  <BarChartHorizontal data={education} height={460} className="perfil-chart-education" />
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
