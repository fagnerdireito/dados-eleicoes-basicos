import { listarCargos } from '@/app/actions';
import { rankingMunicipio } from '@/app/actions-tab6';
import { resolverCapital } from '@/lib/capitais';
import { Info } from 'lucide-react';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(val) + '%';
}

function BarRow({ nm, partido, votos, pct, index, isHighlight }: { nm: string, partido: string, votos: number, pct: number, index: number, isHighlight: boolean }) {
  const color = isHighlight ? '#22c55e' : '#1f6feb';
  const width = Math.max(pct, 0.5) + '%'; // Ensure a minimum width for visibility

  return (
    <div className="flex flex-col py-2 border-b border-gray-100 last:border-0">
      <div className="flex justify-between items-center text-sm mb-1">
        <div className="flex items-center gap-2 truncate">
          <span className="text-gray-400 font-mono w-5">{index}.</span>
          <span className="font-semibold text-[#0b2545] truncate" title={nm}>{nm}</span>
          <span className="text-gray-500 text-xs px-1.5 py-0.5 bg-gray-100 rounded">{partido || '—'}</span>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-[#0b2545] font-medium">{fmtInt(votos)}</span>
          <span className="text-gray-500 text-xs w-12 text-right">{fmtPct(pct)}</span>
        </div>
      </div>
      <div className="w-full bg-gray-100 h-1.5 rounded-full overflow-hidden mt-1 flex">
        <div style={{ width, backgroundColor: color }} className="h-full rounded-full" />
      </div>
    </div>
  );
}

export async function TabRankingMunicipio({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Município e Cargo nos filtros acima.</div>;
  }

  const anoAtual = parseInt(ano, 10);
  const anoAnterior = anoAtual - 4;

  // Eleição geral sem cidade: usa a capital da UF como fallback.
  let municipioEfetivo = municipio;
  let usandoCapital = false;
  let capitalNome = '';
  if (!municipioEfetivo) {
    const capital = await resolverCapital(anoAtual, uf);
    if (!capital) {
      return <div className="text-gray-500 p-4">Selecione Ano, UF, Município e Cargo nos filtros acima.</div>;
    }
    municipioEfetivo = capital.cd;
    capitalNome = capital.nm;
    usandoCapital = true;
  }

  const [dfAtual, dfAnterior, cargos] = await Promise.all([
    rankingMunicipio(anoAtual, uf, municipioEfetivo, cargo),
    rankingMunicipio(anoAnterior, uf, municipioEfetivo, cargo),
    listarCargos(anoAtual, uf, municipioEfetivo),
  ]);

  const cargoLabel = cargos.find((c) => String(c.cd) === cargo)?.ds || cargo;

  return (
    <div className="flex flex-col gap-6">
      {usandoCapital && (
        <div className="flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 print:hidden">
          <Info className="mt-0.5 h-4 w-4 shrink-0" strokeWidth={2.25} />
          <p>
            Eleição geral sem cidade selecionada — exibindo a capital{' '}
            <strong>{capitalNome}</strong> por padrão. Selecione uma cidade nos filtros para ver outro município.
          </p>
        </div>
      )}

      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Ranking geral no município</h2>
        <p className="text-gray-500">Top 10 candidatos mais votados · {cargoLabel}</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        <div className="bg-white border rounded-lg p-5 shadow-sm">
          <h4 className="text-lg font-bold text-[#0b2545] mb-4 border-b pb-2">{anoAnterior}</h4>
          {dfAnterior.length === 0 ? (
            <p className="text-gray-500 text-sm">Sem dados de {anoAnterior} para esse cargo/município.</p>
          ) : (
            <div className="flex flex-col">
              {dfAnterior.map((r, i) => (
                <BarRow 
                  key={r.nr} 
                  index={i + 1} 
                  nm={r.nm} 
                  partido={r.partido} 
                  votos={r.votos} 
                  pct={r.pct} 
                  isHighlight={false} 
                />
              ))}
            </div>
          )}
        </div>

        <div className="bg-white border rounded-lg p-5 shadow-sm">
          <h4 className="text-lg font-bold text-[#0b2545] mb-4 border-b pb-2">{anoAtual}</h4>
          {dfAtual.length === 0 ? (
            <p className="text-gray-500 text-sm">Sem dados para o filtro atual.</p>
          ) : (
            <div className="flex flex-col">
              {dfAtual.map((r, i) => (
                <BarRow 
                  key={r.nr} 
                  index={i + 1} 
                  nm={r.nm} 
                  partido={r.partido} 
                  votos={r.votos} 
                  pct={r.pct} 
                  isHighlight={r.nr === candidato} 
                />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
