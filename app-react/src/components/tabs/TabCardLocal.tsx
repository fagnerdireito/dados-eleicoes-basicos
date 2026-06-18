import { locaisDoMunicipio, nomeLocal, topCandidatosNoLocal } from '@/app/actions-tab8';
import Link from 'next/link';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(val) + '%';
}

function BarRow({ nm, partido, votos, pct, index, isHighlight }: { nm: string, partido: string, votos: number, pct: number, index: number, isHighlight: boolean }) {
  const color = isHighlight ? '#22c55e' : '#1f6feb';
  const width = Math.max(pct, 0.5) + '%';

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

export async function TabCardLocal({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato, local } = searchParams;

  if (!ano || !uf || !municipio || !cargo) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Município e Cargo nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);
  const locais = await locaisDoMunicipio(anoNum, uf, municipio, cargo);

  if (locais.length === 0) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Sem locais para o filtro atual.</div>;
  }

  // Build the list of local options with names
  const locaisComNome = await Promise.all(locais.map(async l => {
    const nome = await nomeLocal(uf, municipio, l.nr_local);
    return { ...l, nome: nome || `Local ${l.nr_local}` };
  }));

  const selectedLocal = local || locaisComNome[0].nr_local;
  const currentLocalObj = locaisComNome.find(l => l.nr_local === selectedLocal) || locaisComNome[0];

  const d = await topCandidatosNoLocal(anoNum, uf, municipio, cargo, selectedLocal);

  const foco = d.ranking.find(r => r.nr === candidato);

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-col md:flex-row gap-6 items-start">
        <div className="w-full md:w-1/3 flex flex-col">
          <label className="text-sm font-medium mb-2 text-gray-700">Selecione o local de votação</label>
          <div className="flex flex-col gap-1 max-h-[600px] overflow-y-auto pr-2 border rounded-md p-2 bg-gray-50">
            {locaisComNome.map(l => {
              const params = new URLSearchParams(searchParams as any);
              params.set('local', l.nr_local);
              const isActive = l.nr_local === selectedLocal;
              
              return (
                <Link 
                  key={l.nr_local}
                  href={`/?${params.toString()}`}
                  className={`p-2 text-sm rounded-md transition-colors truncate ${isActive ? 'bg-[#0b2545] text-white' : 'hover:bg-gray-200 text-gray-700'}`}
                  title={`${l.nome} (${l.nr_local})`}
                >
                  <div className="font-semibold">{l.nome}</div>
                  <div className={`text-xs ${isActive ? 'text-gray-300' : 'text-gray-500'}`}>Local {l.nr_local}</div>
                </Link>
              );
            })}
          </div>
        </div>

        <div className="w-full md:w-2/3 flex flex-col gap-6">
          <div>
            <h2 className="text-2xl font-bold text-[#0b2545]">{currentLocalObj.nome}</h2>
            <p className="text-gray-500">Top 10 no local · Cargo {cargo}</p>
            <p className="text-sm text-gray-400 mt-2">
              {fmtInt(d.validos)} votos válidos · {fmtInt(d.brancos)} em branco · {fmtInt(d.nulos)} nulos
            </p>
          </div>

          <div className="bg-white border rounded-lg p-5 shadow-sm">
            {d.ranking.length === 0 ? (
              <p className="text-gray-500 text-sm">Sem ranking para esse local.</p>
            ) : (
              <div className="flex flex-col">
                {d.ranking.map((r, i) => (
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

          {foco && (
            <div className="bg-blue-50 border border-blue-100 rounded-lg p-4 text-[#0b2545]">
              Desempenho de <b>{foco.nm}</b>: {fmtInt(foco.votos)} votos ({foco.pct.toFixed(2)}%) no local.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
