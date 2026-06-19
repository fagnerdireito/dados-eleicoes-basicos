import { locaisDoMunicipio, nomeLocal, topCandidatosNoLocal } from '@/app/actions-tab8';
import { listarCargos, listarMunicipios } from '@/app/actions';
import { resolverCapital } from '@/lib/capitais';
import { normalizeLocalName } from '@/lib/utils';
import { Info, MapPin } from 'lucide-react';
import { LocalVotacaoPicker } from './LocalVotacaoPicker';

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
      <div className="w-full bg-gray-100 h-1.5 rounded-full overflow-hidden mt-1 flex chart-print-bg">
        <div style={{ width, backgroundColor: color }} className="h-full rounded-full chart-print-bg" />
      </div>
    </div>
  );
}

export async function TabCardLocal({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato, local } = searchParams;

  if (!ano || !uf || !cargo) {
    return <div className="soft-subtitle p-2">Selecione Ano, UF, Município e Cargo nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);

  // Eleição geral sem cidade: usa a capital da UF como fallback.
  let municipioEfetivo = municipio;
  let usandoCapital = false;
  if (!municipioEfetivo) {
    const capital = await resolverCapital(anoNum, uf);
    if (!capital) {
      return <div className="soft-subtitle p-2">Selecione Ano, UF, Município e Cargo nos filtros acima.</div>;
    }
    municipioEfetivo = capital.cd;
    usandoCapital = true;
  }

  const [locais, cargos, municipios] = await Promise.all([
    locaisDoMunicipio(anoNum, uf, municipioEfetivo, cargo),
    listarCargos(anoNum, uf, municipioEfetivo),
    listarMunicipios(anoNum, uf),
  ]);
  const cargoLabel = cargos.find((c) => String(c.cd) === cargo)?.ds ?? cargo;
  const municipioNome = municipios.find((m) => String(m.cd) === municipioEfetivo)?.nm ?? municipioEfetivo;

  const avisoCapital = usandoCapital ? (
    <div className="flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 print:hidden">
      <Info className="mt-0.5 h-4 w-4 shrink-0" strokeWidth={2.25} />
      <p>
        Eleição geral sem cidade selecionada — exibindo a capital{' '}
        <strong>{municipioNome}</strong> por padrão. Selecione uma cidade nos filtros para ver outro município.
      </p>
    </div>
  ) : null;

  if (locais.length === 0) {
    return (
      <div className="flex flex-col gap-8">
        {avisoCapital}
        <header>
          <h2 className="soft-title">Votos por local de votação</h2>
          <p className="soft-subtitle">Top 10 candidatos no local · {municipioNome}</p>
        </header>
        <div className="soft-alert">Sem locais para o filtro atual.</div>
      </div>
    );
  }

  // Build the list of local options with names
  const locaisComNome = await Promise.all(locais.map(async l => {
    const nome = await nomeLocal(uf, municipioEfetivo, l.nr_local);
    return { ...l, nome: nome || `Local ${l.nr_local}` };
  }));

  const currentLocalObj =
    (local
      ? locaisComNome.find((l) => normalizeLocalName(l.nome) === normalizeLocalName(local))
        ?? locaisComNome.find((l) => l.nr_local === local)
      : undefined) ?? locaisComNome[0];

  const d = await topCandidatosNoLocal(anoNum, uf, municipioEfetivo, cargo, currentLocalObj.nr_local);

  const foco = d.ranking.find(r => r.nr === candidato);

  return (
    <div className="flex flex-col gap-8">
      {avisoCapital}
      <header>
        <h2 className="soft-title">Votos por local de votação</h2>
        <p className="soft-subtitle">Top 10 candidatos no local · {municipioNome}</p>
      </header>

      <div className="flex flex-col items-start gap-6 md:flex-row">
        <div className="flex w-full flex-col md:w-1/3 print:hidden">
          <label className="mb-2 text-sm font-medium text-gray-700">Selecione o local de votação</label>
          <LocalVotacaoPicker
            locais={locaisComNome}
            selectedNome={currentLocalObj.nome}
            searchParams={searchParams}
          />
        </div>

        <div className="tab-card-local-print-block flex w-full flex-col gap-6 md:w-2/3 print:w-full">
          <div>
            <span className="recorte-pill chart-print-bg mb-2 inline-flex w-fit max-w-full items-center gap-2 rounded-full bg-indigo-600/70 px-4 py-1.5 text-base font-semibold text-white">
              <span className="truncate">{currentLocalObj.nome}</span>
              <MapPin className="h-4 w-4 shrink-0" strokeWidth={2.25} />
            </span>
            <p className="soft-subtitle">Top 10 no local · {cargoLabel}</p>
            <p className="soft-footnote mt-1">
              {fmtInt(d.validos)} votos válidos · {fmtInt(d.brancos)} em branco · {fmtInt(d.nulos)} nulos
            </p>
          </div>

          <div className="chart-print-bg rounded-lg border bg-white p-5 shadow-sm print:shadow-none">
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
            <div className="chart-print-bg rounded-lg border border-blue-100 bg-blue-50 p-4 text-[#0b2545]">
              Desempenho de <b>{foco.nm}</b>: {fmtInt(foco.votos)} votos ({foco.pct.toFixed(2)}%) no local.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
