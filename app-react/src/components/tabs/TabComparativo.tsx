import { comparativoVotosTerritorio } from '@/app/actions-tab9';
import { listarCandidatos } from '@/app/actions';
import { checkTableExists } from '@/app/actions-tab3';
import { ComparativoCandidatoPicker } from '@/components/tabs/ComparativoCandidatoPicker';
import Link from 'next/link';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(val) + '%';
}

const CAND_COLORS = ["#1f6feb", "#ef4444", "#22c55e", "#f59e0b"];

export async function TabComparativo({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato, dim, cands } = searchParams;

  if (!ano || !uf || !municipio || !cargo) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Município e Cargo nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);
  const dimensao = dim || 'zona';
  
  // Parse selected candidates
  let selectedNrs = cands
    ? cands.split(',').map((s) => s.trim()).filter(Boolean)
    : candidato
      ? [candidato]
      : [];
  if (selectedNrs.length === 0 && candidato) selectedNrs = [candidato];
  if (selectedNrs.length > 4) selectedNrs = selectedNrs.slice(0, 4);

  const todosCandidatos = await listarCandidatos(anoNum, uf, cargo, municipio, 500);
  const hasLocalVotacao = await checkTableExists('local_votacao');

  const dimLabels = {
    "zona": "Zona",
    "bairro": "Bairro",
    "secao": "Seção",
    "local": "Local de votação",
  };

  const dimPrecisaLocalVotacao = ["bairro", "local"];
  const isValidDim = hasLocalVotacao || !dimPrecisaLocalVotacao.includes(dimensao);

  if (!isValidDim) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">A dimensão '{dimLabels[dimensao as keyof typeof dimLabels]}' requer a tabela local_votacao.</div>;
  }

  const rawData = await comparativoVotosTerritorio(anoNum, uf, municipio, cargo, selectedNrs, dimensao);

  // Pivot data
  const rows = [];
  const grouped = rawData.reduce((acc: any, curr: any) => {
    if (!acc[curr.territorio]) {
      acc[curr.territorio] = {
        territorio: curr.territorio,
        nm_local: curr.nm_local,
        total_selecionados: 0,
        total_territorio: curr.total_territorio,
        votos: {}
      };
    }
    acc[curr.territorio].votos[curr.nr] = curr.votos;
    acc[curr.territorio].total_selecionados += curr.votos;
    return acc;
  }, {});

  for (const terr of Object.values(grouped) as any[]) {
    const row: any = {
      territorio: terr.territorio,
      nm_local: terr.nm_local,
    };
    for (const nr of selectedNrs) {
      const v = terr.votos[nr] || 0;
      row[`${nr}_votos`] = v;
      row[`${nr}_pct_territorio`] = terr.total_territorio ? (v / terr.total_territorio * 100) : 0;
      row[`${nr}_pct_comparativo`] = terr.total_selecionados ? (v / terr.total_selecionados * 100) : 0;
    }
    rows.push(row);
  }

  // Sort by first candidate's votes
  const sortCol = `${selectedNrs[0]}_votos`;
  rows.sort((a, b) => (b[sortCol] || 0) - (a[sortCol] || 0));

  const mostrarComparativo = selectedNrs.length > 1;

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Comparativo de candidatos no município</h2>
        <p className="text-gray-500">Compara até 4 candidatos nas divisões do município</p>
      </div>

      <ComparativoCandidatoPicker
        candidatos={todosCandidatos}
        selectedNrs={selectedNrs}
        searchParams={searchParams}
      />

      {selectedNrs.length > 0 && (
        <div className={`grid gap-4 ${selectedNrs.length === 1 ? 'grid-cols-1 max-w-xs' : selectedNrs.length === 2 ? 'grid-cols-2' : selectedNrs.length === 3 ? 'grid-cols-3' : 'grid-cols-4'}`}>
          {selectedNrs.map((nr, i) => {
            const cInfo = todosCandidatos.find((c: { nr: string }) => c.nr === nr);
            const color = CAND_COLORS[i % CAND_COLORS.length];
            return (
              <div
                key={nr}
                className="rounded-lg border-2 bg-[#fafbfd] p-3 text-center"
                style={{ borderColor: color }}
              >
                <div className="text-sm font-bold text-[#0b2545]">{cInfo?.nm ?? nr}</div>
                <div className="text-xs text-gray-500">{cInfo?.sg_partido ?? '—'}</div>
                <div className="mt-1 text-2xl font-extrabold" style={{ color }}>
                  {fmtInt(Number(cInfo?.votos ?? 0))}
                </div>
                <div className="text-xs text-gray-500">votos no município</div>
              </div>
            );
          })}
        </div>
      )}

      <div className="flex gap-4 items-center bg-gray-50 p-4 border rounded-lg overflow-x-auto">
        <span className="font-semibold text-sm mr-2">Dimensão:</span>
        {Object.entries(dimLabels).map(([key, label]) => {
          const isDisabled = !hasLocalVotacao && dimPrecisaLocalVotacao.includes(key);
          const isActive = key === dimensao;
          
          const params = new URLSearchParams(searchParams as any);
          params.set('dim', key);

          if (isDisabled) {
            return <span key={key} className="px-3 py-1 text-sm rounded bg-gray-200 text-gray-400 cursor-not-allowed" title="Requer local_votacao">{label}</span>;
          }

          return (
            <Link 
              key={key} 
              href={`/?${params.toString()}`}
              className={`px-3 py-1 text-sm rounded transition-colors whitespace-nowrap ${isActive ? 'bg-[#0b2545] text-white' : 'bg-white border hover:bg-gray-100 text-[#0b2545]'}`}
            >
              {label}
            </Link>
          );
        })}
      </div>

      {rows.length === 0 ? (
        <div className="bg-blue-50 text-blue-800 p-4 rounded">Sem dados para o filtro atual.</div>
      ) : (
        <div className="overflow-x-auto border rounded-lg">
          <table className="min-w-full bg-white text-sm">
            <thead className="bg-gray-50 border-b">
              <tr>
                <th className="py-3 px-4 text-left font-semibold text-[#0b2545]">
                  {dimLabels[dimensao as keyof typeof dimLabels]}
                </th>
                {selectedNrs.map((nr, i) => {
                  const cInfo = todosCandidatos.find((c: any) => c.nr === nr);
                  const color = CAND_COLORS[i % CAND_COLORS.length];
                  return (
                    <th key={nr} className="py-3 px-4 text-right border-l">
                      <div className="flex flex-col items-end">
                        <span className="font-bold truncate max-w-[150px]" style={{ color }}>{cInfo ? cInfo.nm : nr}</span>
                        <span className="text-xs text-gray-500 font-normal">{cInfo ? cInfo.sg_partido : ''}</span>
                      </div>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i} className="border-b last:border-0 hover:bg-gray-50">
                  <td className="py-3 px-4">
                    <div className="font-medium text-[#0b2545]">{row.territorio}</div>
                    {row.nm_local && <div className="text-xs text-gray-500">{row.nm_local}</div>}
                  </td>
                  {selectedNrs.map((nr) => (
                    <td key={nr} className="py-3 px-4 text-right border-l">
                      <div className="font-bold text-[#0b2545]">{fmtInt(row[`${nr}_votos`])}</div>
                      <div className="text-xs text-gray-500">
                        <span title={`% sobre o total do ${dimLabels[dimensao as keyof typeof dimLabels].toLowerCase()}`}>
                          {fmtPct(row[`${nr}_pct_territorio`])}
                        </span>
                        {mostrarComparativo && (
                          <>
                            <span className="mx-1 text-gray-300">|</span>
                            <span title="% entre os candidatos comparados">
                              {fmtPct(row[`${nr}_pct_comparativo`])}
                            </span>
                          </>
                        )}
                      </div>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
