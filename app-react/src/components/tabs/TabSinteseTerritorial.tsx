import { sinteseTerritorial, sinteseTerritorialUf } from '@/app/actions-tab7';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { style: 'percent', minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(val / 100);
}

export async function TabSinteseTerritorial({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF e Cargo nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);

  if (municipio) {
    const data = await sinteseTerritorial(anoNum, uf, municipio, cargo);
    if (data.length === 0) {
      return <div className="bg-blue-50 text-blue-800 p-4 rounded">Sem dados para o filtro atual.</div>;
    }
    const total = data.reduce((acc, curr) => acc + curr.locais, 0);

    return (
      <div className="flex flex-col gap-6">
        <div>
          <h2 className="text-2xl font-bold text-[#0b2545]">Síntese territorial</h2>
          <p className="text-gray-500">Locais liderados por cada candidato</p>
          <p className="text-sm text-gray-400 mt-1">Total de <strong>{fmtInt(total)} locais</strong> com vencedor apurado.</p>
        </div>

        <div className="flex flex-col">
          {data.map((r, i) => {
            const isFoco = r.nr === candidato;
            return (
              <div key={i} className="flex gap-4 py-3 border-b border-gray-100 last:border-0 items-center">
                <div className="text-gray-400 font-mono w-8">{i + 1}</div>
                <div className="flex-1">
                  <div className="font-semibold text-[#0b2545]">{r.nm}{isFoco && <span className="text-gray-400 font-normal ml-1">(foco)</span>}</div>
                  <div className="text-gray-500 text-sm">{r.partido || '—'}</div>
                </div>
                <div className="text-right">
                  <div className="text-[#0b2545] font-bold">{fmtInt(r.locais)} <span className="text-gray-500 font-normal text-sm">locais</span></div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  // Uf Context
  const dataUf = await sinteseTerritorialUf(anoNum, uf, cargo);
  if (dataUf.length === 0) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Sem dados para o filtro atual.</div>;
  }
  const totalUf = dataUf.reduce((acc, curr) => acc + curr.votos, 0);

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Síntese territorial</h2>
        <p className="text-gray-500">Votos por candidato na UF — {uf} · {cargo}</p>
        <p className="text-sm text-gray-400 mt-1">Total de <strong>{fmtInt(totalUf)} votos válidos</strong> na UF.</p>
      </div>

      <div className="flex flex-col">
        {dataUf.map((r, i) => {
          const isFoco = r.nr === candidato;
          return (
            <div key={i} className="flex gap-4 py-3 border-b border-gray-100 last:border-0 items-center">
              <div className="text-gray-400 font-mono w-8">{i + 1}</div>
              <div className="flex-1">
                <div className="font-semibold text-[#0b2545]">{r.nm}{isFoco && <span className="text-gray-400 font-normal ml-1">(foco)</span>}</div>
                <div className="text-gray-500 text-sm">{r.partido || '—'}</div>
              </div>
              <div className="text-right flex items-center gap-2">
                <div className="text-[#0b2545] font-bold">{fmtInt(r.votos)}</div>
                <div className="text-gray-500 text-sm w-16 text-right">({fmtPct(r.pct)})</div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
