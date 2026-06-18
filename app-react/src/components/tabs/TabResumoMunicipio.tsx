import { resumoCandidatoMunicipio, votosCandidatoPorMunicipio } from '@/app/actions-tab2';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number, decimals = 1) {
  return new Intl.NumberFormat('pt-BR', { style: 'percent', minimumFractionDigits: decimals, maximumFractionDigits: decimals }).format(val / 100);
}

export async function TabResumoMunicipio({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Cargo e Candidato nos filtros acima.</div>;
  }

  const municipios = await votosCandidatoPorMunicipio(parseInt(ano, 10), uf, cargo, candidato, municipio);
  const municipiosFiltrados = municipios.filter(m => m.votos > 0);

  if (municipiosFiltrados.length === 0) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Nenhum município com votação do candidato na UF selecionada.</div>;
  }

  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Resumo por município</h2>
        <p className="text-gray-500">Municípios com votação na UF · {ano} · Cargo {cargo}</p>
      </div>

      {await Promise.all(municipiosFiltrados.map(async (row, i) => {
        const d = await resumoCandidatoMunicipio(parseInt(ano, 10), uf, row.cd, cargo, candidato);

        return (
          <div key={row.cd} className="flex flex-col gap-6">
            {i > 0 && <hr className="border-gray-200" />}
            
            <div>
              <h3 className="text-xl font-semibold text-[#0b2545]">{row.nm} ({uf})</h3>
              <p className="text-sm text-gray-500">Resumo de {d.nm_candidato || candidato} no município</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="bg-gray-50 p-4 rounded-lg border">
                <div className="text-gray-500 text-sm mb-1">Votação do candidato</div>
                <div className="text-3xl font-bold text-[#0b2545]">{fmtInt(d.votos_cand)}</div>
                <div className="text-sm text-gray-500 mt-2">{fmtPct(d.pct_validos)} dos {fmtInt(d.validos)} votos válidos do município.</div>
              </div>

              <div className="bg-gray-50 p-4 rounded-lg border">
                <div className="text-gray-500 text-sm mb-1">Posição geral no município</div>
                <div className="text-3xl font-bold text-[#0b2545]">{d.posicao ? `${d.posicao}º` : '—'}</div>
                <div className="text-sm text-gray-500 mt-2">Classificação entre {fmtInt(d.total_cands)} candidatos ao mesmo cargo.</div>
              </div>

              <div className="bg-gray-50 p-4 rounded-lg border">
                <div className="text-gray-500 text-sm mb-1">Liderança nos locais</div>
                <div className="text-3xl font-bold text-[#0b2545]">{fmtInt(d.lideres)}</div>
                <div className="text-sm text-gray-500 mt-2">locais onde ficou em 1º, de {fmtInt(d.total_locais)} analisados.</div>
              </div>

              <div className="bg-gray-50 p-4 rounded-lg border">
                <div className="text-gray-500 text-sm mb-1">Locais de votação analisados</div>
                <div className="text-3xl font-bold text-[#0b2545]">{fmtInt(d.total_locais)}</div>
                <div className="text-sm text-gray-500 mt-2">Comparecimento de {fmtPct(d.pct_comparec)} ({fmtInt(d.comparec)}/{fmtInt(d.aptos)}).</div>
              </div>
            </div>

            <div>
              <div className="text-lg font-bold text-[#0b2545] mb-4">Composição dos votos no município</div>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <div>
                  <div className="text-sm text-gray-500">Válidos (nominais + legenda)</div>
                  <div className="text-xl font-semibold text-[#0b2545]">{fmtInt(d.validos)}</div>
                </div>
                <div>
                  <div className="text-sm text-gray-500">Brancos</div>
                  <div className="text-xl font-semibold text-[#0b2545]">{fmtInt(d.brancos)}</div>
                </div>
                <div>
                  <div className="text-sm text-gray-500">Nulos</div>
                  <div className="text-xl font-semibold text-[#0b2545]">{fmtInt(d.nulos)}</div>
                </div>
                <div>
                  <div className="text-sm text-gray-500">Abstenções</div>
                  <div className="text-xl font-semibold text-[#0b2545]">{fmtInt(d.abstenc)}</div>
                </div>
                <div>
                  <div className="text-sm text-gray-500">Comparecimento</div>
                  <div className="text-xl font-semibold text-[#0b2545]">{fmtPct(d.pct_comparec, 0)}</div>
                </div>
              </div>
            </div>
          </div>
        );
      }))}
    </div>
  );
}
