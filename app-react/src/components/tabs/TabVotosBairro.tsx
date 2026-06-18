import { votosPorBairro, votosPorLocalCandidato } from '@/app/actions-tab10';
import { votosCandidatoPorMunicipio } from '@/app/actions-tab2';
import { checkTableExists } from '@/app/actions-tab3';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

export async function TabVotosBairro({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato) {
    return <div className="text-gray-500 p-4">Selecione Ano, UF, Cargo e Candidato nos filtros acima.</div>;
  }

  const hasLocalVotacao = await checkTableExists('local_votacao');
  if (!hasLocalVotacao) {
    return <div className="bg-blue-50 text-blue-800 p-4 rounded">Tabela \`local_votacao\` não encontrada no banco de dados.</div>;
  }

  const anoNum = parseInt(ano, 10);

  // If no city selected, just show the whole state table
  const dfMun = await votosCandidatoPorMunicipio(anoNum, uf, cargo, candidato, municipio);
  const totalMun = dfMun.reduce((acc, curr) => acc + curr.votos, 0);

  let dfBairro: any[] = [];
  let dfLocal: any[] = [];
  
  if (municipio) {
    [dfBairro, dfLocal] = await Promise.all([
      votosPorBairro(anoNum, uf, municipio, cargo, candidato),
      votosPorLocalCandidato(anoNum, uf, municipio, cargo, candidato)
    ]);
  }

  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Votos por bairro</h2>
        <p className="text-gray-500">Agregação por bairro/local — {candidato}</p>
      </div>

      {!municipio && (
        <div className="text-gray-500 p-3 bg-gray-50 rounded border">
          Selecione um município nos filtros para ver o detalhamento por bairro e local.
        </div>
      )}

      {dfMun.length > 0 && (
        <div>
          <h4 className="font-semibold text-lg text-[#0b2545] mb-3">Município</h4>
          <div className="overflow-x-auto border rounded-lg">
            <table className="min-w-full bg-white text-sm">
              <thead className="bg-gray-50 border-b text-left text-gray-500">
                <tr>
                  <th className="py-2 px-4 font-semibold">Município</th>
                  <th className="py-2 px-4 font-semibold">Ano</th>
                  <th className="py-2 px-4 font-semibold text-right">Votos</th>
                </tr>
              </thead>
              <tbody>
                {dfMun.map((r, i) => (
                  <tr key={i} className="border-b last:border-0 hover:bg-gray-50">
                    <td className="py-2 px-4 text-[#0b2545] font-medium">{r.nm}</td>
                    <td className="py-2 px-4 text-gray-500">{ano}</td>
                    <td className="py-2 px-4 text-right font-bold text-[#0b2545]">{fmtInt(r.votos)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-sm text-gray-500 mt-2">Total: <strong>{fmtInt(totalMun)}</strong></p>
        </div>
      )}

      {municipio && (
        <>
          <div>
            <h4 className="font-semibold text-lg text-[#0b2545] mb-3">Bairro</h4>
            {dfBairro.length === 0 ? (
              <p className="text-gray-500 text-sm">Sem votos por bairro para o filtro.</p>
            ) : (
              <>
                <div className="overflow-x-auto border rounded-lg max-h-[400px]">
                  <table className="min-w-full bg-white text-sm">
                    <thead className="bg-gray-50 border-b text-left text-gray-500 sticky top-0">
                      <tr>
                        <th className="py-2 px-4 font-semibold">Bairro</th>
                        <th className="py-2 px-4 font-semibold">Ano</th>
                        <th className="py-2 px-4 font-semibold">Candidato</th>
                        <th className="py-2 px-4 font-semibold text-right">Votos</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dfBairro.map((r, i) => (
                        <tr key={i} className="border-b last:border-0 hover:bg-gray-50">
                          <td className="py-2 px-4 text-[#0b2545] font-medium">{r.bairro}</td>
                          <td className="py-2 px-4 text-gray-500">{ano}</td>
                          <td className="py-2 px-4 text-gray-500">{r.nm_votavel}</td>
                          <td className="py-2 px-4 text-right font-bold text-[#0b2545]">{fmtInt(r.votos)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <p className="text-sm text-gray-500 mt-2">Total: <strong>{fmtInt(dfBairro.reduce((a, c) => a + c.votos, 0))}</strong></p>
              </>
            )}
          </div>

          <div>
            <h4 className="font-semibold text-lg text-[#0b2545] mb-3">Local</h4>
            {dfLocal.length === 0 ? (
              <p className="text-gray-500 text-sm">Sem votos por local para o filtro.</p>
            ) : (
              <>
                <div className="overflow-x-auto border rounded-lg max-h-[400px]">
                  <table className="min-w-full bg-white text-sm">
                    <thead className="bg-gray-50 border-b text-left text-gray-500 sticky top-0">
                      <tr>
                        <th className="py-2 px-4 font-semibold">Local</th>
                        <th className="py-2 px-4 font-semibold">Ano</th>
                        <th className="py-2 px-4 font-semibold">Candidato</th>
                        <th className="py-2 px-4 font-semibold text-right">Votos</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dfLocal.map((r, i) => (
                        <tr key={i} className="border-b last:border-0 hover:bg-gray-50">
                          <td className="py-2 px-4 text-[#0b2545] font-medium">{r.local}</td>
                          <td className="py-2 px-4 text-gray-500">{ano}</td>
                          <td className="py-2 px-4 text-gray-500">{r.nm_votavel}</td>
                          <td className="py-2 px-4 text-right font-bold text-[#0b2545]">{fmtInt(r.votos)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <p className="text-sm text-gray-500 mt-2">Total: <strong>{fmtInt(dfLocal.reduce((a, c) => a + c.votos, 0))}</strong></p>
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
}
