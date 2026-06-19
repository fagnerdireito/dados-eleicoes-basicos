import { votosPorBairro, votosPorLocalCandidato } from '@/app/actions-tab10';
import { votosCandidatoPorMunicipio } from '@/app/actions-tab2';
import { checkTableExists } from '@/app/actions-tab3';
import { listarCandidatos, listarMunicipios } from '@/app/actions';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function DataTable({
  title,
  total,
  columns,
  rows,
}: {
  title: string;
  total?: string;
  columns: { key: string; label: string; align?: 'left' | 'right' }[];
  rows: Record<string, string | number>[];
}) {
  return (
    <div className="soft-card chart-print-bg overflow-hidden rounded-md! p-2 sm:p-4 md:p-6">
      <h4 className="soft-section-title mb-2 text-base md:mb-4 md:text-md pl-1">{title}</h4>
      <div className="-mx-2 overflow-x-auto sm:mx-0">
        <table className="w-full min-w-[280px] bg-white text-xs md:text-xs">
          <thead className="border-b bg-gray-50">
            <tr>
              {columns.map((col) => (
                <th
                  key={col.key}
                  className={`px-2 py-1.5 font-semibold text-[#0b2545] md:px-4 md:py-3 ${col.align === 'right' ? 'text-right' : 'text-left'}`}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} className="border-b border-gray-100 last:border-0 hover:bg-gray-50/80">
                {columns.map((col) => (
                  <td
                    key={col.key}
                    className={`px-2 py-1.5 md:px-4 md:py-3 ${col.align === 'right' ? 'text-right font-bold text-[#0b2545]' : col.key === columns[0].key ? 'max-w-[7rem] truncate font-medium text-[#0b2545] sm:max-w-none sm:whitespace-normal' : 'text-gray-500'}`}
                    title={col.key === columns[0].key ? String(row[col.key]) : undefined}
                  >
                    {row[col.key]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {total && (
        <p className="soft-footnote mt-2 border-t border-gray-100 pt-2 text-right md:mt-4 md:pt-4">
          Total: <strong>{total}</strong>
        </p>
      )}
    </div>
  );
}

export async function TabVotosBairro({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato) {
    return <div className="soft-subtitle p-2">Selecione Ano, UF, Cargo e Candidato nos filtros acima.</div>;
  }

  const hasLocalVotacao = await checkTableExists('local_votacao');
  if (!hasLocalVotacao) {
    return <div className="soft-alert">Tabela `local_votacao` não encontrada no banco de dados.</div>;
  }

  const anoNum = parseInt(ano, 10);

  const [dfMun, candidatos, municipios] = await Promise.all([
    votosCandidatoPorMunicipio(anoNum, uf, cargo, candidato, municipio),
    listarCandidatos(anoNum, uf, cargo, municipio || undefined),
    listarMunicipios(anoNum, uf),
  ]);

  const c = candidatos.find((item) => String(item.nr) === candidato);
  const candidatoLabel = c ? `${c.nm}${c.sg_partido ? ` (${c.sg_partido})` : ''}` : candidato;
  const municipioNome = municipio
    ? (municipios.find((m) => String(m.cd) === municipio)?.nm ?? municipio)
    : null;

  const totalMun = dfMun.reduce((acc, curr) => acc + curr.votos, 0);

  let dfBairro: Awaited<ReturnType<typeof votosPorBairro>> = [];
  let dfLocal: Awaited<ReturnType<typeof votosPorLocalCandidato>> = [];

  if (municipio) {
    [dfBairro, dfLocal] = await Promise.all([
      votosPorBairro(anoNum, uf, municipio, cargo, candidato),
      votosPorLocalCandidato(anoNum, uf, municipio, cargo, candidato),
    ]);
  }

  const totalBairro = dfBairro.reduce((a, c) => a + c.votos, 0);
  const totalLocal = dfLocal.reduce((a, c) => a + c.votos, 0);

  return (
    <div className="-mx-2 flex flex-col gap-5 px-0 sm:mx-0 md:gap-8">
      <header className="px-1 sm:px-0">
        <h2 className="soft-title">Votos por bairro</h2>
        <p className="soft-subtitle">
          Agregação por bairro e local · {candidatoLabel}
          {municipioNome ? ` · ${municipioNome}` : ` · ${uf}`}
        </p>
      </header>

      {!municipio && (
        <div className="soft-alert mx-1 sm:mx-0">
          Selecione um município nos filtros para ver o detalhamento por bairro e local.
        </div>
      )}

      {dfMun.length > 0 && (
        <section>
          <DataTable
            title="Município"
            total={fmtInt(totalMun)}
            columns={[
              { key: 'nm', label: 'Município' },
              { key: 'ano', label: 'Ano' },
              { key: 'votos', label: 'Votos', align: 'right' },
            ]}
            rows={dfMun.map((r) => ({
              nm: r.nm,
              ano,
              votos: fmtInt(r.votos),
            }))}
          />
        </section>
      )}

      {municipio && (
        <>
          <section>
            {dfBairro.length === 0 ? (
              <div className="soft-card chart-print-bg rounded-md! p-2 sm:p-4 md:p-6">
                <h4 className="soft-section-title mb-2 text-base md:text-lg">Bairro</h4>
                <p className="soft-subtitle">Sem votos por bairro para o filtro.</p>
              </div>
            ) : (
              <DataTable
                title="Bairro"
                total={fmtInt(totalBairro)}
                columns={[
                  { key: 'bairro', label: 'Bairro' },
                  { key: 'ano', label: 'Ano' },
                  { key: 'candidato', label: 'Candidato' },
                  { key: 'votos', label: 'Votos', align: 'right' },
                ]}
                rows={dfBairro.map((r) => ({
                  bairro: r.bairro,
                  ano,
                  candidato: r.nm_votavel,
                  votos: fmtInt(r.votos),
                }))}
              />
            )}
          </section>

          <section>
            {dfLocal.length === 0 ? (
              <div className="soft-card chart-print-bg rounded-md! p-2 sm:p-4 md:p-6">
                <h4 className="soft-section-title mb-2 text-base md:text-lg">Local</h4>
                <p className="soft-subtitle">Sem votos por local para o filtro.</p>
              </div>
            ) : (
              <DataTable
                title="Local"
                total={fmtInt(totalLocal)}
                columns={[
                  { key: 'local', label: 'Local' },
                  { key: 'ano', label: 'Ano' },
                  { key: 'candidato', label: 'Candidato' },
                  { key: 'votos', label: 'Votos', align: 'right' },
                ]}
                rows={dfLocal.map((r) => ({
                  local: r.local,
                  ano,
                  candidato: r.nm_votavel,
                  votos: fmtInt(r.votos),
                }))}
              />
            )}
          </section>
        </>
      )}
    </div>
  );
}
