import { listarMunicipios, listarCargos, listarCandidatos } from '@/app/actions';

export async function TabSumario({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const ano = searchParams.ano || '-';
  const uf = searchParams.uf || '-';
  const { municipio, cargo, candidato } = searchParams;

  let cidade = 'estado inteiro';
  let cargoLabel = cargo || '-';
  let candidatoLabel = candidato || '-';

  if (searchParams.ano && uf && uf !== '-') {
    const anoNum = parseInt(searchParams.ano, 10);

    if (municipio) {
      const municipios = await listarMunicipios(anoNum, uf);
      cidade = municipios.find((m) => String(m.cd) === municipio)?.nm || municipio;
    }

    if (cargo) {
      const cargos = await listarCargos(anoNum, uf, municipio || undefined);
      cargoLabel = cargos.find((c) => String(c.cd) === cargo)?.ds || cargo;
    }

    if (cargo && candidato) {
      const candidatos = await listarCandidatos(anoNum, uf, cargo, municipio || undefined);
      const c = candidatos.find((item) => String(item.nr) === candidato);
      candidatoLabel = c ? `${c.nm}${c.sg_partido ? ` (${c.sg_partido})` : ''}` : candidato;
    }
  }

  const itens = [
    { titulo: "Resumo no município", descricao: "KPIs do candidato no município, posição geral, liderança em locais e composição dos votos." },
    { titulo: "Perfil do eleitorado (UF)", descricao: "Perfil do eleitorado por faixa etária e escolaridade — recorte por UF." },
    { titulo: "Onde estão os votos no estado", descricao: "Mapa dos votos do candidato por município da UF." },
    { titulo: "Onde estão os votos no município", descricao: "Mapa de bolhas por local de votação." },
    { titulo: "Ranking geral no município", descricao: "Top 10 candidatos no município comparando com a eleição anterior." },
    { titulo: "Síntese territorial", descricao: "Quantos locais cada candidato lidera dentro do município." },
    { titulo: "Votos por local de votação", descricao: "Top 10 candidatos por local + totais (válidos, brancos e nulos)." },
    { titulo: "Votos por bairro", descricao: "Agregação por bairro/local com local de votacao." },
  ];

  const filtros = [
    { label: 'Eleição', value: ano, accent: 'purple' as const, narrow: true, barClass: 'bg-[#7b61ff]' },
    { label: 'UF', value: uf, accent: 'blue' as const, narrow: true, barClass: 'bg-[#3b82f6]' },
    { label: 'Cidade', value: cidade, accent: 'green' as const, narrow: false, barClass: 'bg-[#22c55e]' },
    { label: 'Cargo', value: cargoLabel, accent: 'orange' as const, narrow: false, barClass: 'bg-[#f97316]' },
    { label: 'Candidato', value: candidatoLabel, accent: 'pink' as const, narrow: false, barClass: 'bg-[#ff708d]' },
  ];

  const barSegmentClass = (filtro: (typeof filtros)[number]) => {
    const classes = ['summary-bar-segment', filtro.barClass];
    if (filtro.narrow) classes.push('summary-bar-segment--narrow');
    if (filtro.accent === 'pink') classes.push('summary-bar-segment--wide');
    return classes.join(' ');
  };

  const chipClass = (filtro: (typeof filtros)[number]) => {
    let cls = 'summary-chip';
    if (filtro.narrow) cls += ' summary-chip--narrow';
    if (filtro.accent === 'pink') cls += ' summary-chip--candidato';
    return cls;
  };

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Sumário</h2>
        <p className="text-gray-500">Índice das seções/abas do dossiê</p>
      </div>

      <div className="glass-panel chart-print-bg mb-8 rounded-xl p-4">
        <div className="summary-filters-layout">
          <div className="summary-bar mb-4">
            {filtros.map((filtro) => (
              <div key={filtro.label} className={barSegmentClass(filtro)} />
            ))}
          </div>

          <div className="summary-filters-row">
            {filtros.map((filtro) => (
              <div key={filtro.label} className={chipClass(filtro)}>
                <span className={`summary-chip-dot summary-chip-dot--${filtro.accent} mt-1`} />
                <div className="min-w-0">
                  <div className="summary-chip-label">{filtro.label}</div>
                  <div className="summary-chip-value">{filtro.value}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="flex flex-col">
        {itens.map((item, i) => (
          <div key={i} className="flex gap-4 py-3 border-b border-gray-100 last:border-0">
            <div className="text-gray-400 font-mono w-8">{(i + 2).toString().padStart(2, '0')}</div>
            <div>
              <div className="font-semibold text-[#0b2545]">{item.titulo}</div>
              <div className="text-[#7b61ff] text-sm">{item.descricao}</div>
            </div>
          </div>
        ))}
      </div>

      <hr className="my-6 border-gray-200 print:hidden" />
      <p className="text-sm text-gray-400 print:hidden">
        Use as abas no topo para navegar. Os filtros globais (acima das abas) se aplicam a todo o dossiê.
      </p>
    </div>
  );
}
