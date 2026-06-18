export function TabSumario({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const ano = searchParams.ano || '-';
  const uf = searchParams.uf || '-';
  const cidade = searchParams.municipio || 'estado inteiro';
  const cargo = searchParams.cargo || '-';
  const candidato = searchParams.candidato || '-';

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

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h2 className="text-2xl font-bold text-[#0b2545]">Sumário</h2>
        <p className="text-gray-500">Índice das seções/abas do dossiê</p>
      </div>
      
      <p className="text-sm text-gray-500 bg-gray-50 p-3 border rounded">
        Eleição <strong>{ano}</strong> · UF <strong>{uf}</strong> · Cidade <strong>{cidade}</strong> · Cargo <strong>{cargo}</strong> · Candidato foco <strong>{candidato}</strong>
      </p>

      <div className="flex flex-col mt-4">
        {itens.map((item, i) => (
          <div key={i} className="flex gap-4 py-3 border-b border-gray-100 last:border-0">
            <div className="text-gray-400 font-mono w-8">{(i + 2).toString().padStart(2, '0')}</div>
            <div>
              <div className="font-semibold text-[#0b2545]">{item.titulo}</div>
              <div className="text-gray-500 text-sm">{item.descricao}</div>
            </div>
          </div>
        ))}
      </div>

      <hr className="my-6 border-gray-200" />
      <p className="text-sm text-gray-400">
        Use as abas no topo para navegar. Os filtros globais (acima das abas) se aplicam a todo o dossiê.
      </p>
    </div>
  );
}
