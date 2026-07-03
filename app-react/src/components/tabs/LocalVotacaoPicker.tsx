'use client';

import { usePathname, useRouter } from 'next/navigation';
import { useMemo, useState } from 'react';
import { Search } from 'lucide-react';
import { normalizeLocalName } from '@/lib/utils';
import { useNavigationLoading } from '@/components/NavigationLoading';

type LocalItem = {
  nr_local: string;
  nome: string;
};

export function LocalVotacaoPicker({
  locais,
  selectedNome,
  searchParams,
}: {
  locais: LocalItem[];
  selectedNome: string;
  searchParams: Record<string, string | undefined>;
}) {
  const [query, setQuery] = useState('');
  const selectedNormalized = normalizeLocalName(selectedNome);
  const router = useRouter();
  const pathname = usePathname();
  const { isPending, startNavigation } = useNavigationLoading();

  const filtered = useMemo(() => {
    const q = normalizeLocalName(query.trim());
    if (!q) return locais;
    return locais.filter((l) => normalizeLocalName(l.nome).includes(q));
  }, [locais, query]);

  const irParaLocal = (nome: string, isActive: boolean) => {
    if (isActive) return;
    const params = new URLSearchParams();
    Object.entries(searchParams).forEach(([key, value]) => {
      if (value) params.set(key, value);
    });
    params.set('local', nome);
    startNavigation(() => {
      router.push(`${pathname}?${params.toString()}`);
    });
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="relative">
        <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Buscar local por nome..."
          className="w-full rounded-md border border-gray-200 bg-white py-2 pl-9 pr-3 text-sm text-[#0b2545] placeholder:text-gray-400 focus:border-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400/30"
        />
      </div>

      <div
        className={`flex max-h-[600px] flex-col gap-1 overflow-y-auto rounded-md border bg-gray-50 p-2 pr-2 transition-opacity duration-200 ${
          isPending ? 'pointer-events-none opacity-40' : ''
        }`}
      >
        {filtered.length === 0 ? (
          <p className="px-2 py-3 text-sm text-gray-500">Nenhum local encontrado.</p>
        ) : (
          filtered.map((l) => {
            const isActive = normalizeLocalName(l.nome) === selectedNormalized;

            return (
              <button
                key={l.nome}
                type="button"
                onClick={() => irParaLocal(l.nome, isActive)}
                className={`block w-full rounded-md p-2 text-left text-sm transition-colors ${isActive ? 'bg-[#0b2545] text-white' : 'text-gray-700 hover:bg-gray-200'}`}
                title={`${l.nome} (${l.nr_local})`}
              >
                <div className="truncate font-semibold">{l.nome}</div>
                <div className={`text-xs ${isActive ? 'text-gray-300' : 'text-gray-500'}`}>
                  Local {l.nr_local}
                </div>
              </button>
            );
          })
        )}
      </div>
    </div>
  );
}
