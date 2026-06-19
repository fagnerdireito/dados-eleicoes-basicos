'use client';

import Link from 'next/link';
import { useMemo, useState } from 'react';
import { Search, X } from 'lucide-react';

const CAND_COLORS = ['#1f6feb', '#ef4444', '#22c55e', '#f59e0b'];

type Candidato = {
  nr: string;
  nm: string;
  sg_partido?: string;
  votos?: number;
};

function formatLabel(c: Candidato) {
  return c.sg_partido ? `${c.nm} (${c.sg_partido})` : c.nm;
}

function buildHref(searchParams: Record<string, string | undefined>, nextNrs: string[]) {
  const params = new URLSearchParams();
  Object.entries(searchParams).forEach(([key, value]) => {
    if (value && key !== 'cands') params.set(key, value);
  });
  const primary = searchParams.candidato;
  const isDefaultOnly = nextNrs.length === 1 && nextNrs[0] === primary;
  if (!isDefaultOnly && nextNrs.length > 0) {
    params.set('cands', nextNrs.join(','));
  }
  return `/?${params.toString()}`;
}

export function ComparativoCandidatoPicker({
  candidatos,
  selectedNrs,
  searchParams,
}: {
  candidatos: Candidato[];
  selectedNrs: string[];
  searchParams: Record<string, string | undefined>;
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);

  const labels = useMemo(
    () => Object.fromEntries(candidatos.map((c) => [c.nr, formatLabel(c)])),
    [candidatos],
  );

  const available = useMemo(() => {
    const q = query.trim().toLowerCase();
    return candidatos.filter((c) => {
      if (selectedNrs.includes(c.nr)) return false;
      if (!q) return true;
      return formatLabel(c).toLowerCase().includes(q) || c.nr.includes(q);
    });
  }, [candidatos, selectedNrs, query]);

  const canAdd = selectedNrs.length < 4;

  return (
    <div className="rounded-lg border bg-gray-50 p-4">
      <div className="mb-2 text-xs font-bold uppercase tracking-wide text-gray-500">
        Candidatos para comparar (até 4)
      </div>

      {selectedNrs.length > 0 && (
        <div className="mb-3 flex flex-wrap gap-2">
          {selectedNrs.map((nr, i) => {
            const color = CAND_COLORS[i % CAND_COLORS.length];
            const canRemove = selectedNrs.length > 1;
            const nextNrs = selectedNrs.filter((n) => n !== nr);

            return (
              <div
                key={nr}
                className="flex items-center gap-1 rounded-full border bg-white py-1 pl-3 pr-1 text-sm"
                style={{ borderColor: color }}
              >
                <span className="font-medium" style={{ color }}>
                  {labels[nr] ?? nr}
                </span>
                {canRemove && (
                  <Link
                    href={buildHref(searchParams, nextNrs)}
                    className="rounded-full p-0.5 text-gray-400 hover:bg-gray-100 hover:text-gray-600"
                    title="Remover candidato"
                  >
                    <X className="h-3.5 w-3.5" />
                  </Link>
                )}
              </div>
            );
          })}
        </div>
      )}

      {canAdd ? (
        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input
            type="search"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setOpen(true);
            }}
            onFocus={() => setOpen(true)}
            placeholder="Buscar outro candidato para comparar..."
            className="w-full rounded-md border border-gray-200 bg-white py-2 pl-9 pr-3 text-sm text-[#0b2545] placeholder:text-gray-400 focus:border-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400/30"
          />

          {open && (
            <>
              <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} />
              <div className="absolute z-20 mt-1 max-h-60 w-full overflow-y-auto rounded-md border bg-white shadow-lg">
                {available.length === 0 ? (
                  <p className="px-3 py-2 text-sm text-gray-500">Nenhum candidato encontrado.</p>
                ) : (
                  available.slice(0, 50).map((c) => {
                    const nextNrs = [...selectedNrs, c.nr];
                    return (
                      <Link
                        key={c.nr}
                        href={buildHref(searchParams, nextNrs)}
                        onClick={() => {
                          setQuery('');
                          setOpen(false);
                        }}
                        className="block px-3 py-2 text-sm text-[#0b2545] hover:bg-gray-50"
                      >
                        <span className="font-medium">{c.nm}</span>
                        {c.sg_partido && (
                          <span className="ml-1 text-gray-500">({c.sg_partido})</span>
                        )}
                      </Link>
                    );
                  })
                )}
              </div>
            </>
          )}
        </div>
      ) : (
        <p className="text-sm text-amber-700">Máximo de 4 candidatos atingido.</p>
      )}
    </div>
  );
}
