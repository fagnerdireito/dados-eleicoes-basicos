'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { useRouter, useSearchParams, usePathname } from 'next/navigation';
import { listarAnos, listarUfs, listarMunicipios, listarCargos, listarCandidatos } from '@/app/actions';
import { Button } from '@/components/ui/button';
import { useNavigationLoading } from '@/components/NavigationLoading';

type CandidatoOption = {
  nr: string;
  nm: string;
  sg_partido?: string;
};

type MunicipioOption = {
  cd: string;
  nm: string;
};

type FilterState = {
  ano: number | null;
  uf: string | null;
  municipio: string | null;
  cargo: string | null;
  candidato: string | null;
};

type QueryHistoryEntry = {
  label: string;
  params: {
    ano: string;
    uf: string;
    municipio?: string;
    cargo?: string;
    candidato?: string;
    tab?: string;
  };
};

const QUERY_HISTORY_KEY = 'dados-eleicoes-consultas';
const MAX_QUERY_HISTORY = 5;

function firstName(fullName: string) {
  return fullName.trim().split(/\s+/)[0]?.toLowerCase() ?? fullName.toLowerCase();
}

function historyParamsKey(params: QueryHistoryEntry['params']) {
  return [params.ano, params.uf, params.municipio ?? '', params.cargo ?? '', params.candidato ?? ''].join('|');
}

function loadQueryHistory(): QueryHistoryEntry[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = localStorage.getItem(QUERY_HISTORY_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as QueryHistoryEntry[];
    return Array.isArray(parsed) ? parsed.slice(0, MAX_QUERY_HISTORY) : [];
  } catch {
    return [];
  }
}

function saveQueryHistory(entries: QueryHistoryEntry[]) {
  localStorage.setItem(QUERY_HISTORY_KEY, JSON.stringify(entries.slice(0, MAX_QUERY_HISTORY)));
}

function buildHistoryLabel(
  draft: FilterState,
  municipios: MunicipioOption[],
  cargos: { cd: string; ds: string }[],
  candidatos: CandidatoOption[],
): string | null {
  if (!draft.ano || !draft.uf) return null;

  const parts: string[] = [String(draft.ano), draft.uf];

  if (draft.municipio) {
    const m = municipios.find((x) => x.cd === draft.municipio);
    parts.push((m?.nm ?? draft.municipio).toLowerCase());
  }
  if (draft.cargo) {
    const c = cargos.find((x) => String(x.cd) === draft.cargo);
    parts.push((c?.ds ?? draft.cargo).toLowerCase());
  }
  if (draft.candidato) {
    const cand = candidatos.find((x) => String(x.nr) === draft.candidato);
    parts.push(cand ? firstName(cand.nm) : draft.candidato);
  }

  return parts.join(' · ');
}

function formatCandidatoLabel(c: { nm: string; sg_partido?: string }) {
  return c.sg_partido ? `${c.nm} (${c.sg_partido})` : c.nm;
}

function SearchableSelect({
  options,
  value,
  onChange,
  disabled,
  placeholder = '— Selecione',
  getOptionValue,
  getOptionLabel,
  className = 'w-full min-w-[200px] max-w-[280px]',
  clearOnFocus = false,
}: {
  options: { [key: string]: string }[];
  value: string | null;
  onChange: (value: string | null) => void;
  disabled?: boolean;
  placeholder?: string;
  getOptionValue: (option: { [key: string]: string }) => string;
  getOptionLabel: (option: { [key: string]: string }) => string;
  className?: string;
  clearOnFocus?: boolean;
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (document.activeElement === inputRef.current) return;

    if (value) {
      const selected = options.find((option) => getOptionValue(option) === value);
      setQuery(selected ? getOptionLabel(selected) : '');
    } else {
      setQuery('');
    }
  }, [value, options, getOptionValue, getOptionLabel]);

  const commitQuery = useCallback(() => {
    const trimmed = query.trim();

    if (!trimmed) {
      if (clearOnFocus && value) {
        const selected = options.find((option) => getOptionValue(option) === value);
        setQuery(selected ? getOptionLabel(selected) : '');
        return;
      }

      if (value !== null) {
        onChange(null);
      }
      setQuery('');
      return;
    }

    const exact = options.find(
      (option) => getOptionLabel(option).toLowerCase() === trimmed.toLowerCase(),
    );

    if (exact) {
      const nextValue = getOptionValue(exact);
      if (nextValue !== value) {
        onChange(nextValue);
      }
      setQuery(getOptionLabel(exact));
      return;
    }

    if (value) {
      const selected = options.find((option) => getOptionValue(option) === value);
      setQuery(selected ? getOptionLabel(selected) : '');
      return;
    }

    setQuery('');
  }, [query, value, options, onChange, getOptionValue, getOptionLabel, clearOnFocus]);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        commitQuery();
        setOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [commitQuery]);

  const filtered = options.filter((option) => {
    if (!query.trim()) return true;
    return getOptionLabel(option).toLowerCase().includes(query.toLowerCase());
  });

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <input
        ref={inputRef}
        type="text"
        role="combobox"
        aria-expanded={open}
        aria-autocomplete="list"
        className="w-full rounded-md border bg-white p-2 text-sm disabled:cursor-not-allowed disabled:opacity-50"
        placeholder={placeholder}
        value={query}
        disabled={disabled}
        autoComplete="off"
        onChange={(e) => {
          setQuery(e.target.value);
          setOpen(true);
        }}
        onFocus={() => {
          setOpen(true);
          if (clearOnFocus) {
            setQuery('');
          } else {
            inputRef.current?.select();
          }
        }}
        onBlur={() => {
          commitQuery();
          setOpen(false);
        }}
        onKeyDown={(e) => {
          if (e.key === 'Escape') {
            commitQuery();
            setOpen(false);
            inputRef.current?.blur();
          }
        }}
      />

      {open && !disabled && filtered.length > 0 && (
        <ul
          role="listbox"
          className="absolute z-50 mt-1 max-h-56 w-full overflow-y-auto rounded-md border bg-white shadow-lg"
        >
          {filtered.map((option) => {
            const optionValue = getOptionValue(option);
            return (
              <li key={optionValue} role="option">
                <button
                  type="button"
                  className="w-full px-3 py-2 text-left text-sm hover:bg-slate-100"
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => {
                    if (optionValue !== value) {
                      onChange(optionValue);
                    }
                    setQuery(getOptionLabel(option));
                    setOpen(false);
                  }}
                >
                  {getOptionLabel(option)}
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

function CandidatoAutocomplete({
  candidatos,
  value,
  onChange,
  disabled,
}: {
  candidatos: CandidatoOption[];
  value: string | null;
  onChange: (nr: string | null) => void;
  disabled?: boolean;
}) {
  return (
    <SearchableSelect
      options={candidatos}
      value={value}
      onChange={onChange}
      disabled={disabled}
      getOptionValue={(c) => String(c.nr)}
      getOptionLabel={(c) => formatCandidatoLabel(c as CandidatoOption)}
      className="w-full min-w-[320px] max-w-[420px]"
      clearOnFocus
    />
  );
}

function filtersFromParams(searchParams: URLSearchParams): FilterState {
  return {
    ano: searchParams.get('ano') ? Number(searchParams.get('ano')) : null,
    uf: searchParams.get('uf'),
    municipio: searchParams.get('municipio'),
    cargo: searchParams.get('cargo'),
    candidato: searchParams.get('candidato'),
  };
}

export function GlobalFilters() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { startNavigation } = useNavigationLoading();

  const applied = filtersFromParams(searchParams);
  const [draft, setDraft] = useState<FilterState>(applied);

  const [anos, setAnos] = useState<number[]>([]);
  const [ufs, setUfs] = useState<string[]>([]);
  const [municipios, setMunicipios] = useState<MunicipioOption[]>([]);
  const [cargos, setCargos] = useState<any[]>([]);
  const [candidatos, setCandidatos] = useState<CandidatoOption[]>([]);
  const [queryHistory, setQueryHistory] = useState<QueryHistoryEntry[]>([]);

  useEffect(() => {
    setQueryHistory(loadQueryHistory());
  }, []);

  useEffect(() => {
    setDraft(filtersFromParams(searchParams));
  }, [searchParams]);

  const updateDraft = (key: keyof FilterState, value: string | null) => {
    setDraft((prev) => {
      if (key === 'ano') {
        const nextAno = value ? Number(value) : null;
        if (nextAno === prev.ano) return prev;

        return {
          ...prev,
          ano: nextAno,
          uf: null,
          municipio: null,
          cargo: null,
          candidato: null,
        };
      }

      if (key === 'uf') {
        if (value === prev.uf) return prev;

        return {
          ...prev,
          uf: value,
          municipio: null,
          cargo: null,
          candidato: null,
        };
      }

      if (key === 'municipio') {
        if (value === prev.municipio) return prev;

        // Trocar a cidade (eleição geral ou municipal) não reinicia cargo e
        // candidato — mantém os filtros já escolhidos.
        return {
          ...prev,
          municipio: value,
        };
      }

      if (key === 'cargo') {
        if (value === prev.cargo) return prev;

        return {
          ...prev,
          cargo: value,
          candidato: null,
        };
      }

      if (key === 'candidato') {
        if (value === prev.candidato) return prev;

        return {
          ...prev,
          candidato: value,
        };
      }

      return prev;
    });
  };

  const addToHistory = useCallback((entry: QueryHistoryEntry) => {
    setQueryHistory((prev) => {
      const key = historyParamsKey(entry.params);
      const next = [entry, ...prev.filter((item) => historyParamsKey(item.params) !== key)];
      saveQueryHistory(next);
      return next;
    });
  }, []);

  const applyFilters = () => {
    const params = new URLSearchParams(searchParams.toString());

    const setOrDelete = (key: string, value: string | number | null) => {
      if (value) {
        params.set(key, String(value));
      } else {
        params.delete(key);
      }
    };

    setOrDelete('ano', draft.ano);
    setOrDelete('uf', draft.uf);
    setOrDelete('municipio', draft.municipio);
    setOrDelete('cargo', draft.cargo);
    setOrDelete('candidato', draft.candidato);

    const label = buildHistoryLabel(draft, municipios, cargos, candidatos);
    if (label && draft.ano && draft.uf) {
      addToHistory({
        label,
        params: {
          ano: String(draft.ano),
          uf: draft.uf,
          ...(draft.municipio ? { municipio: draft.municipio } : {}),
          ...(draft.cargo ? { cargo: draft.cargo } : {}),
          ...(draft.candidato ? { candidato: draft.candidato } : {}),
          ...(searchParams.get('tab') ? { tab: searchParams.get('tab')! } : {}),
        },
      });
    }

    startNavigation(() => {
      router.push(`${pathname}?${params.toString()}`);
    });
  };

  const restoreQuery = (entry: QueryHistoryEntry) => {
    const params = new URLSearchParams();
    params.set('ano', entry.params.ano);
    params.set('uf', entry.params.uf);
    if (entry.params.municipio) params.set('municipio', entry.params.municipio);
    if (entry.params.cargo) params.set('cargo', entry.params.cargo);
    if (entry.params.candidato) params.set('candidato', entry.params.candidato);
    if (entry.params.tab) params.set('tab', entry.params.tab);
    startNavigation(() => {
      router.push(`${pathname}?${params.toString()}`);
    });
  };

  useEffect(() => {
    listarAnos().then(setAnos);
  }, []);

  useEffect(() => {
    if (draft.ano) {
      listarUfs(draft.ano).then(setUfs);
    } else {
      setUfs([]);
    }
  }, [draft.ano]);

  useEffect(() => {
    if (draft.ano && draft.uf) {
      listarMunicipios(draft.ano, draft.uf).then(setMunicipios);
    } else {
      setMunicipios([]);
    }
  }, [draft.ano, draft.uf]);

  useEffect(() => {
    if (draft.ano && draft.uf) {
      listarCargos(draft.ano, draft.uf, draft.municipio || undefined).then(setCargos);
    } else {
      setCargos([]);
    }
  }, [draft.ano, draft.uf, draft.municipio]);

  useEffect(() => {
    if (draft.ano && draft.uf && draft.cargo) {
      listarCandidatos(draft.ano, draft.uf, draft.cargo, draft.municipio || undefined).then(setCandidatos);
    } else {
      setCandidatos([]);
    }
  }, [draft.ano, draft.uf, draft.cargo, draft.municipio]);

  const isMunicipal = draft.ano ? draft.ano % 4 === 0 && draft.ano >= 2020 : false;

  const hasPendingChanges =
    draft.ano !== applied.ano ||
    draft.uf !== applied.uf ||
    draft.municipio !== applied.municipio ||
    draft.cargo !== applied.cargo ||
    draft.candidato !== applied.candidato;

  const canApply = Boolean(draft.ano && draft.uf && hasPendingChanges);

  return (
    <div className="relative z-20 mb-5 flex flex-col gap-2 print:hidden">
      <div className="glass-panel relative z-30 flex flex-wrap items-end gap-4 rounded-xl p-4">
      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Eleição/Ano</label>
        <select
          className="rounded-md border bg-white p-2 text-sm"
          value={draft.ano || ''}
          onChange={(e) => updateDraft('ano', e.target.value || null)}
        >
          <option value="">— Selecione</option>
          {anos.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">UF</label>
        <select
          className="min-w-[80px] rounded-md border bg-white p-2 text-sm"
          value={draft.uf || ''}
          onChange={(e) => updateDraft('uf', e.target.value || null)}
          disabled={!draft.ano}
        >
          <option value="">— Selecione</option>
          {ufs.map((uf) => (
            <option key={uf} value={uf}>
              {uf}
            </option>
          ))}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">
          {isMunicipal ? 'Município*' : 'Cidade (opcional)'}
        </label>
        <SearchableSelect
          options={municipios}
          value={draft.municipio}
          onChange={(cd) => updateDraft('municipio', cd)}
          disabled={!draft.uf}
          placeholder={isMunicipal ? '— Selecione' : '— (eleição geral)'}
          getOptionValue={(m) => m.cd}
          getOptionLabel={(m) => m.nm}
          clearOnFocus
        />
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Cargo</label>
        <select
          className="max-w-[200px] rounded-md border bg-white p-2 text-sm"
          value={draft.cargo || ''}
          onChange={(e) => updateDraft('cargo', e.target.value || null)}
          disabled={!draft.uf || (isMunicipal && !draft.municipio)}
        >
          <option value="">— Selecione</option>
          {cargos.map((c) => (
            <option key={c.cd} value={c.cd}>
              {c.ds}
            </option>
          ))}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Candidato</label>
        <CandidatoAutocomplete
          candidatos={candidatos}
          value={draft.candidato}
          onChange={(nr) => updateDraft('candidato', nr)}
          disabled={!draft.cargo}
        />
      </div>

      <Button
        variant="outline"
        onClick={applyFilters}
        disabled={!canApply}
        className="glass-action-btn h-[38px] rounded-lg px-6 font-semibold text-blue-600 hover:text-blue-800 active:translate-y-0"
      >
        Filtrar
      </Button>
      </div>

      {queryHistory.length > 0 && (
        <div className="relative z-10 overflow-x-auto px-1 mt-2 [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
          <div className="flex w-max min-w-full flex-nowrap gap-2 pb-0.5">
          {queryHistory.map((entry) => (
            <button
              key={historyParamsKey(entry.params)}
              type="button"
              onClick={() => restoreQuery(entry)}
              title="Restaurar consulta"
              className="inline-flex shrink-0 items-center rounded-full border border-indigo-200/80 bg-white/95 px-3 py-1 text-xs font-medium whitespace-nowrap text-indigo-900 shadow-md backdrop-blur-sm transition-colors hover:border-indigo-300 hover:bg-indigo-50"
            >
              {entry.label}
            </button>
          ))}
          </div>
        </div>
      )}
    </div>
  );
}
