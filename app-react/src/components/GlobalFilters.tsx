'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { useRouter, useSearchParams, usePathname } from 'next/navigation';
import { listarAnos, listarUfs, listarMunicipios, listarCargos, listarCandidatos } from '@/app/actions';
import { Button } from '@/components/ui/button';

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
        className="w-full rounded border bg-white p-2 text-sm disabled:cursor-not-allowed disabled:opacity-50"
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
          className="absolute z-20 mt-1 max-h-56 w-full overflow-y-auto rounded-md border bg-white shadow-lg"
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

  const applied = filtersFromParams(searchParams);
  const [draft, setDraft] = useState<FilterState>(applied);

  const [anos, setAnos] = useState<number[]>([]);
  const [ufs, setUfs] = useState<string[]>([]);
  const [municipios, setMunicipios] = useState<MunicipioOption[]>([]);
  const [cargos, setCargos] = useState<any[]>([]);
  const [candidatos, setCandidatos] = useState<CandidatoOption[]>([]);

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

        return {
          ...prev,
          municipio: value,
          cargo: null,
          candidato: null,
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

    router.push(`${pathname}?${params.toString()}`);
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
    <div className="glass-panel relative z-20 mb-6 flex flex-wrap items-end gap-4 rounded-xl p-4 print:hidden">
      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Eleição/Ano</label>
        <select
          className="border rounded p-2 text-sm bg-white"
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
          className="border rounded p-2 text-sm bg-white min-w-[80px]"
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
          className="border rounded p-2 text-sm bg-white max-w-[200px]"
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
  );
}
