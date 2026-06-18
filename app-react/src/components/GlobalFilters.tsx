'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams, usePathname } from 'next/navigation';
import { listarAnos, listarUfs, listarMunicipios, listarCargos, listarCandidatos } from '@/app/actions';
import { Button } from '@/components/ui/button';

type FilterState = {
  ano: number | null;
  uf: string | null;
  municipio: string | null;
  cargo: string | null;
  candidato: string | null;
};

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
  const [municipios, setMunicipios] = useState<any[]>([]);
  const [cargos, setCargos] = useState<any[]>([]);
  const [candidatos, setCandidatos] = useState<any[]>([]);

  useEffect(() => {
    setDraft(filtersFromParams(searchParams));
  }, [searchParams]);

  const updateDraft = (key: keyof FilterState, value: string | null) => {
    setDraft((prev) => {
      const next = { ...prev };

      if (key === 'ano') {
        next.ano = value ? Number(value) : null;
        next.uf = null;
        next.municipio = null;
        next.cargo = null;
        next.candidato = null;
      } else if (key === 'uf') {
        next.uf = value;
        next.municipio = null;
        next.cargo = null;
        next.candidato = null;
      } else if (key === 'municipio') {
        next.municipio = value;
        next.cargo = null;
        next.candidato = null;
      } else if (key === 'cargo') {
        next.cargo = value;
        next.candidato = null;
      } else if (key === 'candidato') {
        next.candidato = value;
      }

      return next;
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
    <div className="flex flex-wrap gap-4 items-end mb-6 p-4 bg-slate-50 border rounded-lg">
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
        <select
          className="border rounded p-2 text-sm bg-white max-w-[200px]"
          value={draft.municipio || ''}
          onChange={(e) => updateDraft('municipio', e.target.value || null)}
          disabled={!draft.uf}
        >
          <option value="">{isMunicipal ? '— Selecione' : '— (eleição geral)'}</option>
          {municipios.map((m) => (
            <option key={m.cd} value={m.cd}>
              {m.nm}
            </option>
          ))}
        </select>
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
        <select
          className="border rounded p-2 text-sm bg-white max-w-[300px]"
          value={draft.candidato || ''}
          onChange={(e) => updateDraft('candidato', e.target.value || null)}
          disabled={!draft.cargo}
        >
          <option value="">— Selecione</option>
          {candidatos.map((c) => (
            <option key={c.nr} value={c.nr}>
              {c.nr} - {c.nm} ({c.sg_partido})
            </option>
          ))}
        </select>
      </div>

      <Button onClick={applyFilters} disabled={!canApply} className="h-[38px] px-6">
        Filtrar
      </Button>
    </div>
  );
}
