'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams, usePathname } from 'next/navigation';
import { listarAnos, listarUfs, listarMunicipios, listarCargos, listarCandidatos } from '@/app/actions';

export function GlobalFilters() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [anos, setAnos] = useState<number[]>([]);
  const [ufs, setUfs] = useState<string[]>([]);
  const [municipios, setMunicipios] = useState<any[]>([]);
  const [cargos, setCargos] = useState<any[]>([]);
  const [candidatos, setCandidatos] = useState<any[]>([]);

  // Current selections
  const currentAno = searchParams.get('ano') ? Number(searchParams.get('ano')) : null;
  const currentUf = searchParams.get('uf');
  const currentMunicipio = searchParams.get('municipio');
  const currentCargo = searchParams.get('cargo');
  const currentCandidato = searchParams.get('candidato');

  const updateParam = (key: string, value: string | null) => {
    const params = new URLSearchParams(searchParams.toString());
    if (value) {
      params.set(key, value);
    } else {
      params.delete(key);
    }
    
    // Clear dependencies
    if (key === 'ano') {
      params.delete('uf');
      params.delete('municipio');
      params.delete('cargo');
      params.delete('candidato');
    } else if (key === 'uf') {
      params.delete('municipio');
      params.delete('cargo');
      params.delete('candidato');
    } else if (key === 'municipio') {
      params.delete('cargo');
      params.delete('candidato');
    } else if (key === 'cargo') {
      params.delete('candidato');
    }

    router.push(`${pathname}?${params.toString()}`);
  };

  useEffect(() => {
    listarAnos().then(res => {
      setAnos(res);
      if (!currentAno && res.length > 0) {
        updateParam('ano', res[res.length - 1].toString());
      }
    });
  }, []);

  useEffect(() => {
    if (currentAno) {
      listarUfs(currentAno).then(res => {
        setUfs(res);
        if (!currentUf && res.length > 0) {
          updateParam('uf', res[0]);
        }
      });
    } else {
      setUfs([]);
    }
  }, [currentAno]);

  useEffect(() => {
    if (currentAno && currentUf) {
      listarMunicipios(currentAno, currentUf).then(res => {
        setMunicipios(res);
        // Is municipal election?
        const isMunicipal = currentAno % 4 === 0 && currentAno >= 2020;
        if (!currentMunicipio && res.length > 0 && isMunicipal) {
          updateParam('municipio', res[0].cd);
        }
      });
    } else {
      setMunicipios([]);
    }
  }, [currentAno, currentUf]);

  useEffect(() => {
    if (currentAno && currentUf) {
      listarCargos(currentAno, currentUf, currentMunicipio || undefined).then(res => {
        setCargos(res);
        if (!currentCargo && res.length > 0) {
          updateParam('cargo', res[0].cd);
        }
      });
    } else {
      setCargos([]);
    }
  }, [currentAno, currentUf, currentMunicipio]);

  useEffect(() => {
    if (currentAno && currentUf && currentCargo) {
      listarCandidatos(currentAno, currentUf, currentCargo, currentMunicipio || undefined).then(res => {
        setCandidatos(res);
        if (!currentCandidato && res.length > 0) {
          updateParam('candidato', res[0].nr);
        }
      });
    } else {
      setCandidatos([]);
    }
  }, [currentAno, currentUf, currentCargo, currentMunicipio]);

  const isMunicipal = currentAno ? currentAno % 4 === 0 && currentAno >= 2020 : false;

  return (
    <div className="flex flex-wrap gap-4 items-center mb-6 p-4 bg-slate-50 border rounded-lg">
      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Eleição/Ano</label>
        <select 
          className="border rounded p-2 text-sm bg-white"
          value={currentAno || ''} 
          onChange={(e) => updateParam('ano', e.target.value)}
        >
          {anos.map(a => <option key={a} value={a}>{a}</option>)}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">UF</label>
        <select 
          className="border rounded p-2 text-sm bg-white min-w-[80px]"
          value={currentUf || ''} 
          onChange={(e) => updateParam('uf', e.target.value)}
          disabled={!currentAno}
        >
          {ufs.map(uf => <option key={uf} value={uf}>{uf}</option>)}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">{isMunicipal ? 'Município*' : 'Cidade (opcional)'}</label>
        <select 
          className="border rounded p-2 text-sm bg-white max-w-[200px]"
          value={currentMunicipio || ''} 
          onChange={(e) => updateParam('municipio', e.target.value)}
          disabled={!currentUf}
        >
          {!isMunicipal && <option value="">— (eleição geral)</option>}
          {municipios.map(m => <option key={m.cd} value={m.cd}>{m.nm}</option>)}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Cargo</label>
        <select 
          className="border rounded p-2 text-sm bg-white max-w-[200px]"
          value={currentCargo || ''} 
          onChange={(e) => updateParam('cargo', e.target.value)}
          disabled={!currentUf || (isMunicipal && !currentMunicipio)}
        >
          {cargos.map(c => <option key={c.cd} value={c.cd}>{c.ds}</option>)}
        </select>
      </div>

      <div className="flex flex-col">
        <label className="text-sm font-medium mb-1">Candidato</label>
        <select 
          className="border rounded p-2 text-sm bg-white max-w-[300px]"
          value={currentCandidato || ''} 
          onChange={(e) => updateParam('candidato', e.target.value)}
          disabled={!currentCargo}
        >
          {candidatos.map(c => (
            <option key={c.nr} value={c.nr}>
              {c.nr} - {c.nm} ({c.sg_partido})
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
