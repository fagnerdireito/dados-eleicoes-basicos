'use server';

// Server Actions usadas pelos filtros globais. A lógica de banco fica em
// src/lib/queries-filtros.ts (com "use cache"), então tanto os Client
// Components (via Server Action) quanto os Server Components reaproveitam o
// mesmo cache para o mesmo filtro, sem novas consultas ao banco.
import * as q from '@/lib/queries-filtros';

export async function checkUsaCatalogoFiltros() {
  return q.checkUsaCatalogoFiltros();
}

export async function listarAnos() {
  return q.listarAnos();
}

export async function listarUfs(ano: number) {
  return q.listarUfs(ano);
}

export async function listarMunicipios(ano: number, uf: string) {
  return q.listarMunicipios(ano, uf);
}

export async function listarCargos(ano: number, uf: string, cdMunicipio?: string) {
  return q.listarCargos(ano, uf, cdMunicipio);
}

export async function listarCandidatos(ano: number, uf: string, cdCargo: string, cdMunicipio?: string, limit = 200) {
  return q.listarCandidatos(ano, uf, cdCargo, cdMunicipio, limit);
}
