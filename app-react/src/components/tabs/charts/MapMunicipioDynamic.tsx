'use client';

import dynamic from 'next/dynamic';

const MapMunicipio = dynamic(
  () => import('./MapMunicipio').then((mod) => mod.MapMunicipio),
  { ssr: false, loading: () => <div className="flex h-[520px] items-center justify-center rounded-lg bg-gray-100">Carregando mapa...</div> },
);

export function MapMunicipioDynamicWrapper({
  uf,
  cdIbge,
  municipioNome,
  data,
}: {
  uf: string;
  cdIbge?: string;
  municipioNome: string;
  data: any[];
}) {
  return <MapMunicipio uf={uf} cdIbge={cdIbge} municipioNome={municipioNome} data={data} />;
}
