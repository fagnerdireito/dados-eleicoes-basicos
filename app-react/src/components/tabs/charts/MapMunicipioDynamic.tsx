'use client';

import dynamic from 'next/dynamic';

const MapMunicipio = dynamic(
  () => import('./MapMunicipio').then(mod => mod.MapMunicipio),
  { ssr: false, loading: () => <div className="h-[520px] flex items-center justify-center bg-gray-100 rounded">Carregando mapa...</div> }
);

export function MapMunicipioDynamicWrapper({ data }: { data: any[] }) {
  return <MapMunicipio data={data} />;
}
