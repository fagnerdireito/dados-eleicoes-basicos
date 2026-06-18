'use client';

import dynamic from 'next/dynamic';

const MapEstado = dynamic(
  () => import('./MapEstado').then(mod => mod.MapEstado),
  { ssr: false, loading: () => <div className="h-[600px] flex items-center justify-center bg-gray-100 rounded">Carregando mapa...</div> }
);

export function MapEstadoDynamicWrapper({ uf, data }: { uf: string, data: any[] }) {
  return <MapEstado uf={uf} data={data} />;
}
