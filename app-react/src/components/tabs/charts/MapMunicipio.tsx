'use client';

import { MapContainer, TileLayer, CircleMarker, Tooltip } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

export function MapMunicipio({ data }: { data: any[] }) {
  if (data.length === 0) return null;

  // Calculate center from data points
  const lats = data.map(d => d.lat);
  const lngs = data.map(d => d.lng);
  
  const latMin = Math.min(...lats);
  const latMax = Math.max(...lats);
  const lngMin = Math.min(...lngs);
  const lngMax = Math.max(...lngs);

  // Fallback for single point
  const centerLat = latMin === latMax ? latMin : (latMin + latMax) / 2;
  const centerLng = lngMin === lngMax ? lngMin : (lngMin + lngMax) / 2;

  const maxVotos = Math.max(...data.map(d => d.votos), 1);

  return (
    <div className="h-[520px] w-full rounded border overflow-hidden">
      <MapContainer 
        bounds={[[latMin - 0.02, lngMin - 0.02], [latMax + 0.02, lngMax + 0.02]]}
        scrollWheelZoom={false} 
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://carto.com/attributions">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
        />
        {data.map((r, i) => {
          const raio = 4 + (r.votos / maxVotos) * 22;
          return (
            <CircleMarker
              key={i}
              center={[r.lat, r.lng]}
              radius={raio}
              pathOptions={{ color: '#1f6feb', fillColor: '#1f6feb', fillOpacity: 0.55 }}
            >
              <Tooltip sticky className="text-sm font-sans">
                <strong>{r.nm_local}</strong><br/>
                {new Intl.NumberFormat('pt-BR').format(r.votos)} votos
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
}
