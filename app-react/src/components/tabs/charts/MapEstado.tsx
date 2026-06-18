'use client';

import { useEffect, useState } from 'react';
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

export function MapEstado({ uf, data }: { uf: string, data: any[] }) {
  const [geoData, setGeoData] = useState<any>(null);

  const UF_CODIGO_IBGE: Record<string, string> = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15", "AP": "16", "TO": "17",
    "MA": "21", "PI": "22", "CE": "23", "RN": "24", "PB": "25", "PE": "26", "AL": "27",
    "SE": "28", "BA": "29",
    "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
    "PR": "41", "SC": "42", "RS": "43",
    "MS": "50", "MT": "51", "GO": "52", "DF": "53",
  };

  useEffect(() => {
    const code = UF_CODIGO_IBGE[uf];
    if (code) {
      fetch(`/geojson/geojs-${code}-mun.json`)
        .then(r => r.json())
        .then(d => setGeoData(d))
        .catch(e => console.error('Erro ao carregar GeoJSON:', e));
    }
  }, [uf]);

  if (!geoData) {
    return <div className="h-[500px] flex items-center justify-center bg-gray-100 rounded">Carregando mapa...</div>;
  }

  // Create a map of votes by IBGE code for quick lookup
  const votosMap = new Map(data.map(d => [d.cd_ibge?.toString(), d]));
  const maxVotos = Math.max(...data.map(d => d.votos), 1); // Avoid division by zero

  const style = (feature: any) => {
    const ibgeCode = feature.properties.id; // Or whatever property holds the IBGE code
    // Often geojson from IBGE has properties.id or properties.CD_MUN
    const cd = ibgeCode || feature.properties.CD_MUN;
    const municipioData = votosMap.get(cd?.toString().substring(0, 6)) || votosMap.get(cd?.toString());
    
    let fillColor = '#e2e8f0'; // default gray
    let weight = 1;
    let color = '#ffffff';
    let fillOpacity = 0.5;

    if (municipioData && municipioData.votos > 0) {
      // Calculate opacity based on max votes (simple linear scale)
      const ratio = municipioData.votos / maxVotos;
      fillOpacity = 0.2 + (ratio * 0.8);
      fillColor = '#1f6feb'; // blue
      color = '#0b2545';
      weight = 1.5;
    }

    return {
      fillColor,
      weight,
      opacity: 1,
      color,
      fillOpacity
    };
  };

  const onEachFeature = (feature: any, layer: any) => {
    const ibgeCode = feature.properties.id || feature.properties.CD_MUN;
    const municipioData = votosMap.get(ibgeCode?.toString().substring(0, 6)) || votosMap.get(ibgeCode?.toString());
    
    if (municipioData) {
      layer.bindTooltip(
        `<strong>${municipioData.nm}</strong><br/>${new Intl.NumberFormat('pt-BR').format(municipioData.votos)} votos`,
        { sticky: true, className: 'text-sm font-sans' }
      );
    } else {
      layer.bindTooltip(
        `<strong>${feature.properties.name || feature.properties.NM_MUN}</strong><br/>0 votos`,
        { sticky: true, className: 'text-sm font-sans text-gray-500' }
      );
    }
  };

  // Calculate center from geojson bounds (simplification)
  const firstCoords = geoData.features[0].geometry.coordinates[0][0];
  // Handle multipolygon vs polygon
  const coords = Array.isArray(firstCoords[0]) ? firstCoords[0] : firstCoords;
  const center = [coords[1], coords[0]] as [number, number];

  return (
    <div className="h-[600px] w-full rounded border overflow-hidden">
      <MapContainer center={center} zoom={6} scrollWheelZoom={false} style={{ height: '100%', width: '100%' }}>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <GeoJSON 
          data={geoData} 
          style={style}
          onEachFeature={onEachFeature}
        />
      </MapContainer>
    </div>
  );
}
