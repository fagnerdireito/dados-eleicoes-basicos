'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { Vote } from 'lucide-react';

const UF_CODIGO_IBGE: Record<string, string> = {
  "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15", "AP": "16", "TO": "17",
  "MA": "21", "PI": "22", "CE": "23", "RN": "24", "PB": "25", "PE": "26", "AL": "27",
  "SE": "28", "BA": "29",
  "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
  "PR": "41", "SC": "42", "RS": "43",
  "MS": "50", "MT": "51", "GO": "52", "DF": "53",
};

const WIDTH = 800;

type Tooltip = { x: number; y: number; nome: string; votos: number } | null;

export function MapEstado({ uf, data, totalVotos }: { uf: string; data: any[]; totalVotos: number }) {
  const [geoData, setGeoData] = useState<any>(null);
  const [tooltip, setTooltip] = useState<Tooltip>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const code = UF_CODIGO_IBGE[uf];
    if (code) {
      setGeoData(null);
      fetch(`/geojson/geojs-${code}-mun.json`)
        .then(r => r.json())
        .then(d => setGeoData(d))
        .catch(e => console.error('Erro ao carregar GeoJSON:', e));
    }
  }, [uf]);

  // Mapa de votos por código IBGE
  const votosMap = useMemo(() => new Map(data.map(d => [d.cd_ibge?.toString(), d])), [data]);
  const maxVotos = useMemo(() => Math.max(...data.map(d => d.votos), 1), [data]);
  const topMunicipios = useMemo(
    () => [...data].filter((d) => d.votos > 0).sort((a, b) => b.votos - a.votos),
    [data],
  );

  // Projeção equiretangular dos features para o viewBox do SVG
  const { paths, height } = useMemo(() => {
    if (!geoData) return { paths: [] as any[], height: 0 };

    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    const eachCoord = (geom: any, fn: (lon: number, lat: number) => void) => {
      const polys = geom.type === 'MultiPolygon' ? geom.coordinates : [geom.coordinates];
      for (const poly of polys) for (const ring of poly) for (const [lon, lat] of ring) fn(lon, lat);
    };

    for (const f of geoData.features) {
      eachCoord(f.geometry, (lon, lat) => {
        if (lon < minLon) minLon = lon;
        if (lon > maxLon) maxLon = lon;
        if (lat < minLat) minLat = lat;
        if (lat > maxLat) maxLat = lat;
      });
    }

    const midLatRad = ((minLat + maxLat) / 2) * Math.PI / 180;
    const cos = Math.cos(midLatRad);
    const lonRange = (maxLon - minLon) * cos || 1;
    const latRange = (maxLat - minLat) || 1;
    const h = (WIDTH * latRange) / lonRange;

    const proj = (lon: number, lat: number): [number, number] => [
      ((lon - minLon) * cos / lonRange) * WIDTH,
      h - ((lat - minLat) / latRange) * h,
    ];

    const built = geoData.features.map((f: any, i: number) => {
      const polys = f.geometry.type === 'MultiPolygon' ? f.geometry.coordinates : [f.geometry.coordinates];
      let d = '';
      let fMinX = Infinity, fMaxX = -Infinity, fMinY = Infinity, fMaxY = -Infinity;
      for (const poly of polys) {
        for (const ring of poly) {
          d += ring.map(([lon, lat]: [number, number], idx: number) => {
            const [x, y] = proj(lon, lat);
            if (x < fMinX) fMinX = x;
            if (x > fMaxX) fMaxX = x;
            if (y < fMinY) fMinY = y;
            if (y > fMaxY) fMaxY = y;
            return `${idx === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
          }).join('') + 'Z';
        }
      }
      const ibge = (f.properties.id || f.properties.CD_MUN)?.toString();
      const municipioData = votosMap.get(ibge?.substring(0, 6)) || votosMap.get(ibge);
      const nome = municipioData?.nm || f.properties.name || f.properties.NM_MUN || '';
      const votos = municipioData?.votos || 0;

      let fill = '#e2e8f0';
      let fillOpacity = 0.7;
      if (votos > 0) {
        fill = '#1f6feb';
        fillOpacity = 0.2 + (votos / maxVotos) * 0.8;
      }

      return {
        key: ibge || i,
        d,
        fill,
        fillOpacity,
        nome,
        votos,
        cx: (fMinX + fMaxX) / 2,
        cy: (fMinY + fMaxY) / 2,
      };
    });

    return { paths: built, height: h };
  }, [geoData, votosMap, maxVotos]);

  if (!geoData) {
    return <div className="h-[500px] flex items-center justify-center bg-gray-100 rounded">Carregando mapa...</div>;
  }

  const handleMove = (e: React.MouseEvent, nome: string, votos: number) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, nome, votos });
  };

  return (
    <div className="votos-estado-layout flex w-full flex-col gap-6 lg:flex-row lg:items-start print:flex-col print:gap-4">
      <div
        ref={containerRef}
        className="relative h-auto w-full shrink-0 self-start overflow-hidden rounded-lg bg-white shadow-lg chart-print-bg lg:w-3/5 print:w-full"
      >
        <svg
          viewBox={`0 0 ${WIDTH} ${height}`}
          className="w-full h-auto block"
          preserveAspectRatio="xMidYMid meet"
        >
          {paths.map((p: any) => (
            <path
              key={p.key}
              d={p.d}
              fill={p.fill}
              fillOpacity={p.fillOpacity}
              stroke={p.votos > 0 ? '#0b2545' : '#ffffff'}
              strokeWidth={0.5}
              className="cursor-pointer hover:stroke-slate-900 hover:stroke-[1.5]"
              onMouseMove={(e) => handleMove(e, p.nome, p.votos)}
              onMouseLeave={() => setTooltip(null)}
            />
          ))}

          {paths.filter((p: any) => p.votos > 0).map((p: any) => {
            const label = new Intl.NumberFormat('pt-BR').format(p.votos);
            const w = label.length * 7.5 + 8;
            const h = 16;
            return (
              <g
                key={`lbl-${p.key}`}
                className="pointer-events-none"
                transform={`translate(${p.cx}, ${p.cy})`}
              >
                <rect
                  x={-w / 2}
                  y={-h / 2}
                  width={w}
                  height={h}
                  rx={2}
                  fill="#ffffff"
                  stroke="#93b4e6"
                  strokeWidth={0.8}
                />
                <text
                  x={0}
                  y={0}
                  textAnchor="middle"
                  dominantBaseline="central"
                  fontSize={11}
                  fontWeight={700}
                  fill="#0b2545"
                >
                  {label}
                </text>
              </g>
            );
          })}
        </svg>

        {tooltip && (
          <div
            className="pointer-events-none absolute z-10 rounded bg-slate-900/90 px-2 py-1 text-xs text-white shadow"
            style={{ left: tooltip.x + 12, top: tooltip.y + 12 }}
          >
            <strong>{tooltip.nome}</strong>
            <br />
            {new Intl.NumberFormat('pt-BR').format(tooltip.votos)} votos
          </div>
        )}
      </div>

      <div className="flex min-w-0 w-full flex-col rounded-lg bg-white p-4 shadow-lg chart-print-bg lg:w-2/5 print:mt-10 print:w-full print:p-2">
        <h3 className="mb-3 text-sm font-bold text-[#0b2545]">
          Top municípios (votos do candidato)
        </h3>
        <span className="recorte-pill chart-print-bg mb-3 inline-flex w-fit items-center gap-2 rounded-full bg-indigo-600/70 px-4 py-1.5 text-sm font-semibold text-white">
          Total de votos no estado · {new Intl.NumberFormat('pt-BR').format(totalVotos)}
          <Vote className="h-4 w-4 shrink-0" strokeWidth={2.25} />
        </span>
        <ol className="text-sm lg:max-h-none lg:overflow-visible print:overflow-visible">
          {topMunicipios.map((m, i) => (
            <li
              key={m.cd ?? m.cd_ibge ?? i}
              className="flex items-center justify-between gap-3 border-b border-slate-100 py-1.5 last:border-0"
            >
              <span className="truncate text-gray-600">
                {i + 1}. {m.nm?.toUpperCase()}
              </span>
              <span className="shrink-0 font-bold text-[#0b2545]">
                {new Intl.NumberFormat('pt-BR').format(m.votos)}
              </span>
            </li>
          ))}
        </ol>
      </div>
    </div>
  );
}
