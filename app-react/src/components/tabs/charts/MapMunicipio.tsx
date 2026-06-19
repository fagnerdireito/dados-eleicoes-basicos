'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { RotateCcw, ZoomIn, ZoomOut } from 'lucide-react';

const UF_CODIGO_IBGE: Record<string, string> = {
  RO: '11', AC: '12', AM: '13', RR: '14', PA: '15', AP: '16', TO: '17',
  MA: '21', PI: '22', CE: '23', RN: '24', PB: '25', PE: '26', AL: '27',
  SE: '28', BA: '29',
  MG: '31', ES: '32', RJ: '33', SP: '35',
  PR: '41', SC: '42', RS: '43',
  MS: '50', MT: '51', GO: '52', DF: '53',
};

const WIDTH = 800;
const MIN_ZOOM = 1;
const MAX_ZOOM = 8;
const ZOOM_FACTOR = 1.15;

type MapView = { scale: number; tx: number; ty: number };

const DEFAULT_VIEW: MapView = { scale: 1, tx: 0, ty: 0 };

type LocalPoint = {
  nm_local: string;
  bairro?: string;
  lat: number;
  lng: number;
  votos: number;
};

type Tooltip = { x: number; y: number; nome: string; votos: number; bairro?: string } | null;

function eachCoord(geom: { type: string; coordinates: unknown }, fn: (lon: number, lat: number) => void) {
  const polys = geom.type === 'MultiPolygon' ? geom.coordinates as number[][][][] : [geom.coordinates as number[][][]];
  for (const poly of polys) {
    for (const ring of poly) {
      for (const [lon, lat] of ring) fn(lon, lat);
    }
  }
}

function buildPath(geom: { type: string; coordinates: unknown }, proj: (lon: number, lat: number) => [number, number]) {
  const polys = geom.type === 'MultiPolygon' ? geom.coordinates as number[][][][] : [geom.coordinates as number[][][]];
  let d = '';
  for (const poly of polys) {
    for (const ring of poly) {
      d += ring.map((coord: number[], idx: number) => {
        const [lon, lat] = coord;
        const [x, y] = proj(lon, lat);
        return `${idx === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
      }).join('') + 'Z';
    }
  }
  return d;
}

function matchMunicipioFeature(feature: { properties?: Record<string, unknown>; geometry: unknown }, cdIbge?: string, municipioNome?: string) {
  const id = (feature.properties?.id || feature.properties?.CD_MUN)?.toString() ?? '';
  const name = (feature.properties?.name || feature.properties?.NM_MUN)?.toString() ?? '';

  if (cdIbge) {
    const ibge = cdIbge.toString();
    if (id === ibge || id.startsWith(ibge) || ibge.startsWith(id.slice(0, 6))) return true;
  }

  if (municipioNome && name) {
    return name.localeCompare(municipioNome, 'pt-BR', { sensitivity: 'base' }) === 0;
  }

  return false;
}

function MarkerPin({
  x,
  y,
  scale,
  onMouseEnter,
  onMouseLeave,
}: {
  x: number;
  y: number;
  scale: number;
  onMouseEnter: (e: React.MouseEvent) => void;
  onMouseLeave: () => void;
}) {
  return (
    <g
      transform={`translate(${x.toFixed(2)}, ${y.toFixed(2)}) scale(${scale})`}
      className="cursor-pointer"
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <path
        d="M0,-14 C-7,-14 -12,-9 -12,-2 C-12,6 0,18 0,18 C0,18 12,6 12,-2 C12,-9 7,-14 0,-14 Z"
        fill="#7b61ff"
        stroke="#ffffff"
        strokeWidth={1.5}
      />
      <circle cx={0} cy={-2} r={3.5} fill="#ffffff" />
    </g>
  );
}

export function MapMunicipio({
  uf,
  cdIbge,
  municipioNome,
  data,
}: {
  uf: string;
  cdIbge?: string;
  municipioNome: string;
  data: LocalPoint[];
}) {
  const [geoData, setGeoData] = useState<{ features: { properties?: Record<string, unknown>; geometry: { type: string; coordinates: unknown } }[] } | null>(null);
  const [tooltip, setTooltip] = useState<Tooltip>(null);
  const [view, setView] = useState<MapView>(DEFAULT_VIEW);
  const [isPanning, setIsPanning] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const dragRef = useRef<{ startX: number; startY: number; startTx: number; startTy: number } | null>(null);

  useEffect(() => {
    const code = UF_CODIGO_IBGE[uf];
    if (!code) return;
    setGeoData(null);
    fetch(`/geojson/geojs-${code}-mun.json`)
      .then((r) => r.json())
      .then((d) => setGeoData(d))
      .catch((e) => console.error('Erro ao carregar GeoJSON:', e));
  }, [uf]);

  useEffect(() => {
    setView(DEFAULT_VIEW);
  }, [uf, cdIbge, municipioNome]);

  const maxVotos = useMemo(() => Math.max(...data.map((d) => d.votos), 1), [data]);
  const totalVotos = useMemo(() => data.reduce((acc, d) => acc + d.votos, 0), [data]);

  const mapModel = useMemo(() => {
    if (!geoData) return null;

    const feature = geoData.features.find((f) => matchMunicipioFeature(f, cdIbge, municipioNome));
    if (!feature) return null;

    let minLon = Infinity;
    let maxLon = -Infinity;
    let minLat = Infinity;
    let maxLat = -Infinity;

    eachCoord(feature.geometry as { type: string; coordinates: unknown }, (lon, lat) => {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    });

    for (const p of data) {
      if (p.lng < minLon) minLon = p.lng;
      if (p.lng > maxLon) maxLon = p.lng;
      if (p.lat < minLat) minLat = p.lat;
      if (p.lat > maxLat) maxLat = p.lat;
    }

    const padLon = (maxLon - minLon) * 0.06 || 0.02;
    const padLat = (maxLat - minLat) * 0.06 || 0.02;
    minLon -= padLon;
    maxLon += padLon;
    minLat -= padLat;
    maxLat += padLat;

    const midLatRad = ((minLat + maxLat) / 2) * Math.PI / 180;
    const cos = Math.cos(midLatRad);
    const lonRange = (maxLon - minLon) * cos || 1;
    const latRange = maxLat - minLat || 1;
    const height = (WIDTH * latRange) / lonRange;

    const proj = (lon: number, lat: number): [number, number] => [
      ((lon - minLon) * cos / lonRange) * WIDTH,
      height - ((lat - minLat) / latRange) * height,
    ];

    const pathD = buildPath(feature.geometry as { type: string; coordinates: unknown }, proj);
    const markers = data.map((p, i) => {
      const [x, y] = proj(p.lng, p.lat);
      const scale = 0.75 + (p.votos / maxVotos) * 0.55;
      return { ...p, x, y, scale, key: `${p.nm_local}-${i}` };
    });

    const fillOpacity = Math.min(0.85, 0.35 + (totalVotos / (maxVotos * Math.max(data.length, 1))) * 0.35);

    return { pathD, height, markers, fillOpacity };
  }, [geoData, cdIbge, municipioNome, data, maxVotos, totalVotos]);

  const mapHeight = mapModel?.height ?? 520;

  const clientToSvg = useCallback((clientX: number, clientY: number) => {
    const svg = svgRef.current;
    const rect = svg?.getBoundingClientRect();
    if (!rect) return { x: WIDTH / 2, y: mapHeight / 2 };
    return {
      x: ((clientX - rect.left) / rect.width) * WIDTH,
      y: ((clientY - rect.top) / rect.height) * mapHeight,
    };
  }, [mapHeight]);

  const applyZoom = useCallback((factor: number, anchorX: number, anchorY: number) => {
    setView((current) => {
      const nextScale = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, current.scale * factor));
      if (nextScale === current.scale) return current;
      return {
        scale: nextScale,
        tx: anchorX - (anchorX - current.tx) * (nextScale / current.scale),
        ty: anchorY - (anchorY - current.ty) * (nextScale / current.scale),
      };
    });
  }, []);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !mapModel) return;

    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const { x, y } = clientToSvg(e.clientX, e.clientY);
      const factor = e.deltaY < 0 ? ZOOM_FACTOR : 1 / ZOOM_FACTOR;
      applyZoom(factor, x, y);
    };

    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, [mapModel, clientToSvg, applyZoom]);

  if (!geoData) {
    return (
      <div className="flex h-[520px] items-center justify-center rounded-lg bg-gray-100">
        Carregando mapa...
      </div>
    );
  }

  if (!mapModel) {
    return (
      <div className="flex h-[520px] items-center justify-center rounded-lg bg-gray-100 text-sm text-gray-500">
        Contorno do município não encontrado no GeoJSON.
      </div>
    );
  }

  const handleMove = (e: React.MouseEvent, nome: string, votos: number, bairro?: string) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    if (dragRef.current) return;
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, nome, votos, bairro });
  };

  const handlePointerDown = (e: React.PointerEvent<SVGSVGElement>) => {
    if (e.button !== 0) return;
    e.currentTarget.setPointerCapture(e.pointerId);
    dragRef.current = {
      startX: e.clientX,
      startY: e.clientY,
      startTx: view.tx,
      startTy: view.ty,
    };
    setIsPanning(true);
    setTooltip(null);
  };

  const handlePointerMove = (e: React.PointerEvent<SVGSVGElement>) => {
    if (!dragRef.current) return;
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const dx = e.clientX - dragRef.current.startX;
    const dy = e.clientY - dragRef.current.startY;
    const scaleX = WIDTH / rect.width;
    const scaleY = mapModel.height / rect.height;
    setView((current) => ({
      ...current,
      tx: dragRef.current!.startTx + dx * scaleX,
      ty: dragRef.current!.startTy + dy * scaleY,
    }));
  };

  const handlePointerUp = (e: React.PointerEvent<SVGSVGElement>) => {
    if (dragRef.current) {
      e.currentTarget.releasePointerCapture(e.pointerId);
      dragRef.current = null;
      setIsPanning(false);
    }
  };

  const zoomFromCenter = (factor: number) => {
    applyZoom(factor, WIDTH / 2, mapModel.height / 2);
  };

  return (
    <div
      ref={containerRef}
      className="relative w-full overflow-hidden rounded-lg bg-white shadow-lg chart-print-bg touch-none"
    >
      <div className="absolute right-2 top-2 z-10 flex flex-col gap-1 print:hidden">
        <button
          type="button"
          aria-label="Aumentar zoom"
          onClick={() => zoomFromCenter(ZOOM_FACTOR)}
          className="rounded-md border border-gray-200 bg-white/95 p-1.5 text-[#0b2545] shadow-sm transition hover:bg-gray-50"
        >
          <ZoomIn className="h-4 w-4" strokeWidth={2.25} />
        </button>
        <button
          type="button"
          aria-label="Diminuir zoom"
          onClick={() => zoomFromCenter(1 / ZOOM_FACTOR)}
          className="rounded-md border border-gray-200 bg-white/95 p-1.5 text-[#0b2545] shadow-sm transition hover:bg-gray-50"
        >
          <ZoomOut className="h-4 w-4" strokeWidth={2.25} />
        </button>
        <button
          type="button"
          aria-label="Resetar zoom"
          onClick={() => setView(DEFAULT_VIEW)}
          disabled={view.scale === 1 && view.tx === 0 && view.ty === 0}
          className="rounded-md border border-gray-200 bg-white/95 p-1.5 text-[#0b2545] shadow-sm transition hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-40"
        >
          <RotateCcw className="h-4 w-4" strokeWidth={2.25} />
        </button>
      </div>

      <svg
        ref={svgRef}
        viewBox={`0 0 ${WIDTH} ${mapModel.height}`}
        className={`block h-auto w-full ${isPanning ? 'cursor-grabbing' : 'cursor-grab'}`}
        preserveAspectRatio="xMidYMid meet"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
      >
        <g transform={`translate(${view.tx.toFixed(2)}, ${view.ty.toFixed(2)}) scale(${view.scale.toFixed(4)})`}>
          <path
            d={mapModel.pathD}
            fill="#1f6feb"
            fillOpacity={mapModel.fillOpacity}
            stroke="#0b2545"
            strokeWidth={1.2 / view.scale}
          />

          {mapModel.markers.map((m) => (
            <MarkerPin
              key={m.key}
              x={m.x}
              y={m.y}
              scale={m.scale / view.scale}
              onMouseEnter={(e) => handleMove(e, m.nm_local, m.votos, m.bairro)}
              onMouseLeave={() => setTooltip(null)}
            />
          ))}
        </g>
      </svg>

      {tooltip && (
        <div
          className="pointer-events-none absolute z-10 rounded bg-slate-900/90 px-2 py-1 text-xs text-white shadow"
          style={{ left: tooltip.x + 12, top: tooltip.y + 12 }}
        >
          <strong>{tooltip.nome}</strong>
          {tooltip.bairro ? (
            <>
              <br />
              {tooltip.bairro}
            </>
          ) : null}
          <br />
          {new Intl.NumberFormat('pt-BR').format(tooltip.votos)} votos
        </div>
      )}
    </div>
  );
}
