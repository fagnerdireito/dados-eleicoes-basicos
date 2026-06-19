import { sinteseTerritorial, sinteseTerritorialUf } from '@/app/actions-tab7';
import { listarMunicipios } from '@/app/actions';
import { Building2, MapPin, Trophy, Vote } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number) {
  return new Intl.NumberFormat('pt-BR', { style: 'percent', minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(val / 100);
}

type Accent = 'purple' | 'blue' | 'green' | 'orange';

const ACCENTS: Accent[] = ['purple', 'orange', 'green', 'blue'];

function RankingRow({
  icon: Icon,
  accent,
  rank,
  name,
  partido,
  value,
  detail,
  isFoco,
}: {
  icon: LucideIcon;
  accent: Accent;
  rank: number;
  name: string;
  partido: string;
  value: string;
  detail: string;
  isFoco: boolean;
}) {
  return (
    <div
      className={`grid grid-cols-[2rem_2.5rem_minmax(0,1fr)_auto_5.5rem_9rem] items-center gap-x-3 rounded-lg border border-gray-100 bg-white px-3 py-2 shadow-sm chart-print-bg ${isFoco ? 'ring-2 ring-purple-200' : ''}`}
    >
      <div className={`soft-icon-badge soft-icon-badge--${accent} !h-8 !w-8 !rounded-lg shrink-0`}>
        <Icon className="h-4 w-4" strokeWidth={2.25} />
      </div>
      <span className="text-sm font-semibold text-[var(--soft-text)] tabular-nums">{rank}º</span>
      <span className="truncate text-sm font-medium text-[var(--soft-text)]" title={name}>
        {name}
        {isFoco ? ' (foco)' : ''}
      </span>
      <span className="inline-flex w-fit shrink-0 rounded-full bg-indigo-600/70 px-2 py-0.5 text-xs font-semibold whitespace-nowrap text-white">
        {partido || '—'}
      </span>
      <span className="text-right text-sm font-bold tabular-nums text-[var(--soft-text)]">{value}</span>
      <span className="truncate text-right text-xs text-[var(--soft-text-muted)]">{detail}</span>
    </div>
  );
}

function SoftStatCard({
  icon: Icon,
  accent,
  label,
  value,
  footnote,
}: {
  icon: LucideIcon;
  accent: Accent;
  label: string;
  value: string;
  footnote: string;
}) {
  return (
    <div className="soft-stat-card chart-print-bg">
      <div className={`soft-icon-badge soft-icon-badge--${accent}`}>
        <Icon className="h-5 w-5" strokeWidth={2.25} />
      </div>
      <div className="min-w-0 flex-1">
        <div className="soft-label">{label}</div>
        <div className="soft-value mt-2">{value}</div>
        <div className="soft-footnote">{footnote}</div>
      </div>
    </div>
  );
}

export async function TabSinteseTerritorial({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo) {
    return <div className="soft-subtitle p-2">Selecione Ano, UF e Cargo nos filtros acima.</div>;
  }

  const anoNum = parseInt(ano, 10);

  if (municipio) {
    const municipios = await listarMunicipios(anoNum, uf);
    const municipioNome = municipios.find((m) => String(m.cd) === municipio)?.nm ?? municipio;

    const data = await sinteseTerritorial(anoNum, uf, municipio, cargo);
    if (data.length === 0) {
      return <div className="soft-alert">Sem dados para o filtro atual.</div>;
    }

    const total = data.reduce((acc, curr) => acc + curr.locais, 0);

    return (
      <div className="flex flex-col gap-8">
        <header>
          <h2 className="soft-title">Síntese territorial</h2>
          <p className="soft-subtitle">Locais liderados por cada candidato · {municipioNome}</p>
        </header>

        <SoftStatCard
          icon={Building2}
          accent="blue"
          label="Locais apurados"
          value={fmtInt(total)}
          footnote="Total de locais com vencedor definido no município."
        />

        <h4 className="soft-section-title">Ranking por locais liderados</h4>
        <div className="flex flex-col gap-2">
          {data.map((r, i) => {
            const isFoco = r.nr === candidato;
            return (
              <RankingRow
                key={r.nr ?? i}
                icon={i === 0 ? Trophy : MapPin}
                accent={isFoco ? 'purple' : ACCENTS[i % ACCENTS.length]}
                rank={i + 1}
                name={r.nm}
                partido={r.partido}
                value={fmtInt(r.locais)}
                detail="locais em 1º"
                isFoco={isFoco}
              />
            );
          })}
        </div>
      </div>
    );
  }

  const dataUf = await sinteseTerritorialUf(anoNum, uf, cargo);
  if (dataUf.length === 0) {
    return <div className="soft-alert">Sem dados para o filtro atual.</div>;
  }

  const totalUf = dataUf.reduce((acc, curr) => acc + curr.votos, 0);

  return (
    <div className="flex flex-col gap-8">
      <header>
        <h2 className="soft-title">Síntese territorial</h2>
        <p className="soft-subtitle">Votos por candidato na UF · {uf} · cargo {cargo}</p>
      </header>

      <SoftStatCard
        icon={Vote}
        accent="purple"
        label="Votos válidos na UF"
        value={fmtInt(totalUf)}
        footnote={`Soma dos votos nominais e legenda na UF · ${ano}`}
      />

      <h4 className="soft-section-title">Ranking de votação na UF</h4>
      <div className="flex flex-col gap-2">
        {dataUf.map((r, i) => {
          const isFoco = r.nr === candidato;
          return (
            <RankingRow
              key={r.nr ?? i}
              icon={i === 0 ? Trophy : Vote}
              accent={isFoco ? 'purple' : ACCENTS[i % ACCENTS.length]}
              rank={i + 1}
              name={r.nm}
              partido={r.partido}
              value={fmtInt(r.votos)}
              detail={`${fmtPct(r.pct)} válidos`}
              isFoco={isFoco}
            />
          );
        })}
      </div>
    </div>
  );
}
