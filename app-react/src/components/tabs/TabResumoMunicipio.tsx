import { resumoCandidatoMunicipio, votosCandidatoPorMunicipio } from '@/app/actions-tab2';
import { Building2, MapPin, Trophy, Vote } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

function fmtInt(val: number) {
  return new Intl.NumberFormat('pt-BR').format(val);
}

function fmtPct(val: number, decimals = 1) {
  return new Intl.NumberFormat('pt-BR', { style: 'percent', minimumFractionDigits: decimals, maximumFractionDigits: decimals }).format(val / 100);
}

type Accent = 'purple' | 'blue' | 'green' | 'orange';

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

function SoftMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="soft-label">{label}</div>
      <div className="soft-value-sm mt-2">{value}</div>
    </div>
  );
}

export async function TabResumoMunicipio({ searchParams }: { searchParams: { [key: string]: string | undefined } }) {
  const { ano, uf, municipio, cargo, candidato } = searchParams;

  if (!ano || !uf || !cargo || !candidato) {
    return <div className="soft-subtitle p-2">Selecione Ano, UF, Cargo e Candidato nos filtros acima.</div>;
  }

  const municipios = await votosCandidatoPorMunicipio(parseInt(ano, 10), uf, cargo, candidato, municipio);
  const municipiosFiltrados = municipios.filter(m => m.votos > 0);

  if (municipiosFiltrados.length === 0) {
    return (
      <div className="soft-alert">
        Nenhum município com votação do candidato na UF selecionada.
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-10">
      <header>
        <h2 className="soft-title">Resumo por município</h2>
        <p className="soft-subtitle">Municípios com votação na UF · {ano} · Cargo {cargo}</p>
      </header>

      {await Promise.all(municipiosFiltrados.map(async (row, i) => {
        const d = await resumoCandidatoMunicipio(parseInt(ano, 10), uf, row.cd, cargo, candidato);

        return (
          <section key={row.cd} className={`flex flex-col gap-8 print:break-inside-avoid ${i > 0 ? 'pt-4' : ''}`}>
            <div>
              <h3 className="soft-municipio-title">{row.nm} ({uf})</h3>
              <p className="soft-subtitle">Resumo de {d.nm_candidato || candidato} no município</p>
            </div>

            <div className="grid grid-cols-1 gap-5 md:grid-cols-2 print:grid-cols-2">
              <SoftStatCard
                icon={Vote}
                accent="purple"
                label="Votação do candidato"
                value={fmtInt(d.votos_cand)}
                footnote={`${fmtPct(d.pct_validos)} dos ${fmtInt(d.validos)} votos válidos do município.`}
              />
              <SoftStatCard
                icon={Trophy}
                accent="orange"
                label="Posição geral no município"
                value={d.posicao ? `${d.posicao}º` : '—'}
                footnote={`Classificação entre ${fmtInt(d.total_cands)} candidatos ao mesmo cargo.`}
              />
              <SoftStatCard
                icon={MapPin}
                accent="green"
                label="Liderança nos locais"
                value={fmtInt(d.lideres)}
                footnote={`locais onde ficou em 1º, de ${fmtInt(d.total_locais)} analisados.`}
              />
              <SoftStatCard
                icon={Building2}
                accent="blue"
                label="Locais de votação analisados"
                value={fmtInt(d.total_locais)}
                footnote={`Comparecimento de ${fmtPct(d.pct_comparec)} (${fmtInt(d.comparec)}/${fmtInt(d.aptos)}).`}
              />
            </div>

            <div className="soft-card chart-print-bg">
              <h4 className="soft-section-title mb-6">Composição dos votos no município</h4>
              <div className="grid grid-cols-2 gap-6 md:grid-cols-5 print:grid-cols-5">
                <SoftMetric label="Válidos (nominais + legenda)" value={fmtInt(d.validos)} />
                <SoftMetric label="Brancos" value={fmtInt(d.brancos)} />
                <SoftMetric label="Nulos" value={fmtInt(d.nulos)} />
                <SoftMetric label="Abstenções" value={fmtInt(d.abstenc)} />
                <SoftMetric label="Comparecimento" value={fmtPct(d.pct_comparec, 0)} />
              </div>
            </div>
          </section>
        );
      }))}
    </div>
  );
}
