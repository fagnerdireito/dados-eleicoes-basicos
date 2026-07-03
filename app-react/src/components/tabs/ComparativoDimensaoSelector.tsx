'use client';

import { usePathname, useRouter } from 'next/navigation';
import { useNavigationLoading } from '@/components/NavigationLoading';

const DIM_LABELS: Record<string, string> = {
  zona: 'Zona',
  bairro: 'Bairro',
  secao: 'Seção',
  local: 'Local de votação',
};

const DIM_PRECISA_LOCAL_VOTACAO = ['bairro', 'local'];

export function ComparativoDimensaoSelector({
  dimensao,
  hasLocalVotacao,
  searchParams,
}: {
  dimensao: string;
  hasLocalVotacao: boolean;
  searchParams: Record<string, string | undefined>;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const { isPending, startNavigation } = useNavigationLoading();

  const irParaDimensao = (key: string) => {
    if (key === dimensao) return;
    const params = new URLSearchParams();
    Object.entries(searchParams).forEach(([k, v]) => {
      if (v) params.set(k, v);
    });
    params.set('dim', key);
    startNavigation(() => {
      router.push(`${pathname}?${params.toString()}`);
    });
  };

  return (
    <div
      className={`flex gap-4 items-center bg-gray-50 p-4 border rounded-lg overflow-x-auto transition-opacity duration-200 ${
        isPending ? 'pointer-events-none opacity-40' : ''
      }`}
    >
      <span className="font-semibold text-sm mr-2">Dimensão:</span>
      {Object.entries(DIM_LABELS).map(([key, label]) => {
        const isDisabled = !hasLocalVotacao && DIM_PRECISA_LOCAL_VOTACAO.includes(key);
        const isActive = key === dimensao;

        if (isDisabled) {
          return (
            <span
              key={key}
              className="px-3 py-1 text-sm rounded bg-gray-200 text-gray-400 cursor-not-allowed"
              title="Requer local_votacao"
            >
              {label}
            </span>
          );
        }

        return (
          <button
            key={key}
            type="button"
            onClick={() => irParaDimensao(key)}
            className={`px-3 py-1 text-sm rounded transition-colors whitespace-nowrap ${
              isActive
                ? 'bg-[#0b2545] text-white'
                : 'bg-white border hover:bg-gray-100 text-[#0b2545]'
            }`}
          >
            {label}
          </button>
        );
      })}
    </div>
  );
}
