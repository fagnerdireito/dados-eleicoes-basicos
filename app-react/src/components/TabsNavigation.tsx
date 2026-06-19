'use client';

import { useSearchParams, useRouter, usePathname } from 'next/navigation';
import { useNavigationLoading } from '@/components/NavigationLoading';

export const TAB_LABELS = [
  { id: '1', label: '1. Sumário' },
  { id: '2', label: '2. Resumo município' },
  { id: '3', label: '3. Perfil eleitorado (UF)' },
  { id: '4', label: '4. Votos no estado' },
  { id: '5', label: '5. Votos no município' },
  { id: '6', label: '6. Ranking município' },
  { id: '7', label: '7. Síntese territorial' },
  { id: '8', label: '8. Votos por local de votação' },
  { id: '9', label: '9. Comparativo candidatos' },
  { id: '10', label: '10. Votos por bairro' },
];

export function TabsNavigation() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { isPending, startNavigation } = useNavigationLoading();

  const currentTab = searchParams.get('tab') || '1';

  const setTab = (id: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set('tab', id);
    startNavigation(() => {
      router.push(`${pathname}?${params.toString()}`);
    });
  };

  return (
    <div
      className={`glass-panel mb-6 flex flex-wrap gap-1 rounded-xl p-2 transition-opacity duration-200 print:hidden ${isPending ? 'pointer-events-none opacity-40' : ''}`}
    >
      {TAB_LABELS.map(tab => (
        <button
          key={tab.id}
          onClick={() => setTab(tab.id)}
          className={`whitespace-nowrap rounded-lg px-4 py-2 text-xs font-medium transition-colors ${
            currentTab === tab.id
              ? 'bg-indigo-600/70 text-white shadow-sm'
              : 'text-gray-600 hover:bg-white/70 hover:shadow-sm'
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
