'use client';

import { useSearchParams, useRouter, usePathname } from 'next/navigation';

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

  const currentTab = searchParams.get('tab') || '1';

  const setTab = (id: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set('tab', id);
    router.push(`${pathname}?${params.toString()}`);
  };

  return (
    <div className="flex overflow-x-auto space-x-1 border-b border-gray-200 mb-6 bg-white p-2 rounded-t-lg shadow-sm">
      {TAB_LABELS.map(tab => (
        <button
          key={tab.id}
          onClick={() => setTab(tab.id)}
          className={`whitespace-nowrap py-2 px-4 text-sm font-medium rounded-md transition-colors ${
            currentTab === tab.id
              ? 'bg-[#0b2545] text-white'
              : 'text-gray-600 hover:bg-gray-100'
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
