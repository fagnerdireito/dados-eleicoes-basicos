import { GlobalFilters } from '@/components/GlobalFilters';
import { TabsNavigation } from '@/components/TabsNavigation';
import { TabSumario } from '@/components/tabs/TabSumario';
import { TabResumoMunicipio } from '@/components/tabs/TabResumoMunicipio';
import { TabPerfilEleitorado } from '@/components/tabs/TabPerfilEleitorado';
import { TabVotosEstado } from '@/components/tabs/TabVotosEstado';
import { TabVotosMunicipio } from '@/components/tabs/TabVotosMunicipio';
import { TabRankingMunicipio } from '@/components/tabs/TabRankingMunicipio';
import { TabSinteseTerritorial } from '@/components/tabs/TabSinteseTerritorial';
import { TabCardLocal } from '@/components/tabs/TabCardLocal';
import { TabComparativo } from '@/components/tabs/TabComparativo';
import { TabVotosBairro } from '@/components/tabs/TabVotosBairro';
import { Suspense } from 'react';

export default async function Home({ searchParams }: { searchParams: Promise<{ [key: string]: string | undefined }> }) {
  const resolvedSearchParams = await searchParams;
  const currentTab = resolvedSearchParams.tab || '1';

  return (
    <div className="flex flex-col print:w-full">
      <Suspense fallback={<div>Carregando filtros...</div>}>
        <GlobalFilters />
      </Suspense>
      
      <Suspense fallback={<div>Carregando abas...</div>}>
        <TabsNavigation />
      </Suspense>

      <div className="glass-panel min-h-[400px] rounded-xl p-6 print:mt-0 print:w-full print:p-0 print:shadow-none print:rounded-none print:bg-white!">
        {currentTab === '1' && (
          <Suspense fallback={<div>Carregando sumário...</div>}>
            <TabSumario searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '2' && (
          <Suspense fallback={<div>Carregando resumo do município...</div>}>
            <TabResumoMunicipio searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '3' && (
          <Suspense fallback={<div>Carregando perfil do eleitorado...</div>}>
            <TabPerfilEleitorado searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '4' && (
          <Suspense fallback={<div>Carregando votos no estado...</div>}>
            <TabVotosEstado searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '5' && (
          <Suspense fallback={<div>Carregando votos no município...</div>}>
            <TabVotosMunicipio searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '6' && (
          <Suspense fallback={<div>Carregando ranking do município...</div>}>
            <TabRankingMunicipio searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '7' && (
          <Suspense fallback={<div>Carregando síntese territorial...</div>}>
            <TabSinteseTerritorial searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '8' && (
          <Suspense fallback={<div>Carregando votos por local de votação...</div>}>
            <TabCardLocal searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '9' && (
          <Suspense fallback={<div>Carregando comparativo...</div>}>
            <TabComparativo searchParams={resolvedSearchParams} />
          </Suspense>
        )}
        {currentTab === '10' && (
          <Suspense fallback={<div>Carregando votos por bairro...</div>}>
            <TabVotosBairro searchParams={resolvedSearchParams} />
          </Suspense>
        )}
      </div>
    </div>
  );
}
