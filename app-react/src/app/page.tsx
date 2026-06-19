import { GlobalFilters } from '@/components/GlobalFilters';
import { TabsNavigation } from '@/components/TabsNavigation';
import {
  HomeTabPanel,
  NavigationLoadingProvider,
  TabContentLoadingFallback,
} from '@/components/NavigationLoading';
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
    <NavigationLoadingProvider>
      <div className="flex flex-col print:w-full">
        <Suspense fallback={<div>Carregando filtros...</div>}>
          <GlobalFilters />
        </Suspense>

        <Suspense fallback={<div>Carregando abas...</div>}>
          <TabsNavigation />
        </Suspense>

        <HomeTabPanel>
          {currentTab === '1' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabSumario searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '2' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabResumoMunicipio searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '3' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabPerfilEleitorado searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '4' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabVotosEstado searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '5' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabVotosMunicipio searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '6' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabRankingMunicipio searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '7' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabSinteseTerritorial searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '8' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabCardLocal searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '9' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabComparativo searchParams={resolvedSearchParams} />
            </Suspense>
          )}
          {currentTab === '10' && (
            <Suspense fallback={<TabContentLoadingFallback />}>
              <TabVotosBairro searchParams={resolvedSearchParams} />
            </Suspense>
          )}
        </HomeTabPanel>
      </div>
    </NavigationLoadingProvider>
  );
}
