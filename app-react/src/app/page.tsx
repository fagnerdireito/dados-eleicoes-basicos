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

type SearchParams = { [key: string]: string | undefined };

// Com cacheComponents (PPR) habilitado, o acesso a searchParams precisa
// acontecer dentro de um <Suspense>. Por isso o await é feito aqui, num
// componente que recebe a Promise e é renderizado dentro do boundary.
async function TabContent({ searchParams }: { searchParams: Promise<SearchParams> }) {
  const resolvedSearchParams = await searchParams;
  const currentTab = resolvedSearchParams.tab || '1';

  return (
    <>
      {currentTab === '1' && <TabSumario searchParams={resolvedSearchParams} />}
      {currentTab === '2' && <TabResumoMunicipio searchParams={resolvedSearchParams} />}
      {currentTab === '3' && <TabPerfilEleitorado searchParams={resolvedSearchParams} />}
      {currentTab === '4' && <TabVotosEstado searchParams={resolvedSearchParams} />}
      {currentTab === '5' && <TabVotosMunicipio searchParams={resolvedSearchParams} />}
      {currentTab === '6' && <TabRankingMunicipio searchParams={resolvedSearchParams} />}
      {currentTab === '7' && <TabSinteseTerritorial searchParams={resolvedSearchParams} />}
      {currentTab === '8' && <TabCardLocal searchParams={resolvedSearchParams} />}
      {currentTab === '9' && <TabComparativo searchParams={resolvedSearchParams} />}
      {currentTab === '10' && <TabVotosBairro searchParams={resolvedSearchParams} />}
    </>
  );
}

export default function Home({ searchParams }: { searchParams: Promise<SearchParams> }) {
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
          <Suspense fallback={<TabContentLoadingFallback />}>
            <TabContent searchParams={searchParams} />
          </Suspense>
        </HomeTabPanel>
      </div>
    </NavigationLoadingProvider>
  );
}
