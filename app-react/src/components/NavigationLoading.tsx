'use client';

import { createContext, useCallback, useContext, useTransition, type ReactNode } from 'react';

type NavigationLoadingContextValue = {
  isPending: boolean;
  startNavigation: (navigate: () => void) => void;
};

const NavigationLoadingContext = createContext<NavigationLoadingContextValue | null>(null);

export function NavigationLoadingProvider({ children }: { children: ReactNode }) {
  const [isPending, startTransition] = useTransition();

  const startNavigation = useCallback((navigate: () => void) => {
    startTransition(() => {
      navigate();
    });
  }, []);

  return (
    <NavigationLoadingContext.Provider value={{ isPending, startNavigation }}>
      {children}
    </NavigationLoadingContext.Provider>
  );
}

export function useNavigationLoading() {
  const ctx = useContext(NavigationLoadingContext);
  if (!ctx) {
    throw new Error('useNavigationLoading must be used within NavigationLoadingProvider');
  }
  return ctx;
}

export function DataLoadingIndicator() {
  return (
    <div className="flex flex-col items-center gap-5">
      <div className="relative flex h-14 w-14 items-center justify-center" aria-hidden>
        <span className="absolute inset-0 animate-ping rounded-full bg-indigo-500/25" />
        <span
          className="absolute inset-1 rounded-full bg-indigo-400/15"
          style={{ animation: 'data-loading-pulse 1.6s ease-in-out infinite' }}
        />
        <span
          className="relative h-10 w-10 animate-spin rounded-full border-2 border-indigo-200/50 border-t-indigo-600"
          style={{
            boxShadow:
              '0 0 0 4px rgb(79 70 229 / 0.15), 0 0 22px rgb(79 70 229 / 0.55), 0 0 44px rgb(99 102 241 / 0.35)',
          }}
        />
      </div>
      <p
        className="text-sm font-medium tracking-wide text-indigo-700"
        style={{ animation: 'data-loading-pulse 1.6s ease-in-out infinite' }}
      >
        carregando dados
      </p>
    </div>
  );
}

export function TabContentLoadingFallback() {
  return (
    <div className="flex min-h-[320px] items-start justify-center pt-12">
      <DataLoadingIndicator />
    </div>
  );
}

export function HomeTabPanel({ children }: { children: ReactNode }) {
  const { isPending } = useNavigationLoading();

  return (
    <div className="glass-panel relative min-h-[400px] rounded-xl p-6 print:mt-0 print:w-full print:p-0 print:shadow-none print:rounded-none print:bg-white!">
      {isPending && (
        <div className="absolute inset-0 z-20 flex items-start justify-center rounded-xl bg-white/55 pt-12 backdrop-blur-[2px] print:hidden">
          <DataLoadingIndicator />
        </div>
      )}
      <div
        className={`transition-opacity duration-200 ${isPending ? 'pointer-events-none opacity-40' : ''}`}
      >
        {children}
      </div>
    </div>
  );
}
