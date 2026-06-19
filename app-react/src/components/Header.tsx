'use client';

import Image from 'next/image';
import { Printer } from 'lucide-react';

export function Header() {
  return (
    <header className="mb-10 w-full print:mb-10 print:w-full mt-10 print:mt-0">
      <div className="flex items-center justify-between gap-4">
        <div className="flex min-w-0 items-center gap-3 sm:gap-4">
          <Image
            src="/logo-elegis-light.png"
            alt="Elegis"
            width={120}
            height={42}
            priority
            className="h-9 w-auto shrink-0 sm:h-10"
          />
          <div className="h-10 w-1 shrink-0 bg-blue-600 sm:h-12 chart-print-bg" aria-hidden="true" />
          <div className="min-w-0">
            <h1 className="text-lg font-bold leading-tight text-[#0b1b3f] sm:text-2xl">
              Dados Eleitorais
            </h1>
            <p className="text-xs text-gray-500 sm:text-sm">
              Resultados consolidados a partir do boletim de urna do TSE.
            </p>
          </div>
        </div>

        <button
          type="button"
          onClick={() => window.print()}
          className="inline-flex shrink-0 items-center gap-2 rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm transition-colors hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 print:hidden"
        >
          <Printer className="h-4 w-4" />
          Imprimir
        </button>
      </div>
      <hr className="mt-7 mb-0 border-gray-200" />
    </header>
  );
}
