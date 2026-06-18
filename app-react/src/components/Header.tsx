'use client';

import Image from 'next/image';
import { Printer } from 'lucide-react';

export function Header() {
  return (
    <header className="w-full print:w-full">
      <div className="flex justify-end mb-4 sm:mb-6 print:hidden">
        <button
          type="button"
          onClick={() => window.print()}
          className="inline-flex items-center gap-2 rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm transition-colors hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 print:hidden mb-10"
        >
          <Printer className="h-4 w-4" />
          Imprimir
        </button>
      </div>

      <div className="flex items-center gap-3 sm:gap-4 mb-10 print:mb-4">
        <Image
          src="/logo-elegis-light.png"
          alt="Elegis"
          width={120}
          height={42}
          priority
          className="h-9 w-auto sm:h-10"
        />
        <div className="h-10 w-1 bg-blue-600 sm:h-12" aria-hidden="true" />
        <div>
          <h1 className="text-lg font-bold leading-tight text-[#0b1b3f] sm:text-2xl">
            Dados Eleitorais
          </h1>
          <p className="text-xs text-gray-500 sm:text-sm">
            Resultados consolidados a partir do boletim de urna do TSE.
          </p>
        </div>
      </div>
    </header>
  );
}
