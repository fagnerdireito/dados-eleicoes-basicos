'use client';

import Image from 'next/image';
import { MoreVertical, Printer } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';

export function Header() {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!menuOpen) return;

    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [menuOpen]);

  const handlePrint = () => {
    setMenuOpen(false);
    window.print();
  };

  return (
    <header className="mb-10 mt-10 w-full print:mb-10 print:mt-0 print:w-full">
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
              Painel Eleitoral
            </h1>
            <p className="text-xs text-gray-500 sm:text-sm">
              Resultados consolidados a partir do boletim de urna do TSE.
            </p>
          </div>
        </div>

        <button
          type="button"
          onClick={handlePrint}
          className="hidden shrink-0 items-center gap-2 rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm transition-colors hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 sm:inline-flex print:hidden"
        >
          <Printer className="h-4 w-4" />
          Imprimir
        </button>

        <div ref={menuRef} className="relative sm:hidden print:hidden">
          <button
            type="button"
            onClick={() => setMenuOpen((open) => !open)}
            aria-expanded={menuOpen}
            aria-haspopup="menu"
            aria-label="Abrir menu"
            className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-200 bg-white text-gray-700 shadow-sm transition-colors hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2"
          >
            <MoreVertical className="h-5 w-5" />
          </button>

          {menuOpen && (
            <div
              role="menu"
              className="absolute right-0 top-full z-50 mt-1 min-w-40 overflow-hidden rounded-md border border-gray-200 bg-white py-1 shadow-lg"
            >
              <button
                type="button"
                role="menuitem"
                onClick={handlePrint}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-gray-700 transition-colors hover:bg-gray-50"
              >
                <Printer className="h-4 w-4 text-indigo-600" />
                Imprimir
              </button>
            </div>
          )}
        </div>
      </div>
      <hr className="mb-0 mt-7 border-gray-200" />
    </header>
  );
}
