import Image from 'next/image';
import Link from 'next/link';

export function Header() {
  return (
    <header className="bg-[#0b2545] text-white p-4 shadow-md">
      <div className="container mx-auto flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* Logo will go here if we copy the image over */}
          <div className="text-xl font-bold">🗳️ Dados Eleitorais</div>
        </div>
        <nav>
          <ul className="flex space-x-4">
            <li>
              <Link href="/" className="hover:underline">Início</Link>
            </li>
            {/* Additional navigation links can go here */}
          </ul>
        </nav>
      </div>
    </header>
  );
}
