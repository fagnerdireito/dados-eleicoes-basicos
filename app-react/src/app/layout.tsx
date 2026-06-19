import type { Metadata } from "next";
import { Geist_Mono, Plus_Jakarta_Sans } from "next/font/google";
import "./globals.css";
import { Header } from "@/components/Header";

const plusJakartaSans = Plus_Jakarta_Sans({
  variable: "--font-sans",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700", "800"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Dados Eleitorais",
  description: "Dashboard de dados eleitorais básicos.",
  icons: {
    icon: "/favicon.png",
    shortcut: "/favicon.png",
    apple: "/favicon.png",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="pt-BR"
      className={`${plusJakartaSans.variable} ${geistMono.variable} h-full antialiased print:h-auto print:w-full`}
    >
      <body className="min-h-full flex flex-col bg-radial from-slate-300 to-slate-50 font-sans text-gray-900 print:m-0 print:p-0 print:w-full print:max-w-none print:bg-white mb-36">
        <main className="mx-auto w-full max-w-7xl p-4 grow flex flex-col print:max-w-none print:w-full print:mx-0 print:px-0 print:py-0 mb-36">
          <svg
            className="absolute inset-0 w-full h-full pointer-events-none opacity-[0.25] print:hidden" style={{ zIndex: -1 }}
            viewBox="0 0 1000 600"
            preserveAspectRatio="xMidYMid slice"
            aria-hidden
          >
            <defs>
              <radialGradient id="topoFade" cx="50%" cy="50%" r="60%">
                <stop offset="0%" stopColor="oklch(0.7 0.15 260)" stopOpacity="0.35" />
                <stop offset="100%" stopColor="oklch(0.7 0.15 260)" stopOpacity="0" />
              </radialGradient>
            </defs>
            <g fill="none" stroke="oklch(0.7 0.08 260)" strokeWidth="0.6" opacity="0.55">
              {Array.from({ length: 18 }).map((_, i) => {
                const r = 60 + i * 22;
                return (
                  <ellipse
                    key={i}
                    cx="500"
                    cy="310"
                    rx={r * 1.4}
                    ry={r * 0.85}
                    transform={`rotate(${i * 3} 500 310)`}
                  />
                );
              })}
            </g>
            <circle cx="500" cy="310" r="220" fill="none" stroke="oklch(0.55 0.18 260)" strokeWidth="1" strokeDasharray="2 4" opacity="0.4" />
            <circle cx="500" cy="310" r="220" fill="url(#topoFade)" />
            {[
              [180, 120], [820, 90], [880, 480], [120, 520], [940, 300], [60, 280],
            ].map(([cx, cy], i) => (
              <circle key={i} cx={cx} cy={cy} r="3" fill="oklch(0.55 0.18 260)" opacity="0.5" />
            ))}
          </svg>
          <Header />
          {children}
        </main>
      </body>
    </html>
  );
}
