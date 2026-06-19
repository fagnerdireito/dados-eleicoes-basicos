import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Habilita Cache Components (necessário para a diretiva "use cache").
  cacheComponents: true,
  cacheLife: {
    // Sobrescreve o perfil padrão: os dados eleitorais são imutáveis após a
    // apuração, então qualquer "use cache" sem perfil explícito reaproveita o
    // resultado por muito tempo e evita novas idas ao banco para o mesmo filtro.
    default: {
      stale: 60 * 60 * 24,           // 1 dia (cache do cliente)
      revalidate: 60 * 60 * 24 * 30, // 30 dias (revalidação em background)
      expire: 60 * 60 * 24 * 365,    // 1 ano (expiração total)
    },
  },
};

export default nextConfig;
