# Cache de consultas ao banco (Cache Components / "use cache")

## Objetivo
Evitar novas consultas ao PostgreSQL quando o filtro (ano/UF/município/cargo/
candidato) é o mesmo. Antes, toda exibição de dados disparava query nova.

## Como funciona
Usa o recurso **Cache Components** do Next.js 16 (`"use cache"`). A chave de
cache é gerada automaticamente a partir dos argumentos de cada função — ou seja,
o mesmo conjunto de filtros reaproveita o resultado em memória; filtros
diferentes geram entradas separadas.

## Arquivos alterados
- `next.config.ts`
  - `cacheComponents: true` (habilita `"use cache"` e ativa PPR por padrão).
  - Perfil `cacheLife.default` sobrescrito com TTL longo (revalidate 30 dias,
    expire 1 ano), pois dados eleitorais são imutáveis após a apuração.
- `src/app/actions-tab2.ts` ... `actions-tab10.ts`
  - Diretiva `"use cache"` a nível de arquivo (queries pesadas das abas).
- `src/lib/queries-filtros.ts` (novo)
  - Consultas dos filtros globais com `"use cache"` a nível de arquivo.
- `src/app/actions.ts`
  - Continua `"use server"`, mas agora só faz wrappers que chamam
    `queries-filtros.ts`. Necessário porque uma função `"use server"` não pode
    receber `"use cache"` diretamente, e ela é usada também por Client
    Components (GlobalFilters/pickers).
- `src/app/page.tsx`
  - O `await searchParams` foi movido para dentro de um `<Suspense>`
    (componente `TabContent`). Com PPR, acessar dados de runtime fora de
    Suspense quebra o prerender do shell estático.

## Observações importantes
- **Ambiente self-hosted (Node.js):** o cache em memória persiste entre
  requisições — comportamento desejado aqui. Em serverless o cache em memória
  NÃO persiste entre requisições; nesse caso seria preciso `"use cache: remote"`
  (Redis/KV).
- Revalidação manual (se algum dia os dados mudarem): usar `cacheTag` nas
  funções + `revalidateTag`/`updateTag` numa Server Action.
- Build confirma: rota `/` virou Partial Prerender (`◐`).

## Deploy no VPS próprio (ambiente usado neste projeto)
Como o app roda num **servidor Node.js de longa duração**, o cache em memória
persiste entre requisições — não é preciso nenhum serviço externo (Redis/KV).

Fluxo de deploy:
1. `npm run build`
2. `npm run start` (= `next start`, modo produção). **Nunca usar `next dev` em
   produção** — o cache se comporta diferente em dev.
3. Manter o processo sempre de pé (PM2, systemd ou container). O cache vive na
   memória desse processo; se ele reiniciar, o cache reaquece na primeira
   requisição de cada filtro.
4. Garantir o `.env` com as variáveis `PGSQL_VECTOR_*` no servidor.

Observações:
- Em PM2 modo **cluster** ou múltiplas réplicas/load balancer, cada instância
  tem seu próprio cache em memória (não compartilham). Funciona normalmente;
  apenas cada instância aquece o cache separadamente.
- Tamanho do cache em memória pode ser ajustado via `cacheMaxMemorySize` no
  `next.config.ts`, se necessário.
