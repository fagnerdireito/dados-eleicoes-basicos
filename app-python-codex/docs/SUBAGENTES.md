# Subagentes utilizados

O planejamento e a implementacao foram apoiados por tres frentes paralelas:

1. **Banco e dados**: inventario read-only do PostgreSQL, cobertura por ano/turno e lista de dados TSE ausentes.
2. **Analise visual**: leitura das nove imagens, identificacao de cards, tabelas, mapas e mapeamento 1:1 de abas.
3. **Arquitetura**: revisao dos tres prototipos existentes, componentes reaproveitaveis e riscos de SQL.

Principais achados incorporados:

- filtrar `NR_TURNO` e `CD_ELEICAO` em todas as consultas;
- incluir o turno nos joins com `local_votacao`;
- nao misturar votos de legenda em rankings nominais;
- evitar nomes de tabelas divergentes usados por prototipos anteriores;
- manter erros de esquema visiveis em vez de converte-los silenciosamente em placeholders.
