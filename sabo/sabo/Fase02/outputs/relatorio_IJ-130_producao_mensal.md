# Relatório de Produção Mensal — IJ-130

**Fonte:** `data/raw/IJ-130.csv`
**Período coberto:** 08/05/2025 → 20/03/2026 (~10,5 meses)
**Total de registros (apontamentos):** 116
**Total produzido no período:** 20.822 peças
**Total refugado:** 276 peças (1,32% do produzido)
**Total retrabalhado:** 0 peças

## Produção mensal

| Ano-Mês  | Apontamentos | Dias com produção | Qtd. Produzida | % do total | Refugo | Taxa refugo |
|----------|-------------:|------------------:|---------------:|-----------:|-------:|------------:|
| 2025-05  | 10           | 4                 | 1.740          | **8,36 %** | 18     | 1,03 %      |
| 2025-06  | 0            | 0                 | 0              | **0,00 %** | 0      | —           |
| 2025-07  | 0            | 0                 | 0              | **0,00 %** | 0      | —           |
| 2025-08  | 0            | 0                 | 0              | **0,00 %** | 0      | —           |
| 2025-09  | 0            | 0                 | 0              | **0,00 %** | 0      | —           |
| 2025-10  | 0            | 0                 | 0              | **0,00 %** | 0      | —           |
| 2025-11  | 41           | 18                | 8.996          | **43,20 %**| 44     | 0,49 %      |
| 2025-12  | 18           | 8                 | 3.457          | **16,60 %**| 30     | 0,87 %      |
| 2026-01  | 12           | 5                 | 1.468          | **7,05 %** | 53     | 3,61 %      |
| 2026-02  | 29           | 16                | 4.475          | **21,49 %**| 117    | 2,61 %      |
| 2026-03  | 6            | 4                 | 686            | **3,29 %** | 14     | 2,04 %      |
| **TOTAL**| **116**      | **55**            | **20.822**     | **100,00 %**| **276** | **1,32 %**  |

> Obs.: 2026-03 está parcial (dados até 20/03).

## Observações relevantes

- **Hiato de produção: 6 meses sem registros** — entre 06/2025 e 10/2025 a IJ-130 não tem apontamentos. O histórico de preventivas RM.195 (carregado no commit `bc4bac7`) confirma uma preventiva agendada em **18/09/2025**, exatamente no meio desse hiato. É consistente com equipamento parado para manutenção / aguardando ordem.
- **Concentração da produção:** quase **81 %** do total (≈16,9k peças) foi produzido em apenas 4 meses — nov/2025, dez/2025, fev/2026 e maio/2025. O retorno após a parada (nov/2025 com 43,20 %) é o pico absoluto.
- **Tendência da taxa de refugo:** depois da parada, o refugo subiu de 0,5–1 % (mai–dez/2025) para 2,0–3,6 % (jan–mar/2026). Pode indicar reajuste de processo pós-preventiva ou produto/composto diferente nesse período.
- **Dados ausentes não significam zero "0":** os meses jun–out/2025 não estão no CSV. Eu os exibi como zero para fechar a série, mas o pipeline atual descarta esses meses (não há linhas para agregar). Se a intenção for tratá-los como dias de ociosidade, isso já é capturado em `outputs/equipamentos_ociosidade.csv` no s07.
