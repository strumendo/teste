  # Capítulo 11 — Fórmulas Explicadas

Documento complementar ao **Relatório SABO R20**. Detalha, em linguagem
descritiva, todas as fórmulas usadas no Capítulo 11 ("Previsão de Troca de
Peças") e apresenta exemplos numéricos com dados reais do próprio relatório.

Data de referência usada nos exemplos: **14/04/2026** (data de emissão do R20).
Métricas do modelo: XGBoost — R² = 0,9534, MSE = 1094,50, MAE = 20,02.

---

## 11.1 — Previsão Histórica (Baseada em Intervalos)

### Para que serve

Estimar a próxima troca de peças usando apenas o **histórico de intervalos**
entre trocas anteriores, sem considerar o estado atual do equipamento. É o
método mais simples e funciona como linha de base de comparação.

### Fórmula 1 — Data prevista da próxima troca

> **Data Prevista da Próxima Troca** é igual à **Data da Última Troca já
> realizada no equipamento**, somada ao **Intervalo Médio Histórico em dias**
> (ou seja, a média de dias entre as trocas anteriores daquele mesmo
> equipamento).

Em outras palavras: "Pegue a última data em que o equipamento teve uma peça
substituída e some a quantidade média de dias que esse equipamento costuma
operar entre uma troca e outra."

### Fórmula 2 — Quantos dias ainda faltam

> **Dias Restantes até a Próxima Troca** é igual à diferença entre a **Data
> Prevista da Próxima Troca** (calculada na Fórmula 1) e a **Data de Hoje**.
> O resultado é expresso em número inteiro de dias.

Quando o resultado é **negativo**, significa que a data prevista já passou e o
equipamento entrou em situação de **ATRASADO** — a troca deveria ter sido
feita há tantos dias quanto o valor absoluto do número.

### Exemplo numérico — Equipamento IJ-044 (situação OK)

| Termo da fórmula                         | Valor                |
| ---------------------------------------- | -------------------- |
| Data da Última Troca                     | 08/11/2025           |
| Intervalo Médio Histórico                | 531 dias             |
| Data de Hoje (referência)                | 14/04/2026           |

Aplicando a Fórmula 1:
"08/11/2025 mais 531 dias dá **23/04/2027**" — esta é a data prevista da
próxima troca.

Aplicando a Fórmula 2:
"De 14/04/2026 até 23/04/2027 são **373 dias** restantes" — como o número é
positivo, o equipamento está dentro do prazo.

### Exemplo numérico — Equipamento IJ-046 (situação ATRASADO)

| Termo da fórmula                         | Valor                |
| ---------------------------------------- | -------------------- |
| Data da Última Troca                     | 05/01/2025           |
| Intervalo Médio Histórico                | 343 dias             |
| Data de Hoje (referência)                | 14/04/2026           |

Aplicando a Fórmula 1:
"05/01/2025 mais 343 dias dá **14/12/2025**" — esta data já passou.

Aplicando a Fórmula 2:
"De 14/04/2026 até 14/12/2025 são **−122 dias**" — o número negativo indica
que a troca está **122 dias atrasada** segundo o critério histórico.

---

## 11.2 — Previsão Prescritiva (Modelo XGBoost)

### Para que serve

Prever a próxima troca usando o **modelo de Machine Learning XGBoost**
(R² = 0,9534) treinado sobre o histórico de operação. Ao contrário do método
histórico, este modelo considera o **estado atual do equipamento** — produção
acumulada, desgastes, refugo, medições de cilindro e fuso — e ajusta a
previsão dinamicamente.

### Fórmula 3 — Dias previstos pelo modelo

> **Dias Previstos pelo Modelo ML** é o valor numérico devolvido pelo modelo
> XGBoost quando recebe como entrada o **último estado conhecido do
> equipamento** (último registro disponível no arquivo `data_eda.csv`,
> contendo as 22 variáveis usadas no treinamento). O modelo aprendeu, durante
> a fase de treino, a relação entre essas variáveis e o número de dias até a
> próxima manutenção.

Em linguagem simples: "O modelo lê o estado atual do equipamento (quanto
produziu, qual o nível de desgaste, qual a taxa de refugo, etc.) e devolve um
número que estima quantos dias ainda faltam até precisar trocar peças."

Caso o modelo retorne valor negativo, ele é forçado para zero (não faz
sentido prever "dias negativos restantes").

### Fórmula 4 — Data prevista pelo modelo

> **Data Prevista da Próxima Troca pelo Modelo** é igual à **Data de Hoje**
> somada ao número de **Dias Previstos pelo Modelo ML** (Fórmula 3).

### Fórmula 5 — Classificação de status

A classificação **ATRASADO / URGENTE / ATENÇÃO / OK** é decidida apenas com
base em **quantos dias o modelo prevê até a próxima troca**:

> Se o número de dias previstos é **menor que zero**, o status é **ATRASADO**
> (a troca já deveria ter ocorrido).
>
> Se o número de dias previstos é **igual ou menor que 30**, o status é
> **URGENTE** (resta menos de um mês).
>
> Se o número de dias previstos é **maior que 30 e menor ou igual a 90**, o
> status é **ATENÇÃO** (entre um e três meses).
>
> Se o número de dias previstos é **maior que 90**, o status é **OK** (mais
> de três meses de margem).

### Exemplo numérico — Equipamento IJ-119 (URGENTE)

O modelo XGBoost analisou o último registro do IJ-119 e devolveu **3 dias**
como previsão.

Aplicando a Fórmula 4:
"14/04/2026 mais 3 dias dá **17/04/2026**" — data prevista pelo ML.

Aplicando a Fórmula 5:
"3 dias está entre 0 e 30 → status **URGENTE**".

### Exemplo numérico — Equipamento IJ-044 (OK)

O modelo devolveu **380 dias** para o IJ-044.

Aplicando a Fórmula 4:
"14/04/2026 mais 380 dias dá **29/04/2027**".

Aplicando a Fórmula 5:
"380 dias é maior que 90 → status **OK**".

### Exemplo numérico — Equipamento IJ-118 (URGENTE)

O modelo devolveu **18 dias** para o IJ-118.

Aplicando a Fórmula 4:
"14/04/2026 mais 18 dias dá **02/05/2026**".

Aplicando a Fórmula 5:
"18 dias está entre 0 e 30 → status **URGENTE**".

---

## 11.3 — Comparação: Histórico vs. Modelo ML

### Para que serve

Confrontar os dois métodos para identificar quando o modelo recomenda
**antecipar** ou **adiar** a manutenção em relação ao calendário histórico, e
quando ambos concordam ("Conforme").

### Fórmula 6 — Diferença entre os dois métodos

> **Diferença em Dias** é igual ao número de **Dias Previstos pelo Modelo ML**
> (Fórmula 3) menos o número de **Dias Restantes pelo Histórico** (Fórmula 2).

Quando o resultado é positivo, o modelo está dizendo "o equipamento aguenta
mais tempo do que o histórico sugere". Quando é negativo, está dizendo "o
equipamento precisa de manutenção antes do que o histórico previa".

### Fórmula 7 — Recomendação operacional

> Se a **Diferença em Dias** é **menor que −30**, a recomendação é
> **Antecipar** (o modelo aponta degradação acima do esperado e sugere
> adiantar a manutenção em relação ao calendário).
>
> Se a **Diferença em Dias** é **maior que +30**, a recomendação é
> **Pode adiar** (o equipamento está em condições melhores do que o histórico
> sugere e pode operar mais tempo).
>
> Se a **Diferença em Dias** está **entre −30 e +30 inclusive**, a
> recomendação é **Conforme** (os dois métodos concordam dentro de uma
> tolerância de um mês).

### Exemplo numérico — Equipamento IJ-046

| Termo                                    | Valor       |
| ---------------------------------------- | ----------- |
| Dias Previstos pelo Modelo ML            | 18 dias     |
| Dias Restantes pelo Histórico            | −122 dias   |

Aplicando a Fórmula 6:
"18 menos (−122) dá **+140 dias** de diferença" — o modelo prevê 140 dias a
mais que o histórico.

Aplicando a Fórmula 7:
"+140 é maior que +30 → recomendação **Pode adiar**".

Interpretação prática: o histórico marcaria o IJ-046 como atrasado, mas o
modelo, ao analisar o estado real do equipamento, percebe que ele ainda tem
margem de operação.

### Exemplo numérico — Equipamento IJ-134

| Termo                                    | Valor       |
| ---------------------------------------- | ----------- |
| Dias Previstos pelo Modelo ML            | 301 dias    |
| Dias Restantes pelo Histórico            | 272 dias    |

Aplicando a Fórmula 6:
"301 menos 272 dá **+29 dias** de diferença".

Aplicando a Fórmula 7:
"+29 está entre −30 e +30 → recomendação **Conforme**".

---

## 11.4 — Fatores Considerados pelo Modelo ML

Esta seção descreve as fórmulas das **variáveis derivadas** que alimentam o
modelo XGBoost. São os "olhos" do modelo: cada uma traduz um aspecto físico
do equipamento em um número que o algoritmo consegue processar.

### Fórmula 8 — Desgaste do Cilindro

> **Desgaste do Cilindro** (em milímetros) é igual à **Maior Medição de
> Cilindro registrada na ordem de produção** menos o valor nominal de
> referência **20,0 mm** (diâmetro de fábrica considerado como ponto zero do
> desgaste).

Quando o cilindro novo tem 20 mm e a leitura atual mostra 20,4 mm, o desgaste
é de 0,4 mm. Quanto maior o número, mais desgastado está o cilindro.

### Fórmula 9 — Desgaste do Fuso

> **Desgaste do Fuso** (em milímetros) é igual ao valor nominal de referência
> **20,0 mm** menos a **Menor Medição de Fuso registrada na ordem de
> produção**.

A lógica é inversa à do cilindro: o fuso novo tem 20 mm e vai diminuindo com
o uso, então o desgaste é "o quanto ele encolheu" em relação ao nominal.

### Fórmula 10 — Variação dimensional

> **Variação do Cilindro** é igual à **Maior Medição** menos a **Menor
> Medição** de cilindro daquela ordem de produção. A **Variação do Fuso** é
> calculada da mesma forma para o fuso.

Mede a oscilação dimensional dentro de uma mesma ordem — quanto maior, mais
instável está a peça.

### Fórmula 11 — Taxa de Desgaste

> **Taxa de Desgaste do Cilindro** é igual ao **Desgaste do Cilindro**
> (Fórmula 8) dividido pelo **Intervalo de Manutenção em dias** daquele
> equipamento. A **Taxa de Desgaste do Fuso** segue a mesma lógica usando a
> Fórmula 9.

Resultado: desgaste **por dia** de operação. Permite comparar equipamentos
com intervalos de manutenção diferentes em uma base homogênea.

### Fórmula 12 — Índice de Desgaste Combinado (escala 0 a 100)

> **Índice de Desgaste** é a soma ponderada de dois sub-scores:
>
> 1. O **Sub-score do Cilindro** é calculado dividindo o **Desgaste do
>    Cilindro** (Fórmula 8) pelo valor de referência **0,6 mm**, multiplicando
>    por **100** e limitando o resultado em **100** (qualquer desgaste acima
>    de 0,6 mm é considerado 100% — desgaste crítico).
>
> 2. O **Sub-score do Fuso** é calculado dividindo o **Desgaste do Fuso**
>    (Fórmula 9) pelo valor de referência **2,0 mm**, multiplicando por
>    **100** e limitando o resultado em **100**.
>
> 3. O índice final dá **peso 60% para o cilindro** e **peso 40% para o
>    fuso**, refletindo a maior criticidade do desgaste do cilindro nas
>    extrusoras Y125.

Resultado: número entre 0 (equipamento como novo) e 100 (equipamento em
desgaste crítico).

### Exemplo numérico — Cálculo do Índice de Desgaste

Suponha um equipamento com leituras: cilindro_max = 20,30 mm e fuso_min =
19,40 mm.

Aplicando a Fórmula 8: "20,30 menos 20,0 dá **0,30 mm** de desgaste do
cilindro".

Aplicando a Fórmula 9: "20,0 menos 19,40 dá **0,60 mm** de desgaste do fuso".

Sub-score do cilindro: "0,30 dividido por 0,6 dá 0,5; vezes 100 dá **50** (não
ultrapassa o limite de 100)".

Sub-score do fuso: "0,60 dividido por 2,0 dá 0,3; vezes 100 dá **30**".

Índice combinado: "50 vezes 0,6 dá 30; mais 30 vezes 0,4 dá 12; somando dá
**Índice de Desgaste = 42**" (numa escala 0–100, equipamento em desgaste
moderado).

### Fórmula 13 — Desgaste por 1000 peças

> **Desgaste por 1000 peças** é igual ao **Desgaste acumulado do equipamento**
> (medido pelas Fórmulas 8 e 9 no último registro) dividido pela **Quantidade
> Produzida Acumulada** desde a última troca, multiplicado por **1000** para
> normalizar.

Permite comparar equipamentos com volumes de produção muito diferentes — um
equipamento pode estar pouco desgastado em valor absoluto mas, se produziu
pouco, sua taxa por 1000 peças pode ser alta.

### Fórmula 14 — Variáveis Acumuladas (4 features)

> Para cada uma das variáveis **Quantidade Produzida**, **Quantidade
> Refugada**, **Quantidade Retrabalhada** e **Consumo de Massa em Kg por 100
> peças**, a versão **Acumulada** é a **soma cumulativa, ordenada por data,
> agrupada por equipamento**.

Exemplo: se o equipamento IJ-044 produziu 1000 peças em 01/01, 800 em 02/01 e
1200 em 03/01, a coluna `Qtd_Produzida_Acumulado` terá os valores 1000, 1800
e 3000 respectivamente para cada uma dessas linhas. Essas variáveis dão ao
modelo a noção de "quanto desse equipamento já foi gasto desde a última
troca".

### Fórmula 15 — Taxa de Refugo / Retrabalho

> **Taxa de Refugo** é a razão entre a **Quantidade Refugada Acumulada** e a
> **Quantidade Produzida Acumulada**, geralmente expressa em percentual
> (multiplicada por 100).
>
> A **Taxa de Retrabalho** segue a mesma fórmula usando a **Quantidade
> Retrabalhada Acumulada** no numerador.

Refugo e retrabalho crescentes ao longo do tempo sinalizam degradação do
equipamento — peças saindo fora de especificação.

### Resumo dos 6 fatores principais (como aparecem no PDF)

| Fator (PDF)                  | Fórmula derivada       | O que capta                          |
| ---------------------------- | ---------------------- | ------------------------------------ |
| Produção Acumulada           | Fórmula 14             | Volume total operado desde a troca   |
| Índice de Desgaste           | Fórmula 12             | Score 0–100 de desgaste físico       |
| Medições de Cilindro         | Fórmulas 8 e 10        | Estado dimensional do cilindro       |
| Medições de Fuso             | Fórmulas 9 e 10        | Estado dimensional do fuso           |
| Taxa de Refugo/Retrabalho    | Fórmula 15             | Indicador de qualidade               |
| Consumo de Massa             | Fórmula 14             | Eficiência de matéria-prima          |
| Taxa de Desgaste             | Fórmulas 11 e 13       | Desgaste por dia ou por peça         |

---

## Resumo visual do fluxo de previsão

```
                    ESTADO ATUAL DO EQUIPAMENTO
                  (último registro em data_eda.csv)
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │  22 features numéricas:                  │
        │  - Acumuladas (Fórmula 14)               │
        │  - Desgastes (Fórmulas 8, 9, 10)         │
        │  - Taxas (Fórmulas 11, 13, 15)           │
        │  - Índice combinado (Fórmula 12)         │
        └──────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ Modelo XGBoost  │  (R² = 0,9534)
                    └─────────────────┘
                              │
                              ▼
                  Dias previstos (Fórmula 3)
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
    Data prevista       Status (Fórmula 5)  Comparação com
    (Fórmula 4)         OK/ATENÇÃO/         histórico
                        URGENTE/ATRASADO    (Fórmulas 6, 7)
```

---

**Origem das fórmulas no código:**

- Fórmulas 1, 2, 3, 4, 5, 6, 7 → `Fase02/scripts/s06_generate_report.py`
  (função `generate_chapter_11` e `predict_maintenance_with_ml`).
- Fórmulas 8 a 15 → `Fase02/scripts/s02_preprocessing.py`
  (funções `_calc_indice_desgaste`, `_calc_desgaste_por_pecas` e bloco de
  cálculo das medições de cilindro/fuso).
