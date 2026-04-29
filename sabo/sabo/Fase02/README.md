# SABO - Pipeline de Machine Learning para Manutenção Prescritiva

Sistema de Machine Learning para **manutenção prescritiva** em extrusoras de borracha Y125.

## Visão Geral

Este pipeline implementa um fluxo completo de ciência de dados para manutenção prescritiva, desde a coleta de dados até a geração de relatórios PDF com **previsões baseadas em ML**. O objetivo é prever quantos dias faltam até a próxima manutenção de cada equipamento com base em:

- Dados históricos de produção
- **Medições de desgaste** (cilindro e fuso)
- **Estado atual do equipamento** (produção acumulada, taxa de refugo, índice de desgaste)

**Período dos Dados:** 19/03/2023 a 31/01/2026 (34 meses)

**Melhor Modelo:** XGBoost (R² = 0.8369, MAE = 39,43 dias, MSE = 4206,15) — última execução `R23` (2026-04-28)

**Equipamentos Monitorados:** 27 extrusoras Y125 (IJ-044 a IJ-164)

### Diferencial: Previsão Prescritiva

O sistema oferece **três fontes de previsão de manutenção**, consolidadas em um único CSV:

| Fonte | Descrição | Origem |
|-------|-----------|--------|
| **Histórica** | Última troca + intervalo médio entre trocas | `equipment_stats.json` |
| **SAP (RM.195)** | Próxima preventiva agendada no SAP | `Histórico Geral Preventivas RM.195 - 27 Equip IRP´s.xlsx` |
| **Prescritiva (ML)** | Data prescrita pelo modelo XGBoost com base no estado atual | `prescricao_manutencao.csv` |

A previsão ML considera produção acumulada, índice de desgaste, medições de cilindro/fuso, taxa de refugo, consumo de massa e tempo de ociosidade para determinar quando cada equipamento precisará de manutenção. A consolidação das três fontes é gravada em `outputs/previsao_manutencao_consolidada.csv` e referenciada como link clicável no Capítulo 11 do PDF.

## Requisitos

### Sistema
- Python 3.8+ (testado com Python 3.12)
- Sistema operacional: Linux/Windows/macOS

### Ambiente Virtual (venv) — Local Canônico

**O venv oficial do projeto é `Fase02/.venv`**, e é o único local que os scripts
esperam. Não use venvs em outros pontos da árvore (como `Fase02/scripts/.venv`
ou `sabo/.venv`) — se existirem, ignore-os.

```bash
# 1. Entrar no diretório do projeto
cd caminho/para/Fase02

# 2. Criar o venv (só na primeira vez)
python3 -m venv .venv
# Em distros Debian/Ubuntu/Pop!_OS, se faltar o módulo venv:
#   sudo apt install python3-venv python3-pip
# Se ensurepip não estiver disponível e não puder usar sudo, faça:
#   python3 -m venv --without-pip .venv
#   curl -fsSL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
#   .venv/bin/python /tmp/get-pip.py

# 3. Ativar
source .venv/bin/activate     # Linux/macOS
# .venv\Scripts\activate      # Windows

# 4. Instalar dependências
pip install -r requirements.txt
# ou, manualmente:
pip install pandas numpy scikit-learn xgboost matplotlib seaborn reportlab joblib openpyxl pypdf
```

Todos os comandos do pipeline (incluindo os scripts de manutenção `s07`, `s08`
e `append_capitulo_18_manutencao.py`) assumem que esse venv está ativado.

#### Lista de Dependências
| Pacote | Versão Mínima | Descrição |
|--------|---------------|-----------|
| pandas | 2.0+ | Manipulação de dados |
| numpy | 1.24+ | Computação numérica |
| scikit-learn | 1.3+ | Algoritmos de ML |
| xgboost | 2.0+ | Gradient boosting |
| matplotlib | 3.7+ | Visualização |
| seaborn | 0.12+ | Gráficos estatísticos |
| reportlab | 4.0+ | Geração de PDF |
| pypdf | 4.0+ | Manipulação de PDF (append de capítulos) |
| joblib | 1.3+ | Serialização de modelos |
| openpyxl | 3.1+ | Leitura de arquivos Excel |

## Estrutura do Projeto

```
Fase02/
├── scripts/                     # Scripts do pipeline
│   ├── run_pipeline.py          # Orquestrador principal
│   ├── auto_pipeline.py         # Automação com detecção de alterações
│   ├── s00_split_unified.py     # Etapa 0: Split de planilhas unificadas
│   ├── s01_data_collection.py   # Etapa 1: Coleta e integração
│   ├── s02_preprocessing.py     # Etapa 2: Pré-processamento
│   ├── s03_eda.py               # Etapa 3: Análise exploratória
│   ├── s03b_advanced_eda.py     # Etapa 3b: EDA avançado
│   ├── s04_modeling.py          # Etapa 4: Modelagem
│   ├── s05_evaluation.py        # Etapa 5: Avaliação
│   ├── s06_generate_report.py   # Etapa 6: Relatório PDF + CSV/PPTX consolidados (Cap. 11)
│   ├── s07_hist_manutencao.py   # Etapa 7: Histórico de manutenção por equipamento
│   ├── s08_prescricao_manutencao.py     # Etapa 8: Data prescrita pelo ML
│   ├── gerar_relatorio_componentes_mensal.py  # Geração dos MD por equipamento
│   ├── gerar_apresentacao_componentes.py      # Geração dos PPTX/PDF por equipamento
│   ├── append_capitulo_17.py    # Anexa o Capítulo 17 ao último PDF
│   ├── append_capitulo_18_manutencao.py # Anexa o Capítulo 18 (consumo + manutenção)
│   └── history_manager.py       # Gerenciador de histórico
│
├── config/
│   └── paths.py                 # Configuração centralizada de caminhos
│
├── data/
│   ├── manutencao/              # PASTA DE MANUTENÇÃO (NOVA!)
│   │   ├── Dados Manut*.xlsx    # Dados de manutenção e medições
│   │   └── dados_manutencao.csv # Histórico de manutenção
│   └── raw/                     # Dados brutos de produção
│       ├── IJ-*.xlsx            # Arquivos por equipamento
│       └── DadosProducao*.xlsx  # Dados consolidados de produção
│
└── outputs/                     # Saídas do pipeline
    ├── eda_plots/               # Gráficos gerados
    ├── models/                  # Modelos treinados
    ├── history/                 # Histórico de execuções
    ├── relatorios_mensais_componentes/        # MD por equipamento
    ├── relatorios_mensais_componentes_ppt/    # PPTX/PDF por equipamento (27 IJ-*.pptx)
    ├── equipment_stats.csv      # Estatísticas por equipamento
    ├── equipment_stats.json     # Estatísticas em JSON
    ├── prescricao_manutencao.csv               # Saída do s08 (data prescrita ML)
    ├── previsao_manutencao_consolidada.csv     # NOVO! Cap. 11 — histórico × SAP × ML
    ├── Apresentacoes_Componentes_Consolidado.pptx  # NOVO! Cap. 11 — merge dos PPTX
    ├── .data_state.json         # Estado para automação
    └── Relatorio_SABO_RX.pdf    # Relatório final
```

## Execução Detalhada

### Pré-requisitos para execução

1. **Ambiente virtual ativado** com as dependências instaladas (ver seção "Dependências Python").
2. **Dados de entrada nas pastas corretas** (ver "Preparação dos Dados" abaixo).
3. **Sempre executar os scripts a partir de `Fase02/scripts/`** — o orquestrador
   faz `os.chdir(OUTPUTS_DIR)` logo no início e importa os módulos das etapas
   por nome simples via `sys.path`. Rodar de outro diretório quebra os imports.

### Preparação dos Dados

Antes da primeira execução, distribua os arquivos conforme abaixo:

**`Fase02/data/raw/`** (append-only — nunca sobrescreva):
- Arquivos de produção por equipamento: `IJ-044.xlsx`, `IJ-046.xlsx`, …, `IJ-164.xlsx`
- Dados consolidados (opcional): `DadosProducao*.xlsx`
- Históricos estendidos (opcional): `IJ-*.2.xlsx`

**`Fase02/data/manutencao/`**:
- Histórico de manutenção + medições de desgaste: `Dados Manut - 27 Equip - 2025.xlsx`
- CSV histórico (opcional): `dados_manutencao.csv`

**`Fase02/data/arquivo_unico/`** (opcional):
- Arquivos agregados vindos de sistemas externos. A Etapa 0 detecta, divide e
  move os arquivos processados para `data/arquivo_unico_processado/`.

### Primeira Execução — passo a passo

```bash
# 1. Ir para Fase02 e ativar o venv (ver "Ambiente Virtual (venv)")
cd caminho/para/Fase02
source .venv/bin/activate            # Linux/macOS
# .venv\Scripts\activate             # Windows

# 2. Instalar dependências (apenas na primeira vez)
pip install -r requirements.txt

# 3. Entrar no diretório dos scripts (obrigatório, ver pré-requisitos)
cd scripts

# 4. Executar o pipeline completo
python run_pipeline.py
```

Ao final, o relatório fica em `Fase02/outputs/Relatorio_SABO_R<N>.pdf`, onde
`<N>` é auto-incrementado a partir do último relatório existente.

---

## Comando Principal: `run_pipeline.py`

Orquestra as 8 etapas do pipeline (0, 1, 2, 3, 3b, 4, 5, 6). Sem argumentos,
executa tudo em sequência. Etapas marcadas como `optional=True` na declaração
`PIPELINE_STEPS` não abortam o pipeline em caso de falha — são logadas e a
execução segue.

### Referência completa de flags

| Flag | Tipo / Valores | Default | Descrição |
|------|----------------|---------|-----------|
| `--step N` | `{0, 1, 2, 3, 3b, 4, 5, 6}` | — | Executa somente a etapa indicada |
| `--list` | *(sem valor)* | — | Lista as etapas do pipeline e sai |
| `--diagram` | *(sem valor)* | — | Imprime um diagrama ASCII do fluxo e sai |
| `--history` | *(sem valor)* | — | Lista execuções anteriores de `outputs/history/runs/` |
| `--compare N` | `int` | `0` | Imprime tabela Markdown comparando as últimas N execuções |
| `--no-history` | *(sem valor)* | — | Não salva esta execução no histórico |
| `--inicio YYYY-MM-DD` | `str` | — | Filtra dados a partir desta data (inclusive) |
| `--fim YYYY-MM-DD` | `str` | — | Filtra dados até esta data (inclusive) |
| `--suffix TEXTO` | `str` | `""` | Acrescenta sufixo ao nome do relatório (ex.: `_v1`) |
| `--version X` | `str` | — | Fixa a versão do relatório (ex.: `R21`), sobrescrevendo o auto-incremento |

### Cenários comuns

#### Regenerar apenas o PDF (sem retreinar modelos)

```bash
python run_pipeline.py --step 6
```

Usa os artefatos já salvos (`best_model.joblib`, `evaluation_report.txt`,
`data_eda.csv`) para remontar o PDF. Útil quando você ajustou o
`s06_generate_report.py` e quer republicar sem pagar o custo de retreinamento.

#### Retreinar e reavaliar sem refazer coleta / pré-processamento

```bash
python run_pipeline.py --step 4 && \
python run_pipeline.py --step 5 && \
python run_pipeline.py --step 6
```

As etapas 4 (modelagem) e 5 (avaliação) leem `data_eda.csv` já existente, então
essa cadeia é rápida se os dados não mudaram.

#### Sobrescrever um relatório específico em vez de incrementar

```bash
python run_pipeline.py --step 6 --version R21
```

Gera `Fase02/outputs/Relatorio_SABO_R21.pdf`, sobrescrevendo se já existir.
Sem `--version`, o gerador auto-incrementa (`R21` → `R22` → `R23` …).

#### Gerar relatório recortado por período

```bash
python run_pipeline.py --inicio 2025-01-01 --fim 2025-12-31 --version R_2025
```

`--inicio` e `--fim` fluem pelo `pipeline_context` até cada etapa que aceita
filtro de data. O resultado é `Relatorio_SABO_R_2025.pdf`.

#### Versão rascunho (com sufixo)

```bash
python run_pipeline.py --step 6 --suffix _rascunho
```

Produz `Relatorio_SABO_R<N>_rascunho.pdf` sem afetar o próximo auto-incremento
de versão numérica.

#### Inspecionar o pipeline sem executar nada

```bash
python run_pipeline.py --list      # ordem, nomes, inputs/outputs de cada etapa
python run_pipeline.py --diagram   # diagrama ASCII do fluxo
```

#### Rodar apenas os cruzamentos de manutenção (Item 7.4)

```bash
# Já são chamados pelo pipeline completo; também rodam isoladamente:
python run_pipeline.py --step 7    # s07_hist_manutencao — cruza xlsx × raw
python run_pipeline.py --step 8    # s08_prescricao_manutencao — prescrição
python append_capitulo_18_manutencao.py --keep-original
```

A etapa 7 lê TODOS os `data/manutencao/Dados Manut*.xlsx` (2025, 2026, …) e
preserva cada leitura como uma fotografia histórica por equipamento (a
apresentação "recente" é a mais nova; todas continuam disponíveis em
`equipamentos_historico_completo.csv`). A etapa 8 prescreve a próxima data de
manutenção integrando (i) desgaste medido nas peças substituídas, (ii) consumo
de massa na janela atual e (iii) dias de ociosidade da máquina. O
`append_capitulo_18_manutencao.py` anexa, ao último `Relatorio_SABO_R*.pdf`,
um capítulo com os números e gráficos por equipamento — seguindo o mesmo
padrão desacoplado do capítulo 17.

#### Auditar execuções anteriores

```bash
python run_pipeline.py --history       # lista todos os run-ids gravados
python run_pipeline.py --compare 5     # tabela Markdown com métricas dos últimos 5 runs
```

Cada execução é registrada em `outputs/history/runs/run_<YYYYMMDD_HHMMSS>.json`
com métricas por etapa e no `outputs/history/reports/report_<id>.txt` em texto.

#### Execução "suja" (sem gravar histórico)

```bash
python run_pipeline.py --step 6 --no-history
```

Útil para testes exploratórios que não devem poluir o histórico de produção.

---

## Automação: `auto_pipeline.py`

Detecta alterações nos dados de entrada via hash MD5 persistido em
`outputs/.data_state.json`. Se algum arquivo em `data/raw/`,
`data/manutencao/` ou `data/arquivo_unico/` mudou (ou é novo), reexecuta o
pipeline completo. Útil para agendar em cron / systemd ou rodar em background.

### Referência completa de flags

| Flag | Tipo | Default | Descrição |
|------|------|---------|-----------|
| `--status` | *(sem valor)* | — | Mostra hashes atuais dos arquivos monitorados |
| `--force` | *(sem valor)* | — | Ignora a detecção e reprocessa tudo |
| `--watch` | *(sem valor)* | — | Modo daemon: loop infinito verificando mudanças |
| `--interval N` | `int` (segundos) | `300` | Intervalo entre verificações no `--watch` |
| `--reset` | *(sem valor)* | — | Remove `.data_state.json` (próxima execução reprocessa tudo) |

### Cenários comuns

#### Execução idempotente (rodar só se houve mudança)

```bash
python auto_pipeline.py
```

Se nenhum arquivo mudou, apenas imprime "Nenhuma alteração detectada" e sai.
Seguro de colocar em cron a cada 5–10 minutos.

#### Monitoramento contínuo em primeiro plano

```bash
python auto_pipeline.py --watch --interval 60
```

Fica verificando a cada 60 segundos. Interrompa com `Ctrl+C`.

#### Forçar reexecução mesmo sem alterações

```bash
python auto_pipeline.py --force
```

Equivalente a rodar `run_pipeline.py` diretamente, mas ainda atualiza o
`.data_state.json` com os hashes atuais.

#### Limpar o estado rastreado

```bash
python auto_pipeline.py --reset
```

Útil após reorganizar os diretórios de dados ou quando o
`.data_state.json` ficou corrompido. **Não edite o JSON à mão** — use
`--reset` ou `--force`.

#### Verificar o que está sendo rastreado

```bash
python auto_pipeline.py --status
```

Lista arquivos monitorados, hash armazenado, hash atual e se há divergência.

---

## Onde encontrar os artefatos gerados

Todos os arquivos de saída ficam em `Fase02/outputs/`:

| Arquivo | Conteúdo |
|---------|----------|
| `Relatorio_SABO_R<N>.pdf` | Relatório final (versão auto-incrementada ou fixada por `--version`) |
| `data_raw.csv` | Dados consolidados da etapa 1 |
| `data_preprocessed.csv` | Dados com target `Manutencao` calculado (etapa 2) |
| `data_eda.csv` | Dataset pronto para modelagem (etapa 3) |
| `models/*.joblib` | Modelos treinados: Linear, Decision Tree, Random Forest, XGBoost |
| `best_model.joblib` | Melhor modelo selecionado na etapa 5 |
| `train_test_split.npz` | Divisão treino/teste e nomes das features |
| `evaluation_report.txt` | Métricas completas em texto (R², MSE, MAE, RMSE) |
| `equipment_stats.{csv,json}` | Estatísticas agregadas por equipamento |
| `eda_plots/*.png` | Todos os gráficos usados no PDF |
| `history/runs/run_<id>.json` | Snapshot estruturado de cada execução |
| `history/reports/report_<id>.txt` | Resumo textual de cada execução |
| `.data_state.json` | Estado de hashes do `auto_pipeline` (não editar à mão) |

### Saída típica no terminal (execução completa)

```
============================================================
PIPELINE SABO - ML Prescritivo
============================================================

[1/8] Etapa 0: Split de arquivo unificado...
  ✓ Concluído

[2/8] Etapa 1: Coleta e integração...
  ✓ Lidos N arquivos de equipamento

...

[8/8] Etapa 6: Geração de relatório...
  ✓ Previsões ML geradas para 26 equipamentos
  ✓ Relatório PDF gerado: Relatorio_SABO_R<N>.pdf

============================================================
PIPELINE CONCLUÍDO
============================================================

Modelo selecionado: XGBOOST
  R²:  0.XXXX
  MSE: XXXX.XX
  MAE: XX.XX

📊 Histórico registrado para: s06_generate_report
📄 Relatório salvo: history/reports/report_<id>.txt
✅ Execução salva: history/runs/run_<id>.json
```

## Fluxo do Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│  ETAPA 1: COLETA E INTEGRAÇÃO                                   │
│  - Carrega IJ-*.xlsx (dados por equipamento)                    │
│  - Carrega DadosProducao*.xlsx (dados consolidados)             │
│  - Converte datas para formato ISO (yyyy-mm-dd)                 │
│  → Saída: data_raw.csv                                          │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 2: PRÉ-PROCESSAMENTO E LIMPEZA                           │
│  - Remoção de duplicatas                                        │
│  - Tratamento de valores nulos                                  │
│  - Cálculo de dias até manutenção (variável target)             │
│  - Geração de variáveis acumulativas                            │
│  - Adição de features de medição/desgaste (NOVO!)               │
│  - One-Hot Encoding para categorias                             │
│  → Saída: data_preprocessed.csv                                 │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 3: ANÁLISE EXPLORATÓRIA (EDA)                            │
│  - Estatísticas descritivas                                     │
│  - Histogramas e boxplots                                       │
│  - Matriz de correlação                                         │
│  → Saída: data_eda.csv, eda_report.txt, eda_plots/              │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 3b: EDA AVANÇADO                                         │
│  - Análise temporal                                             │
│  - Matriz de urgência                                           │
│  - Consumo vs produção                                          │
│  → Saída: gráficos adicionais em eda_plots/                     │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 4: MODELAGEM E TREINAMENTO                               │
│  - Divisão: 80% treino / 20% teste                              │
│  - Algoritmos: Linear, Decision Tree, Random Forest, XGBoost    │
│  → Saída: models/*.joblib, train_test_split.npz                 │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 5: VALIDAÇÃO E AVALIAÇÃO                                 │
│  - Métricas: R², MSE, MAE, RMSE                                 │
│  - Ranking comparativo                                          │
│  - Seleção do melhor modelo                                     │
│  → Saída: best_model.joblib, evaluation_report.txt              │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 6: GERAÇÃO DE RELATÓRIO                                  │
│  - Relatório PDF no formato padrão SABO (sumário 11→17)         │
│  - Capítulo 11: gera CSV consolidado + PPTX consolidado e os    │
│    inclui como links clicáveis no PDF                           │
│  - Inclusão de gráficos e métricas                              │
│  → Saída: Relatorio_SABO_RX.pdf,                                │
│           previsao_manutencao_consolidada.csv,                  │
│           Apresentacoes_Componentes_Consolidado.pptx            │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 7: HISTÓRICO DE MANUTENÇÃO POR EQUIPAMENTO               │
│  - Cruza Dados Manut*.xlsx (todas as fotografias) com data_raw  │
│  - Janelas de manutenção por equipamento e ociosidade           │
│  → Saída: equipamentos_historico_*.csv,                         │
│           equipamentos_janelas_manutencao.csv,                  │
│           equipamentos_ociosidade.csv                           │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 8: PRESCRIÇÃO DE PRÓXIMA MANUTENÇÃO POR EQUIPAMENTO      │
│  - Combina desgaste medido + consumo de massa + ociosidade      │
│  - Calcula data_prescrita por equipamento                       │
│  → Saída: prescricao_manutencao.csv                             │
└─────────────────────────────────────────────────────────────────┘
```

## Dados de Entrada

### Arquivos Suportados

| Tipo | Padrão | Local | Descrição |
|------|--------|-------|-----------|
| Equipamento | `IJ-*.xlsx` | data/raw/ | Dados individuais por máquina |
| Consolidado | `DadosProducao*.xlsx` | data/raw/ | Dados de múltiplos equipamentos |
| Histórico | `IJ-*.2.xlsx` | data/raw/ | Dados históricos estendidos |
| **Manutenção** | `Dados Manut*.xlsx` | data/ | **Datas e medições de desgaste** |

### Colunas dos Arquivos de Produção

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| Data de Produção | Data (dd/mm/yyyy) | Data do registro |
| Cód. Ordem | Numérico | Código da ordem de produção |
| Cód. Recurso | Texto | Identificador do equipamento |
| Cód. Produto | Texto | Código do produto |
| Qtd. Produzida | Numérico | Quantidade produzida |
| Qtd. Refugada | Numérico | Quantidade refugada |
| Qtd. Retrabalhada | Numérico | Quantidade retrabalhada |
| Fator Un. | Numérico | Fator de unidade |
| Cód. Un. | Texto | Código da unidade |
| Descrição da massa (Composto) | Texto | Tipo de composto |
| Consumo de massa no item em (Kg/100pçs) | Numérico | Consumo de matéria-prima |

### Colunas do Arquivo de Manutenção

| Coluna | Descrição |
|--------|-----------|
| Equipamento | Identificador (IJ-XXX) |
| Data execução da última substituição | Data da última troca |
| Data da penúltima substituição | Data anterior |
| Dias em operação | Intervalo entre manutenções |
| Medições Cilindro (A-E, Máx, Mín) | Medidas de desgaste do cilindro |
| Medições Fuso (A-D, Máx, Mín) | Medidas de desgaste do fuso |

## Features do Modelo

### Features de Produção
- `Qtd_Produzida` - Quantidade produzida no registro
- `Qtd_Refugada` - Quantidade refugada
- `Qtd_Retrabalhada` - Quantidade retrabalhada
- `Qtd_Produzida_Acumulado` - Produção total acumulada do equipamento
- `Consumo_de_massa` - Consumo de matéria-prima

### Features de Medição de Desgaste (NOVO!)

| Feature | Descrição | Correlação com Target |
|---------|-----------|----------------------|
| `cilindro_max` | Medição máxima do cilindro | +0.03 |
| `cilindro_min` | Medição mínima do cilindro | +0.18 |
| `cilindro_variacao` | Diferença max-min do cilindro | -0.03 |
| `desgaste_cilindro` | Desvio do valor nominal (20mm) | +0.03 |
| `fuso_max` | Medição máxima do fuso | -0.18 |
| `fuso_min` | Medição mínima do fuso | -0.09 |
| `fuso_variacao` | Diferença max-min do fuso | +0.02 |
| `desgaste_fuso` | Desvio do valor nominal (20mm) | +0.09 |

### Features Derivadas

| Feature | Descrição | Correlação com Target |
|---------|-----------|----------------------|
| `taxa_desgaste_cilindro` | Desgaste do cilindro por dia | **-0.17** |
| `taxa_desgaste_fuso` | Desgaste do fuso por dia | **-0.26** |
| `indice_desgaste` | Score combinado 0-100 | **-0.12** |
| `desgaste_por_1000_pecas` | Correlação produção/desgaste | -0.04 |

> **Nota:** A feature `intervalo_manutencao` foi removida do modelo por causar **data leakage** (correlação ~1.0 com o target), o que resultava em memorização ao invés de aprendizado real.

### Variável Target

A variável `Manutencao` representa o número de dias até a próxima manutenção programada. O cálculo considera:

1. **Registros antes da última manutenção:** `dias = data_manutenção - data_produção`
2. **Registros após a última manutenção:** `dias = próxima_manutenção_prevista - data_produção`

A próxima manutenção é estimada com base no intervalo médio de manutenção do equipamento.

## Carregamento Automático de Dados de Manutenção

O sistema carrega automaticamente os dados de manutenção do arquivo `Dados Manut*.xlsx`:

```python
# Dados carregados automaticamente:
# - Data da última manutenção
# - Intervalo entre manutenções
# - Medições do cilindro (5 pontos: A, B, C, D, E)
# - Medições do fuso (4 pontos: A, B, C, D)
# - Desgaste calculado automaticamente

# Exemplo de uso:
from s02_preprocessing import load_full_maintenance_data

maint, intervalos, medicoes = load_full_maintenance_data()
print(medicoes["IJ-125"])
# {'cilindro_max': 20.03, 'cilindro_min': 20.02, 'desgaste_cilindro': 0.03, ...}
```

## Saídas Geradas

### Arquivos de Dados

| Arquivo | Descrição |
|---------|-----------|
| `data_raw.csv` | Dados consolidados de todas as fontes |
| `data_preprocessed.csv` | Dados limpos e transformados (90 colunas) |
| `data_eda.csv` | Dados prontos para modelagem |
| `train_test_split.npz` | Divisão treino/teste |
| `equipment_stats.csv` | Estatísticas agregadas por equipamento |
| `equipment_stats.json` | Estatísticas em formato JSON |
| `prescricao_manutencao.csv` | Saída do `s08`: data prescrita pelo ML por equipamento |
| `previsao_manutencao_consolidada.csv` | **NOVO! (R23)** Capítulo 11 — combina histórico × SAP × ML por equipamento |
| `Apresentacoes_Componentes_Consolidado.pptx` | **NOVO! (R23)** Capítulo 11 — merge dos 27 PPTX de componentes em um único arquivo |

### Estatísticas por Equipamento (equipment_stats.csv)

O arquivo contém as seguintes métricas para cada equipamento:

| Coluna | Descrição |
|--------|-----------|
| `equipamento` | Identificador do equipamento (IJ-XXX) |
| `total_produzido` | Total de peças produzidas |
| `media_producao_diaria` | Média de produção por registro |
| `max_producao_diaria` | Produção máxima em um registro |
| `total_registros` | Número de registros de produção |
| `total_refugado` | Total de peças refugadas |
| `total_retrabalhado` | Total de peças retrabalhadas |
| `taxa_refugo_pct` | Taxa de refugo em percentual |
| `consumo_massa_total_kg` | Consumo total de massa em Kg |
| `media_dias_manutencao` | Média de dias até manutenção |
| `min_dias_manutencao` | Mínimo de dias até manutenção |
| `max_dias_manutencao` | Máximo de dias até manutenção |
| `intervalo_manutencao_dias` | Intervalo típico entre manutenções |
| `data_ultima_manutencao` | Data da última troca de peças |
| `data_penultima_manutencao` | Data da penúltima troca |
| `observacoes_manutencao` | Observações da manutenção |
| `cilindro_max/min` | Medições do cilindro |
| `fuso_max/min` | Medições do fuso |
| `indice_desgaste_medio` | Índice de desgaste (0-100) |

### Modelos

| Arquivo | Descrição |
|---------|-----------|
| `models/model_linear.joblib` | Regressão Linear |
| `models/model_decision_tree.joblib` | Árvore de Decisão |
| `models/model_random_forest.joblib` | Random Forest |
| `models/model_xgboost.joblib` | XGBoost |
| `best_model.joblib` | Melhor modelo selecionado |

### Relatórios

| Arquivo | Descrição |
|---------|-----------|
| `eda_report.txt` | Estatísticas descritivas |
| `evaluation_report.txt` | Métricas de avaliação |
| `Relatorio_SABO_RX.pdf` | Relatório final completo |

### Capítulo 11 do PDF — Previsão de Troca de Peças (CSV consolidado)

A partir da versão **R23**, o Capítulo 11 do PDF não traz mais as tabelas 11.1–11.4. Em vez disso, o capítulo contém um texto explicativo seguido de **dois links clicáveis**:

1. **`previsao_manutencao_consolidada.csv`** — uma linha por equipamento com 9 colunas:
   - `Equipamento`
   - `Ultima_Data_Producao` (max `Data de Produção` do equipamento em `data_raw.csv`)
   - `Ultima_Manutencao` (`data_ultima_manutencao` em `equipment_stats.json`)
   - `Intervalo_Penultima_Ultima_Manutencao_dias`
   - `Data_Proxima_Manutencao_SAP` (primeira preventiva RM.195 com data ≥ hoje)
   - `Dias_Ultima_Manutencao_ate_SAP`
   - `Dias_SAP_ate_Hoje`
   - `Data_Proxima_Manutencao_ML` (do `prescricao_manutencao.csv`)
   - `Diferenca_ML_vs_SAP_dias` (positivo = ML após SAP; negativo = ML antes do SAP)
2. **`Apresentacoes_Componentes_Consolidado.pptx`** — concatenação, via `python-pptx`, de todos os `IJ-*.pptx` em `outputs/relatorios_mensais_componentes_ppt/` em um único arquivo.

**Como interpretar a diferença ML vs SAP:**

| Diferença | Significado | Ação sugerida |
|-----------|-------------|---------------|
| `< -30 dias` | ML prescreve manutenção bem antes do SAP | Antecipar a OM |
| `entre -30 e +30` | Previsões alinhadas | Seguir o agendamento SAP |
| `> +30 dias` | Equipamento em bom estado | Avaliar adiamento |

> O Capítulo 11 anterior (subitens 11.1 a 11.4 com tabelas Histórica, Prescritiva, Comparação e Fatores) foi removido do PDF para enxugar o relatório, mas as três fontes que o alimentavam (`equipment_stats.json`, preventivas RM.195 e `prescricao_manutencao.csv`) seguem disponíveis e agora aparecem juntas no CSV.

### Gráfico de Resumo por Equipamento (Figura 7)

O novo gráfico `resumo_equipamentos.png` apresenta três visualizações:

1. **Produção Total**: Barras horizontais mostrando a produção em milhares de peças
2. **Taxa de Refugo**: Percentual de refugo com linha média para comparação
3. **Dias até Manutenção**: Média de dias com linha de referência

Este gráfico permite identificar rapidamente:
- Equipamentos com maior volume de produção
- Equipamentos com problemas de qualidade (alto refugo)
- Equipamentos que precisam de atenção urgente (poucos dias até manutenção)

### Gráficos (eda_plots/)

| Gráfico | Descrição |
|---------|-----------|
| `correlation_matrix_full.png` | Matriz de correlação completa (20 variáveis) |
| `heatmap_correlacao.png` | Heatmap de correlação |
| `consumo_vs_producao.png` | Análise de consumo vs produção |
| `analise_temporal.png` | Evolução temporal |
| `matriz_urgencia.png` | Priorização de manutenção |
| `scatter_plots_features.png` | **Scatter plots coloridos por equipamento** (26 equipamentos) |
| `resumo_equipamentos.png` | **NOVO!** Resumo visual por equipamento (produção, refugo, manutenção) |
| `dispersao_target.png` | Dispersão vs target |
| `histogramas.png` | Distribuição das variáveis |
| `boxplots.png` | Outliers e distribuição |

## Modelos e Métricas

### Algoritmos Implementados

| Modelo | Descrição | Caso de Uso |
|--------|-----------|-------------|
| Regressão Linear | Baseline linear | Referência |
| Decision Tree | Árvore de decisão | Interpretabilidade |
| Random Forest | Ensemble de árvores | Robustez |
| XGBoost | Gradient boosting | Performance |

### Métricas de Avaliação

| Métrica | Descrição | Interpretação |
|---------|-----------|---------------|
| R² | Coeficiente de determinação | Quanto mais próximo de 1, melhor |
| MSE | Erro quadrático médio | Quanto menor, melhor |
| MAE | Erro absoluto médio | Erro médio em dias |
| RMSE | Raiz do MSE | Erro em mesma escala do target |

### Resultados Atuais (R23 — 2026-04-28)

```
Rank   Modelo           R²       MSE        MAE
─────────────────────────────────────────────────
1      XGBoost          0.8369   4206.15    39.43   ★ MELHOR
```

> Os números acima são da execução `R23`, com o dataset depurado após as correções de
> ingestão (dupla contagem de `DadosProducao*.xlsx`, sobrescrita silenciosa do baseline
> e parsing ISO/dayfirst). Para histórico completo de cada execução, consulte
> `outputs/history/runs/run_<id>.json`.

> **Correções aplicadas (v3.2.0):**
> - Removida feature `intervalo_manutencao` por data leakage
> - Random Forest e Decision Tree com hiperparâmetros ajustados para evitar overfitting
> - Modelos agora generalizam melhor (não memorizam)

## Alterações Recentes

### R23 (2026-04-28) — Capítulo 11 consolidado em CSV + reordenação do sumário

#### Item 7.4 — Histórico de Troca de Peças por Equipamento
- **Removida** a frase "Fonte: Dados Manut - 27 Equip - 2025.xlsx" da descrição do bloco.
- Tabela agora tem colunas: `#` (índice), `Equipamento`, `Penúltima Troca`, `Última Troca`, `Intervalo (dias)`, `Média Hist. (dias)`, `Mín (dias)`, `Máx (dias)`. As três últimas colunas mostram, por equipamento, a estatística do intervalo entre manutenções (vindas de `equipment_stats.json` → `media/min/max_dias_manutencao`).
- A penúltima troca é lida de `data_penultima_manutencao` no mesmo JSON.

#### Item 7.5 — Features Utilizadas no Modelo Final
- "Features Numéricas Originais" e "Features Acumuladas" agora aparecem como **lista com bullets**, uma feature por linha (sem truncamento). Apenas o bloco de "Features de Encoding" mantém apresentação compacta para evitar tabela enorme.

#### Item 11 — Previsão de Troca de Peças (reformulado)
- **Removidos** os subitens 11.1 (Histórica), 11.2 (Prescritiva), 11.3 (Comparação) e 11.4 (Fatores).
- Em vez disso, o Capítulo 11 agora gera dois artefatos e os referencia como **links clicáveis** no PDF:
  - `outputs/previsao_manutencao_consolidada.csv` — uma linha por equipamento, 9 colunas (`Equipamento`, `Ultima_Data_Producao`, `Ultima_Manutencao`, `Intervalo_Penultima_Ultima_Manutencao_dias`, `Data_Proxima_Manutencao_SAP`, `Dias_Ultima_Manutencao_ate_SAP`, `Dias_SAP_ate_Hoje`, `Data_Proxima_Manutencao_ML`, `Diferenca_ML_vs_SAP_dias`).
  - `outputs/Apresentacoes_Componentes_Consolidado.pptx` — concatena todos os `IJ-*.pptx` da pasta `outputs/relatorios_mensais_componentes_ppt/` em um único arquivo, via `python-pptx` (clonagem de `<p:sp>`, relacionamentos de mídia preservados).

#### Reordenação do sumário
A partir do **R23** o sumário do PDF segue esta ordem:

| Ordem | Capítulo |
|-------|----------|
| 11 | Previsão de Troca de Peças (CSV + PPTX consolidados) |
| **12** | **Análise Mensal por Equipamento** *(antes era o 16)* |
| 13 | Considerações *(antes era o 12)* |
| 14 | Próximos Passos *(antes era o 13)* |
| 15 | Recomendações *(antes era o 14)* |
| 16 | Considerações Finais *(antes era o 15)* |
| 17 | Consumo de Massa por Equipamento e Composto |

#### Novas funções em `s06_generate_report.py`
- `_load_sap_scheduled_dates()` — lê `Histórico Geral Preventivas RM.195*.xlsx` e devolve `{equip: [datas ordenadas]}`.
- `_load_ml_predictions()` — lê `prescricao_manutencao.csv` e devolve `{equip: data_prescrita}`.
- `_load_last_production_dates()` — lê `data_raw.csv` e devolve `{equip: max(Data de Produção)}`.
- `generate_previsao_manutencao_csv(equip_data)` — escreve o CSV consolidado.
- `merge_componentes_pptx()` — consolida os PPTX por equipamento via `python-pptx` (cópia profunda dos elementos do slide e relacionamentos de imagens/charts/mídia).

#### Métricas do modelo nesta versão (R23)
- XGBoost: R² = 0,8369 · MSE = 4206,15 · MAE = 39,43

---

### v3.3.0 - Previsão Prescritiva com ML

#### Novas Tabelas de Previsão no Relatório (Seção 11)

O relatório agora inclui **3 tabelas de previsão**:

| Tabela | Descrição | Cor |
|--------|-----------|-----|
| **11.1 Histórica** | Baseada no intervalo entre trocas | Cinza |
| **11.2 Prescritiva (ML)** | Baseada no modelo XGBoost | Laranja |
| **11.3 Comparação** | Diferença entre métodos | Azul |

#### Previsão com Modelo ML
- Nova função `predict_maintenance_with_ml()` em `s06_generate_report.py`
- O modelo considera o **estado atual** de cada equipamento:
  - Produção acumulada desde última troca
  - Índice de desgaste (0-100)
  - Medições de cilindro e fuso
  - Taxa de refugo e retrabalho
- Previsões baseadas em 23 features

#### Recomendações Automáticas
A tabela de comparação inclui recomendações:
- **Antecipar**: ML indica necessidade antes do histórico (diferença < -30 dias)
- **Pode adiar**: Equipamento em bom estado (diferença > +30 dias)
- **Conforme**: Previsões similares (±30 dias)

#### Status Colorido por Urgência
- 🔴 **ATRASADO**: Troca já deveria ter sido realizada
- 🟠 **URGENTE**: Menos de 30 dias
- 🟡 **ATENÇÃO**: Entre 30 e 90 dias
- 🟢 **OK**: Mais de 90 dias

---

### v3.2.0 - Correção de Data Leakage

#### Correções Críticas
- **Removida feature `intervalo_manutencao`** do treinamento (causava R² = 1.0 por memorização)
- **Hiperparâmetros ajustados** para evitar overfitting:
  - Random Forest: `max_depth=8`, `min_samples_leaf=10`
  - Decision Tree: `max_depth=6`, `min_samples_leaf=10`
- **Datas dinâmicas** no relatório (antes eram hardcoded)

---

### v3.1.0 - Estatísticas por Equipamento

#### Nova Pasta de Manutenção (`data/manutencao/`)
- Nova estrutura de pastas para organizar arquivos de manutenção
- Suporte a múltiplos arquivos de manutenção (XLSX e CSV)
- Carregamento automático da pasta `data/manutencao/`
- Fallback para `data/` mantido para compatibilidade

#### Estatísticas por Equipamento
- **Novo arquivo `equipment_stats.csv`** com métricas agregadas por equipamento
- **Novo arquivo `equipment_stats.json`** para integração com relatório
- Métricas incluídas: produção total, refugo, retrabalho, consumo de massa, dias até manutenção
- Datas de manutenção (última e penúltima) e observações

---

### v3.0.0

#### Automação (auto_pipeline.py)
- **Detecção automática de alterações** nos arquivos de dados
- Monitoramento usando hash MD5 para detectar modificações
- Modo watch para monitoramento contínuo
- Re-execução automática do pipeline quando há mudanças

#### Medições de Desgaste
- **Carregamento automático** das medições do arquivo de manutenção
- **13 novas features** de medição e desgaste
- Cálculo de taxa de desgaste por dia e por peças produzidas
- Índice de desgaste combinado (cilindro + fuso)

#### Carregamento Dinâmico
- Dados de manutenção lidos automaticamente do arquivo XLSX
- Não há mais necessidade de atualizar valores hardcoded
- Quando o arquivo é atualizado, o pipeline reflete automaticamente

---

### v2.0.0
- Suporte a arquivos `DadosProducao*.xlsx` para dados consolidados
- Cálculo de manutenção para registros após última manutenção conhecida
- Intervalo de manutenção configurável por equipamento

### Correções
- **Parsing de datas:** Corrigido para formato brasileiro (dd/mm/yyyy)
- **Variável target:** Registros pós-manutenção agora incluídos no modelo

## Troubleshooting

### Erro: "Arquivo não encontrado"
Execute as etapas na ordem correta:
```bash
python run_pipeline.py  # Executa todas as etapas
```

### Erro: "ModuleNotFoundError"
Confirme que está no venv canônico (`Fase02/.venv`) e instale:
```bash
cd caminho/para/Fase02
source .venv/bin/activate
pip install -r requirements.txt
```

### Erro: "cannot execute binary file: Exec format error" ao rodar `python`
Um venv herdou binários de outra arquitetura/máquina. Apague o venv quebrado
e recrie em `Fase02/.venv` (ver "Ambiente Virtual (venv)"):
```bash
rm -rf caminho/para/venv_quebrado
cd caminho/para/Fase02
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Datas incorretas no relatório
Verifique se os arquivos Excel usam formato dd/mm/yyyy. O sistema converte automaticamente.

### Gráficos não aparecem no PDF
Verifique se a etapa 3 (EDA) foi executada:
```bash
ls outputs/eda_plots/
```

### Poucos registros após pré-processamento
Verifique as datas de manutenção no arquivo `Dados Manut*.xlsx`. Registros muito antigos ou futuros podem ser filtrados.

### Medições não aparecem no modelo
Verifique se o arquivo de manutenção está na pasta `data/` (não em `data/raw/`):
```bash
ls data/*.xlsx
# Deve mostrar: Dados Manut - 27 Equip - 2025.xlsx
```

## Extensibilidade

### Adicionar Novo Equipamento

1. Adicionar dados de produção em `data/raw/IJ-XXX.xlsx`
2. Adicionar linha no arquivo `data/manutencao/Dados Manut*.xlsx` com:
   - Nome do equipamento
   - Data da última manutenção
   - Data da penúltima manutenção
   - Dias em operação
   - Medições do cilindro (A, B, C, D, E, Máx, Mín)
   - Medições do fuso (A, B, C, D, Máx, Mín)
3. Re-executar pipeline:
```bash
python auto_pipeline.py --force
```

### Atualizar Dados de Manutenção

1. Editar o arquivo em `data/manutencao/Dados Manut - 27 Equip - 2025.xlsx`
2. Atualizar:
   - Data da última substituição
   - Data da penúltima substituição
   - Dias em operação
   - Medições atualizadas do cilindro e fuso
3. O sistema detectará automaticamente a alteração:
```bash
python auto_pipeline.py  # Detecta e reprocessa
```

### Estrutura do Arquivo de Manutenção

O arquivo XLSX deve conter as seguintes colunas:

| Coluna | Descrição |
|--------|-----------|
| B | Equipamento (IJ-XXX) |
| C | Data execução da última substituição |
| D | Data da penúltima substituição |
| E | Dias em operação |
| F | Observações |
| G-L | Medições Cilindro (A, B, C, D, E, Máx, Mín) |
| M-R | Medições Fuso (A, B, C, D, Máx, Mín) |

### Adicionar Novo Modelo

1. Editar `s04_modeling.py`
2. Adicionar modelo na função `train_models()`
3. O modelo será automaticamente avaliado

### Personalizar Relatório PDF

Editar `s06_generate_report.py`, classe `SABOReportGenerator`.

## Histórico de Execuções

O sistema mantém histórico em `outputs/history/`:
- `runs/run_YYYYMMDD_HHMMSS.json` - Dados estruturados
- `reports/report_YYYYMMDD_HHMMSS.txt` - Relatório textual

```bash
python run_pipeline.py --history   # Ver histórico
python run_pipeline.py --compare 5 # Comparar últimas 5 execuções
```

## Licença

Projeto interno SABO - Uso restrito.

## Contato

Para dúvidas ou sugestões, consulte a equipe de Engenharia de Dados.
