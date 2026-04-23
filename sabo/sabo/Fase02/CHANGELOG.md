# Changelog - SABO Pipeline

Todas as alterações notáveis deste projeto serão documentadas neste arquivo.

## [3.3.0] - 2026-02-06

### Adicionado

#### Previsão Prescritiva com Modelo ML (s06_generate_report.py)
- **NOVO**: Tabela 11.1 - Previsão Histórica (baseada em intervalos entre trocas)
- **NOVO**: Tabela 11.2 - Previsão Prescritiva (baseada no modelo ML treinado)
- **NOVO**: Tabela 11.3 - Comparação entre métodos (Histórico vs ML)
- Nova função `predict_maintenance_with_ml()` para previsões usando o modelo
- O modelo considera estado atual: produção acumulada, desgaste, refugo, medições
- Recomendações automáticas: "Antecipar", "Pode adiar", "Conforme"

#### Melhorias na Seção 11 do Relatório
- Três tabelas distintas para melhor visualização
- Status colorido por urgência (ATRASADO, URGENTE, ATENÇÃO, OK)
- Seção 11.4 com fatores considerados pelo modelo ML
- Explicação clara da diferença entre previsão histórica e prescritiva

#### Gráficos Melhorados (s03b_advanced_eda.py)
- **Figura 6 (scatter_plots_features.png)**: Agora colorido por equipamento
  - Cada equipamento tem cor única (26 cores distintas)
  - Legenda identificando todos os equipamentos
- **NOVO: Figura 7 (resumo_equipamentos.png)**: Resumo visual por equipamento
  - Produção Total por equipamento (mil peças)
  - Taxa de Refugo por equipamento (%) com linha média
  - Dias até Manutenção por equipamento com linha média
- Nova função `get_equipment_from_onehot()` para identificar equipamentos
- Nova função `generate_equipment_summary_plot()` para gráfico de resumo
- Relatório agora inclui 8 gráficos (antes eram 6)

### Corrigido
- Removida página vazia no final do relatório PDF

### Impacto das Melhorias
| Antes | Depois |
|-------|--------|
| Apenas previsão histórica | Previsão histórica + ML + comparação |
| Tabela única simples | 3 tabelas informativas |
| Sem recomendações | Recomendações automáticas |
| Status genérico | Status com cores por urgência |
| Scatter plots cor única | Scatter plots coloridos por equipamento |
| 6 gráficos no relatório | 8 gráficos no relatório |

---

## [3.2.0] - 2026-02-06

### Corrigido

#### Tabela de Previsão de Manutenção (s06_generate_report.py)
- **CRÍTICO**: Removidos valores hardcoded de datas de manutenção
- Agora carrega dados dinamicamente de `equipment_stats.json`
- Datas de previsão refletem dados reais do Excel
- Nova função `load_equipment_stats()` para carregamento dinâmico

#### Data Leakage Corrigido (s04_modeling.py)
- **CRÍTICO**: Removida feature `intervalo_manutencao` do treinamento
- Esta feature causava correlação ~1.0 com target (overfitting)
- R² de Random Forest deixará de ser 1.0000 (era memorização)
- Modelo agora aprende padrões reais, não memoriza tabela

#### Hiperparâmetros de Modelos Ajustados (s04_modeling.py)
- **Random Forest**: `max_depth` reduzido de 15 para 8
- **Random Forest**: `min_samples_leaf` aumentado de 2 para 10
- **Decision Tree**: `max_depth` reduzido de 10 para 6
- **Decision Tree**: `min_samples_leaf` aumentado de 2 para 10
- Modelos agora generalizam melhor, evitando overfitting

### Impacto das Correções
| Antes | Depois |
|-------|--------|
| Datas hardcoded no código | Datas carregadas do JSON |
| R² = 1.0000 (memorização) | R² realista (aprendizado) |
| Previsões nunca mudavam | Previsões refletem dados atuais |

---

## [3.1.0] - 2026-02-06

### Adicionado

#### Nova Pasta de Manutenção (data/manutencao/)
- Nova estrutura de pastas para organizar arquivos de manutenção
- Suporte a múltiplos arquivos de manutenção (XLSX e CSV)
- Carregamento automático da pasta `data/manutencao/`
- Fallback para `data/` para compatibilidade

#### Estatísticas por Equipamento
- Novo arquivo `equipment_stats.csv` com métricas agregadas
- Novo arquivo `equipment_stats.json` para integração com relatório
- Métricas incluídas:
  - Total produzido, média de produção diária
  - Total refugado, retrabalhado, taxa de refugo
  - Consumo de massa total e médio
  - Dias até manutenção (média, mín, máx)
  - Data da última e penúltima manutenção
  - Medições de cilindro e fuso
  - Índice de desgaste médio

#### Data da Penúltima Manutenção
- Agora o sistema considera ambas as datas de manutenção:
  - Data da última substituição
  - Data da penúltima substituição
- Observações do arquivo de manutenção também são carregadas

### Modificado

#### s02_preprocessing.py
- Pipeline agora tem 8 etapas (adicionada etapa de estatísticas)
- Nova função `calculate_equipment_statistics()`
- Nova função `export_equipment_statistics()`
- Nova função `get_equipment_statistics()`
- Função `load_full_maintenance_data()` atualizada para nova pasta

#### auto_pipeline.py
- Monitoramento da nova pasta `data/manutencao/`
- Detecção de arquivos CSV e XLSX de manutenção

#### config/paths.py
- Nova constante `DATA_MANUTENCAO_DIR`
- Nova função `get_maintenance_history_file()`
- Nova função `get_all_maintenance_files()`
- Atualizada função `get_maintenance_file()` para nova pasta

---

## [3.0.0] - 2026-02-06

### Adicionado

#### Automação do Pipeline (auto_pipeline.py)
- Novo script `auto_pipeline.py` para automação completa
- Detecção automática de alterações usando hash MD5
- Modo watch para monitoramento contínuo
- Comandos: `--status`, `--force`, `--watch`, `--interval`, `--reset`
- Estado persistido em `.data_state.json`

#### Features de Medição de Desgaste
- 13 novas features extraídas do arquivo de manutenção:
  - `cilindro_max`, `cilindro_min`, `cilindro_variacao`
  - `fuso_max`, `fuso_min`, `fuso_variacao`
  - `desgaste_cilindro`, `desgaste_fuso`
  - `intervalo_manutencao`
  - `taxa_desgaste_cilindro`, `taxa_desgaste_fuso`
  - `indice_desgaste` (score combinado 0-100)
  - `desgaste_por_1000_pecas`

#### Carregamento Dinâmico de Dados
- Função `load_full_maintenance_data()` em s02_preprocessing.py
- Leitura automática das medições do arquivo `Dados Manut*.xlsx`
- Cache de dados para evitar leituras repetidas
- Fallback para valores padrão se arquivo não encontrado

### Modificado

#### s02_preprocessing.py
- Adicionada função `add_measurement_features()`
- Atualizada função `calculate_maintenance_days()` para usar dados dinâmicos
- Pipeline agora tem 7 etapas (antes eram 6)
- Removida dependência de valores hardcoded de manutenção

#### config/paths.py
- Adicionada função `get_maintenance_file()`
- Adicionada função `get_all_data_files()`
- Adicionada constante `DATA_STATE_FILE`

#### README.md
- Documentação completa das novas funcionalidades
- Seção de Features do Modelo atualizada
- Instruções de uso do auto_pipeline.py
- Troubleshooting para medições

### Correlações Identificadas
| Feature | Correlação com Target |
|---------|----------------------|
| intervalo_manutencao | +0.92 |
| taxa_desgaste_fuso | -0.26 |
| taxa_desgaste_cilindro | -0.17 |
| indice_desgaste | -0.12 |

---

## [2.0.0] - 2026-01-05

### Adicionado
- Suporte a arquivos `DadosProducao*.xlsx`
- Cálculo de manutenção para registros pós-manutenção
- Intervalo configurável por equipamento
- Sistema de histórico de execuções

### Corrigido
- Parsing de datas para formato brasileiro (dd/mm/yyyy)
- Variável target incluindo registros pós-manutenção

---

## [1.0.0] - 2025-12-09

### Adicionado
- Pipeline completo de 6 etapas
- Modelos: Linear, Decision Tree, Random Forest, XGBoost
- Geração de relatório PDF
- Gráficos de EDA
- Sistema de versionamento de relatórios
