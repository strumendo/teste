# Relatório de Alterações - SABO Pipeline
## De R12 (05/01/2026) para R26 (06/02/2026)

---

## Resumo Executivo

| Métrica | R12 | R26 |
|---------|-----|-----|
| **Data de Geração** | 05/01/2026 | 06/02/2026 |
| **Versão do Sistema** | 2.0.0 | 3.3.0 |
| **Tamanho do PDF** | 1.157 KB | 1.575 KB (+36%) |
| **Total de Páginas** | ~28 | 34 |
| **Gráficos no Relatório** | 6 | 8 |
| **MSE do Modelo** | 2155.67 | 49.62 |

---

``````## Alterações por Versão

### Versão 3.3.0 (06/02/2026) - R22 a R26

#### Previsão Prescritiva com Modelo ML
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

#### Correção de Bug
- Removida página vazia no final do relatório PDF (página 28)

---

### Versão 3.2.0 (06/02/2026) - R15 a R21

#### Correção Crítica: Tabela de Previsão de Manutenção
- **CRÍTICO**: Removidos valores hardcoded de datas de manutenção
- Agora carrega dados dinamicamente de `equipment_stats.json`
- Datas de previsão refletem dados reais do Excel
- Nova função `load_equipment_stats()` para carregamento dinâmico

#### Correção de Data Leakage (s04_modeling.py)
- **CRÍTICO**: Removida feature `intervalo_manutencao` do treinamento
- Esta feature causava correlação ~1.0 com target (overfitting)
- R² de Random Forest deixou de ser 1.0000 (era memorização)
- Modelo agora aprende padrões reais, não memoriza tabela

#### Hiperparâmetros de Modelos Ajustados
- **Random Forest**: `max_depth` reduzido de 15 para 8
- **Random Forest**: `min_samples_leaf` aumentado de 2 para 10
- **Decision Tree**: `max_depth` reduzido de 10 para 6
- **Decision Tree**: `min_samples_leaf` aumentado de 2 para 10
- Modelos agora generalizam melhor, evitando overfitting

---

### Versão 3.1.0 (06/02/2026) - R13 a R14

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
- O sistema agora considera ambas as datas de manutenção
- Observações do arquivo de manutenção também são carregadas

---

### Versão 3.0.0 (05-06/02/2026)

#### Automação do Pipeline (auto_pipeline.py)
- Novo script `auto_pipeline.py` para automação completa
- Detecção automática de alterações usando hash MD5
- Modo watch para monitoramento contínuo
- Comandos: `--status`, `--force`, `--watch`, `--interval`, `--reset`
- Estado persistido em `.data_state.json`

#### Features de Medição de Desgaste
13 novas features extraídas do arquivo de manutenção:
| Feature | Descrição |
|---------|-----------|
| `cilindro_max` | Medição máxima do cilindro |
| `cilindro_min` | Medição mínima do cilindro |
| `cilindro_variacao` | Variação do cilindro |
| `fuso_max` | Medição máxima do fuso |
| `fuso_min` | Medição mínima do fuso |
| `fuso_variacao` | Variação do fuso |
| `desgaste_cilindro` | Desgaste calculado do cilindro |
| `desgaste_fuso` | Desgaste calculado do fuso |
| `intervalo_manutencao` | Dias entre manutenções |
| `taxa_desgaste_cilindro` | Taxa de desgaste por dia |
| `taxa_desgaste_fuso` | Taxa de desgaste por dia |
| `indice_desgaste` | Score combinado 0-100 |
| `desgaste_por_1000_pecas` | Desgaste relativo à produção |

#### Carregamento Dinâmico de Dados
- Função `load_full_maintenance_data()` em s02_preprocessing.py
- Leitura automática das medições do arquivo `Dados Manut*.xlsx`
- Cache de dados para evitar leituras repetidas
- Fallback para valores padrão se arquivo não encontrado``````

---

## Comparação Visual de Funcionalidades

| Funcionalidade | R12 (v2.0) | R26 (v3.3) |
|----------------|------------|------------|
| Previsão de manutenção | Apenas histórica | Histórica + ML + Comparação |
| Tabelas de previsão | 1 tabela simples | 3 tabelas informativas |
| Recomendações automáticas | Não | Sim ("Antecipar", "Pode adiar") |
| Status de urgência | Genérico | Colorido (ATRASADO, URGENTE, etc.) |
| Scatter plots | Cor única | Coloridos por equipamento (26 cores) |
| Gráficos no relatório | 6 | 8 |
| Features do modelo | ~15 | ~28 (incluindo medições) |
| Carregamento de dados | Manual | Automático com detecção de alterações |
| Estatísticas por equipamento | Não | Sim (CSV + JSON) |
| Modo de monitoramento | Não | Sim (watch mode) |

---

## Evolução do Desempenho do Modelo

| Data | Relatório | MSE | Observação |
|------|-----------|-----|------------|
| 05/01/2026 | R12 | 2155.67 | Versão 2.0.0 |
| 05/02/2026 | R13 | 2220.53 | Início v3.0.0 |
| 06/02/2026 | R15 | 0.19 | Correção data leakage (MSE artificial) |
| 06/02/2026 | R17 | 49.62 | Ajuste hiperparâmetros |
| 06/02/2026 | R26 | 49.62 | Versão 3.3.0 final |

**Nota**: O MSE de 0.19 em R15 era resultado de data leakage (feature `intervalo_manutencao` causando overfitting). Após correção, o MSE de 49.62 representa o desempenho real do modelo.

---

## Arquivos Novos/Modificados

### Scripts Novos
- `scripts/auto_pipeline.py` - Automação com detecção de alterações
- `scripts/s03b_advanced_eda.py` - EDA avançada com gráficos melhorados

### Scripts Modificados
- `scripts/s02_preprocessing.py` - 8 etapas (antes 6), novas funções de estatísticas
- `scripts/s04_modeling.py` - Correção data leakage, hiperparâmetros ajustados
- `scripts/s06_generate_report.py` - 3 tabelas de previsão, gráficos coloridos, 34 páginas

### Configuração
- `config/paths.py` - Novas constantes e funções para pasta de manutenção

### Dados de Saída
- `outputs/equipment_stats.csv` - Estatísticas agregadas por equipamento
- `outputs/equipment_stats.json` - Dados para integração com relatório
- `outputs/.data_state.json` - Estado para detecção de alterações

### Gráficos Novos
- `outputs/graphs/scatter_plots_features.png` - Reformulado com cores por equipamento
- `outputs/graphs/resumo_equipamentos.png` - NOVO: resumo visual por equipamento

---

## Histórico de Relatórios

| Relatório | Data | Tamanho | Versão |
|-----------|------|---------|--------|
| R12 | 05/01/2026 16:59 | 1.157 KB | 2.0.0 |
| R13 | 05/02/2026 12:18 | 1.003 KB | 3.0.0 |
| R14 | 06/02/2026 11:19 | 1.003 KB | 3.1.0 |
| R15 | 06/02/2026 11:37 | 1.167 KB | 3.2.0 |
| R16 | 06/02/2026 13:45 | 1.167 KB | 3.2.0 |
| R17 | 06/02/2026 14:04 | 1.167 KB | 3.2.0 |
| R18 | 06/02/2026 14:18 | 1.170 KB | 3.2.0 |
| R19-R21 | 06/02/2026 14:19-14:20 | 1.170-1.172 KB | 3.2.0 |
| R22 | 06/02/2026 14:38 | 1.577 KB | 3.3.0 |
| R23-R24 | 06/02/2026 14:54-14:56 | 1.576 KB | 3.3.0 |
| R25-R26 | 06/02/2026 15:04-15:23 | 1.575 KB | 3.3.0 |

---

## Conclusão

Entre R12 e R26, o sistema SABO passou por uma evolução significativa:

1. **Qualidade das Previsões**: De previsão puramente histórica para previsão prescritiva com ML
2. **Correções Críticas**: Eliminação de data leakage e valores hardcoded
3. **Automação**: Pipeline agora detecta alterações automaticamente
4. **Visualização**: 8 gráficos com cores por equipamento vs 6 gráficos monocromáticos
5. **Dados**: 28 features no modelo vs ~15 anteriormente
6. **Relatório**: 34 páginas com 3 tabelas de previsão vs ~28 páginas com 1 tabela

O sistema agora oferece recomendações acionáveis ("Antecipar", "Pode adiar") baseadas em aprendizado de máquina, permitindo decisões mais informadas sobre manutenção preditiva.

---

*Gerado em: 06/02/2026*
*Versão atual: 3.3.0*
