# SABO ML Pipeline - Scripts Atuais (v2.0)

Pipeline de Machine Learning para manutenção preditiva de equipamentos industriais.

## Estrutura dos Scripts

| Script | Descrição | Entrada | Saída |
|--------|-----------|---------|-------|
| `s01_gen_dataset.py` | Gera dataset sintético para testes | - | `dados_manutencao.csv` |
| `s02_regression_linear.py` | Modelos de regressão linear (Linear, Ridge, Lasso) | CSV | Métricas, gráficos |
| `s03_regression_trees.py` | Modelos baseados em árvores (Decision Tree, RF, XGBoost) | CSV | Métricas comparativas |
| `s04_regression_svr.py` | Support Vector Regression com diferentes kernels | CSV | Métricas por kernel |
| `s05_classification.py` | Classificação de produtos com GridSearch | CSV | Modelo otimizado |
| `s06_maintenance_basic.py` | Predição de manutenção básica | CSV | Comparativo de modelos |
| `s07_maintenance_decision_tree.py` | Decision Tree para manutenção | CSV | Modelo treinado |
| `s08_maintenance_random_forest.py` | Random Forest + GridSearch + Confusion Matrix | CSV | Modelo otimizado |
| `s09_maintenance_ensemble.py` | Comparativo: SVM, RF, Gradient Boosting | CSV | Ranking de modelos |
| `s10_maintenance_xgboost.py` | XGBoost com GridSearch | CSV | Modelo otimizado |
| `s11_maintenance_xgboost_bayes.py` | XGBoost com RandomizedSearchCV | CSV | Hiperparâmetros ótimos |
| `s12_gen_real_data.py` | Processa dados reais por equipamento | CSVs brutos | `mnt-oficial-*.csv` |
| `s13_prediction_real_data.py` | Predição usando dados reais consolidados | `mnt-oficial-*.csv` | Métricas finais |

---

## Descrição Detalhada

### s01_gen_dataset.py - Geração de Dataset

**O que faz:**
- Gera dataset sintético com dados simulados de produção
- Cria variáveis como quantidade produzida, refugada, retrabalhada
- Calcula "Tempo Restante para Manutenção" como target

**Quando usar:**
- Para testes iniciais do pipeline
- Quando dados reais não estão disponíveis
- Para validar funcionamento dos modelos

---

### s02_regression_linear.py - Regressão Linear

**O que faz:**
- Treina Linear Regression, Ridge e Lasso
- Compara coeficientes e regularização
- Avalia MSE e R² de cada modelo

**Quando usar:**
- Para baseline de performance
- Quando se espera relações lineares
- Para entender importância das features

---

### s03_regression_trees.py - Modelos de Árvores

**O que faz:**
- Treina Decision Tree, Random Forest e XGBoost
- Usa GridSearchCV para otimização
- Compara performance entre modelos

**Quando usar:**
- Para dados não-lineares
- Quando features têm interações complexas
- Para obter importância das variáveis

---

### s04_regression_svr.py - Support Vector Regression

**O que faz:**
- Treina SVR com kernels: linear, poly, rbf, sigmoid
- Compara performance de cada kernel
- Avalia trade-off entre complexidade e precisão

**Quando usar:**
- Para datasets menores/médios
- Quando se quer modelagem não-paramétrica
- Para comparar diferentes tipos de relações

---

### s05_classification.py - Classificação de Produtos

**O que faz:**
- Classifica produtos usando Random Forest
- Aplica GridSearchCV para otimização
- Gera relatório de classificação (precision, recall, F1)

**Quando usar:**
- Para categorização de itens
- Quando o target é categórico
- Para análise de qualidade por tipo

---

### s06_maintenance_basic.py - Manutenção Básica

**O que faz:**
- Treina múltiplos modelos para predição de manutenção
- Compara Linear Regression, Ridge, Lasso, Decision Tree, RF, XGBoost
- Identifica melhor modelo por MSE

**Quando usar:**
- Para análise exploratória inicial
- Para decidir qual modelo aprofundar
- Como ponto de partida do pipeline

---

### s07_maintenance_decision_tree.py - Decision Tree

**O que faz:**
- Treina Decision Tree Regressor
- Visualiza estrutura da árvore
- Extrai regras de decisão

**Quando usar:**
- Quando interpretabilidade é importante
- Para gerar regras de negócio
- Para análise visual do modelo

---

### s08_maintenance_random_forest.py - Random Forest Otimizado

**O que faz:**
- Treina Random Forest com GridSearchCV
- Gera matriz de confusão discretizada (Curto/Médio/Longo)
- Exibe exemplos de predições

**Quando usar:**
- Para produção com alta precisão
- Quando overfitting é preocupação
- Para obter estimativa de incerteza

---

### s09_maintenance_ensemble.py - Comparativo Ensemble

**O que faz:**
- Compara SVM, Random Forest e Gradient Boosting
- Usa GridSearchCV em todos
- Gera ranking por MSE e R²

**Quando usar:**
- Para escolher melhor modelo ensemble
- Quando se quer análise comparativa completa
- Para justificar escolha de arquitetura

---

### s10_maintenance_xgboost.py - XGBoost com GridSearch

**O que faz:**
- Treina XGBoost com otimização de hiperparâmetros
- Compara com SVM e Random Forest
- Gera matrizes de confusão

**Quando usar:**
- Para datasets tabulares médios/grandes
- Quando XGBoost é candidato principal
- Para alta performance de predição

---

### s11_maintenance_xgboost_bayes.py - XGBoost Otimizado

**O que faz:**
- Usa RandomizedSearchCV (alternativa a BayesSearchCV)
- Explora espaço de busca mais amplo
- Otimiza subsample, colsample_bytree

**Quando usar:**
- Quando GridSearch é muito lento
- Para espaços de busca maiores
- Para otimização mais eficiente

---

### s12_gen_real_data.py - Dados Reais

**O que faz:**
- Processa CSVs por equipamento (IJ-044, IJ-046, etc.)
- Calcula dias até manutenção
- Gera arquivos com acumulados totais

**Equipamentos suportados:**
- IJ-044 até IJ-164 (27 equipamentos)

**Quando usar:**
- Quando dados reais estão disponíveis
- Para preparar dados históricos
- Para consolidar múltiplos equipamentos

---

### s13_prediction_real_data.py - Predição com Dados Reais

**O que faz:**
- Carrega dados processados de múltiplos equipamentos
- Aplica one-hot encoding automático
- Treina e compara 4 modelos
- Gera ranking e feature importance

**Quando usar:**
- Para modelos de produção
- Para validar pipeline com dados reais
- Para predição final de manutenção

---

## Fluxo Recomendado

```
┌─────────────────────────────────────────────────────────────┐
│                    FLUXO DE EXECUÇÃO                        │
└─────────────────────────────────────────────────────────────┘

1. PREPARAÇÃO DE DADOS
   ├── Dados Sintéticos: s01_gen_dataset.py
   └── Dados Reais: s12_gen_real_data.py

2. ANÁLISE EXPLORATÓRIA
   ├── s02_regression_linear.py (baseline)
   ├── s03_regression_trees.py (não-linear)
   └── s04_regression_svr.py (SVR)

3. MODELOS DE MANUTENÇÃO
   ├── s06_maintenance_basic.py (comparativo inicial)
   ├── s07_maintenance_decision_tree.py (interpretável)
   ├── s08_maintenance_random_forest.py (robusto)
   └── s09_maintenance_ensemble.py (ensemble)

4. OTIMIZAÇÃO
   ├── s10_maintenance_xgboost.py (GridSearch)
   └── s11_maintenance_xgboost_bayes.py (RandomizedSearch)

5. PRODUÇÃO
   └── s13_prediction_real_data.py (modelo final)
```

---

## Execução

### Script Individual
```bash
cd sabo/scripts/current
python s01_gen_dataset.py
```

### Via Pipeline Principal
```bash
cd sabo/scripts
python run_pipeline.py
```

---

## Métricas Utilizadas

| Métrica | Descrição | Uso |
|---------|-----------|-----|
| MSE | Mean Squared Error | Erro médio quadrático |
| R² | Coeficiente de determinação | Variância explicada |
| MAE | Mean Absolute Error | Erro médio absoluto |
| Accuracy | Acurácia | Classificação |
| Precision | Precisão | Classificação |
| Recall | Revocação | Classificação |
| F1-Score | Média harmônica P/R | Classificação |

---

## Categorias de Manutenção

Os modelos discretizam predições em categorias:

| Categoria | Dias para Manutenção |
|-----------|---------------------|
| **Curto** | < 100 dias |
| **Médio** | 100-200 dias |
| **Longo** | > 200 dias |

---

## Dependências

```
pandas>=1.5.0
numpy>=1.23.0
scikit-learn>=1.2.0
xgboost>=1.7.0
matplotlib>=3.6.0
```

---

## Histórico de Execuções

O sistema mantém histórico de execuções em:
- `history/runs/` - Resultados JSON por execução
- `history/reports/` - Relatórios markdown

Use `history_manager.py` para consultar e comparar execuções.

---

## Versão

- **Versão atual:** 2.0.0
- **Data:** 2024
- **Tipo:** Scripts Python (convertidos de Jupyter Notebooks)
