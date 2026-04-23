# SABO ML Pipeline - Versão Legacy (v1.0)

Esta pasta contém os notebooks Jupyter originais do projeto SABO.

## Estrutura

```
legacy/
└── notebooks/
    ├── 01.gen_dataset_scheme.ipynb      → s01_gen_dataset.py
    ├── 02.classification.ipynb          → s02_regression_linear.py
    ├── 03.predicao_massa_total.ipynb    → s03_regression_trees.py
    ├── 04.svr_predicao_massa.ipynb      → s04_regression_svr.py
    ├── 05.classificacao_produtos...     → s05_classification.py
    ├── 06.pred_tempo_manutencao.ipynb   → s06_maintenance_basic.py
    ├── 07.pred_tempo_manut_novo...      → s07_maintenance_decision_tree.py
    ├── 08.pred_tempo_manutencao_rf...   → s08_maintenance_random_forest.py
    ├── 09.pred_tempo_man_gboost...      → s09_maintenance_ensemble.py
    ├── 10.pred_tempo_man_xgboost...     → s10_maintenance_xgboost.py
    ├── 11.pred_tempo_man_xgboost_bayes  → s11_maintenance_xgboost_bayes.py
    ├── gen_data_ij.ipynb                → s12_gen_real_data.py
    ├── gen_data_ij044.ipynb             → s12_gen_real_data.py
    ├── prediction_1.ipynb               → s13_prediction_real_data.py
    ├── prediction_2.ipynb               → s13_prediction_real_data.py
    ├── explo_data_ij044.ipynb           (análise exploratória)
    ├── explo_data_ij046.ipynb           (análise exploratória)
    ├── explo_data_ij117.ipynb           (análise exploratória)
    └── analise_resultados.ipynb         (análise de resultados)
```

## Mapeamento Notebook → Script

| Notebook Original | Script Atual |
|-------------------|--------------|
| 01.gen_dataset_scheme.ipynb | s01_gen_dataset.py |
| 02.classification.ipynb | s02_regression_linear.py |
| 03.predicao_massa_total.ipynb | s03_regression_trees.py |
| 04.svr_predicao_massa.ipynb | s04_regression_svr.py |
| 05.classificacao_produtos_arvores_decisao.ipynb | s05_classification.py |
| 06.pred_tempo_manutencao.ipynb | s06_maintenance_basic.py |
| 07.pred_tempo_manut_novo_esquema.ipynb | s07_maintenance_decision_tree.py |
| 08.pred_tempo_manutencao_random_forest.ipynb | s08_maintenance_random_forest.py |
| 09.pred_tempo_man_gboost_svm_random_confucio.ipynb | s09_maintenance_ensemble.py |
| 10.pred_tempo_man_xgboost.ipynb | s10_maintenance_xgboost.py |
| 11.pred_tempo_man_xgboost_bayes.ipynb | s11_maintenance_xgboost_bayes.py |
| gen_data_ij.ipynb + gen_data_ij044.ipynb | s12_gen_real_data.py |
| prediction_1.ipynb + prediction_2.ipynb | s13_prediction_real_data.py |

## Nota

Os notebooks originais estão preservados aqui para referência histórica.
Para desenvolvimento e produção, use os scripts Python na pasta `../current/`.

## Versão

- **Versão:** 1.0.0 (Legacy)
- **Status:** Arquivado
- **Formato:** Jupyter Notebooks (.ipynb)
