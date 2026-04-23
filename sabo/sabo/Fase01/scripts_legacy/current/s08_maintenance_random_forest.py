"""
S08 - Predição de Manutenção com Random Forest + GridSearch
===========================================================
Random Forest otimizado com GridSearchCV e matriz de confusão.

O QUE FAZ:
- Treina Random Forest com pipeline completo
- Aplica GridSearchCV para otimização
- Discretiza previsões em categorias (Curto/Médio/Longo)
- Gera matriz de confusão para análise

QUANDO USAR:
- Para predição robusta de manutenção
- Quando se precisa de um modelo ensemble
- Para análise categorizada do tempo de manutenção

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Modelo otimizado
- Matriz de confusão
- Métricas de avaliação
"""

import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, confusion_matrix
import numpy as np


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """Carrega e pré-processa os dados."""
    df = pd.read_csv(filepath)

    df = df.drop([
        "Data de Produção Acumulada", "Cod. Ordem", "Cod Recurso",
        "Fator Un.", "Cód. Un.", "Descrição da massa (Composto)",
    ], axis=1)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
    })

    # One-hot encoding
    df = pd.get_dummies(df, columns=["Cod Produto"])

    return df


def create_pipeline():
    """
    Cria pipeline com pré-processamento e Random Forest.

    Pipeline:
    1. Preprocessor: Escala features numéricas
    2. Regressor: Random Forest
    """
    numeric_features = [
        "Qtd_Produzida", "Qtd_Refugada", "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada"
    ]
    categorical_features = [
        "Cod Produto_SA02004", "Cod Produto_SA02961", "Cod Produto_SA05780"
    ]

    numeric_transformer = Pipeline([("scaler", StandardScaler())])
    categorical_transformer = Pipeline([("onehot", OneHotEncoder(handle_unknown="ignore"))])

    preprocessor = ColumnTransformer(transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features),
    ])

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", RandomForestRegressor(random_state=42))
    ])

    return pipeline


def train_with_gridsearch(X_train, y_train) -> GridSearchCV:
    """
    Treina Random Forest com GridSearchCV.

    Hiperparâmetros otimizados:
    - n_estimators: número de árvores
    - max_depth: profundidade máxima
    - min_samples_split: mínimo para split
    """
    pipeline = create_pipeline()

    param_grid = {
        "regressor__n_estimators": [50, 100, 200],
        "regressor__max_depth": [None, 5, 10],
        "regressor__min_samples_split": [2, 5, 10],
    }

    grid_search = GridSearchCV(
        pipeline,
        param_grid,
        cv=5,
        scoring="neg_mean_squared_error",
        n_jobs=-1
    )

    print("\nExecutando GridSearchCV...")
    grid_search.fit(X_train, y_train)

    return grid_search


def discretize_predictions(y_true, y_pred, bins=None, labels=None):
    """
    Discretiza valores contínuos em categorias.

    Categorias padrão:
    - Curto: < -200 dias
    - Médio: -200 a -100 dias
    - Longo: > -100 dias
    """
    if bins is None:
        bins = [-float("inf"), -200, -100, float("inf")]
    if labels is None:
        labels = ["Curto", "Médio", "Longo"]

    y_true_cat = pd.cut(y_true, bins=bins, labels=labels)
    y_pred_cat = pd.cut(y_pred, bins=bins, labels=labels)

    return y_true_cat, y_pred_cat


def print_confusion_matrix(y_true_cat, y_pred_cat, labels):
    """Exibe matriz de confusão formatada."""
    cm = confusion_matrix(y_true_cat, y_pred_cat, labels=labels)

    print("\nMatriz de Confusão:")
    print("-" * 40)

    # Header
    header = "           " + "  ".join([f"{l:>8}" for l in labels])
    print(header)
    print("           " + "-" * (len(labels) * 10))

    # Linhas
    for i, label in enumerate(labels):
        row = f"{label:>10} |" + "  ".join([f"{cm[i,j]:>8}" for j in range(len(labels))])
        print(row)


def main():
    """Função principal."""
    print("=" * 60)
    print("S08 - RANDOM FOREST COM GRIDSEARCH")
    print("Target: Tempo Restante para Manutenção")
    print("=" * 60)

    # Carregar dados
    df = load_and_preprocess("dados_manutencao.csv")

    # Separar features e target
    X = df.drop("Tempo Restante para Manutenção", axis=1)
    y = df["Tempo Restante para Manutenção"]

    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nDataset: {len(df)} amostras")
    print(f"Treino: {len(X_train)} | Teste: {len(X_test)}")

    # Treinar com GridSearch
    grid_search = train_with_gridsearch(X_train, y_train)

    # Melhores parâmetros
    print(f"\nMelhores parâmetros: {grid_search.best_params_}")

    # Predições
    y_pred = grid_search.predict(X_test)

    # Métricas
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\nResultados:")
    print(f"  MSE: {mse:.2f}")
    print(f"  R²:  {r2:.4f}")

    # Matriz de confusão (discretizada)
    labels = ["Curto", "Médio", "Longo"]
    y_true_cat, y_pred_cat = discretize_predictions(y_test, y_pred, labels=labels)
    print_confusion_matrix(y_true_cat, y_pred_cat, labels)

    print("\n" + "=" * 60)

    return {
        "model": grid_search.best_estimator_,
        "best_params": grid_search.best_params_,
        "mse": mse,
        "r2": r2
    }


if __name__ == "__main__":
    main()
