"""
S11 - XGBoost com Otimização Bayesiana
======================================
XGBoost com RandomizedSearchCV (alternativa à otimização Bayesiana).

O QUE FAZ:
- Treina XGBoost com espaço de busca mais amplo
- Usa RandomizedSearchCV como alternativa ao BayesSearchCV
- Otimiza mais hiperparâmetros (subsample, colsample_bytree)
- Avalia com matriz de confusão

QUANDO USAR:
- Quando GridSearch é muito lento
- Para explorar espaços de busca maiores
- Quando se quer otimização mais eficiente

NOTA:
- O notebook original usava BayesSearchCV (scikit-optimize)
- Esta versão usa RandomizedSearchCV que está disponível no sklearn

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Modelo XGBoost otimizado
- Melhores hiperparâmetros
- Métricas de avaliação
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score, confusion_matrix
from scipy.stats import uniform, randint


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """Carrega e pré-processa os dados."""
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
    })

    df = df.drop([
        "Data de Produção Acumulada", "Cod. Ordem", "Cod Recurso",
        "Fator Un.", "Cód. Un.", "Descrição da massa (Composto)",
    ], axis=1)

    df = pd.get_dummies(df, columns=["Cod Produto"])

    return df


def create_preprocessor():
    """Cria preprocessor."""
    numeric_features = [
        "Qtd_Produzida", "Qtd_Refugada", "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada"
    ]
    categorical_features = [
        "Cod Produto_SA02004", "Cod Produto_SA02961", "Cod Produto_SA05780"
    ]

    return ColumnTransformer(transformers=[
        ("num", StandardScaler(), numeric_features),
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
    ])


def train_xgboost_randomized(X_train, y_train, preprocessor, n_iter=50) -> RandomizedSearchCV:
    """
    Treina XGBoost com RandomizedSearchCV.

    Espaço de busca:
    - n_estimators: 50-200 (inteiro)
    - max_depth: 3-7 (inteiro)
    - learning_rate: 0.001-0.1 (contínuo)
    - subsample: 0.5-1.0 (contínuo)
    - colsample_bytree: 0.5-1.0 (contínuo)
    """
    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", xgb.XGBRegressor(random_state=42))
    ])

    # Espaço de busca com distribuições
    param_distributions = {
        "regressor__n_estimators": randint(50, 201),  # 50 a 200
        "regressor__max_depth": randint(3, 8),  # 3 a 7
        "regressor__learning_rate": uniform(0.001, 0.099),  # 0.001 a 0.1
        "regressor__subsample": uniform(0.5, 0.5),  # 0.5 a 1.0
        "regressor__colsample_bytree": uniform(0.5, 0.5),  # 0.5 a 1.0
    }

    randomized_search = RandomizedSearchCV(
        pipeline,
        param_distributions,
        n_iter=n_iter,
        cv=5,
        scoring="neg_mean_squared_error",
        random_state=42,
        n_jobs=-1
    )

    print(f"\nExecutando RandomizedSearchCV ({n_iter} iterações)...")
    randomized_search.fit(X_train, y_train)

    return randomized_search


def print_confusion_matrix(y_true, y_pred):
    """Exibe matriz de confusão discretizada."""
    bins = [-float("inf"), -200, -100, float("inf")]
    labels = ["Curto", "Médio", "Longo"]

    y_true_cat = pd.cut(y_true, bins=bins, labels=labels)
    y_pred_cat = pd.cut(y_pred, bins=bins, labels=labels)

    cm = confusion_matrix(y_true_cat, y_pred_cat, labels=labels)

    print("\nMatriz de Confusão:")
    print("-" * 35)
    print(f"           Curto   Médio   Longo")
    print("-" * 35)
    for i, label in enumerate(labels):
        print(f"{label:>10} {cm[i,0]:>6} {cm[i,1]:>7} {cm[i,2]:>7}")
    print("-" * 35)


def main():
    """Função principal."""
    print("=" * 60)
    print("S11 - XGBOOST COM OTIMIZAÇÃO RANDOMIZADA")
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

    # Preprocessor
    preprocessor = create_preprocessor()

    # Treinar com RandomizedSearchCV
    randomized_search = train_xgboost_randomized(
        X_train, y_train, preprocessor, n_iter=50
    )

    # Melhores parâmetros
    print("\n" + "=" * 60)
    print("MELHORES HIPERPARÂMETROS")
    print("=" * 60)
    for param, value in randomized_search.best_params_.items():
        param_name = param.replace("regressor__", "")
        if isinstance(value, float):
            print(f"  {param_name}: {value:.4f}")
        else:
            print(f"  {param_name}: {value}")

    # Predições
    y_pred = randomized_search.predict(X_test)

    # Métricas
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("\n" + "=" * 60)
    print("RESULTADOS")
    print("=" * 60)
    print(f"MSE: {mse:.2f}")
    print(f"R²:  {r2:.4f}")

    # Matriz de confusão
    print_confusion_matrix(y_test, y_pred)

    # Exemplos de predições
    print("\n" + "=" * 60)
    print("EXEMPLOS DE PREDIÇÕES")
    print("=" * 60)
    indices = np.random.choice(len(y_test), 10, replace=False)
    for idx in indices:
        real = y_test.iloc[idx]
        pred = y_pred[idx]
        print(f"Real: {real:>6} | Predito: {pred:>8.2f} | Erro: {abs(real-pred):>6.2f}")

    print("=" * 60)

    return {
        "model": randomized_search.best_estimator_,
        "best_params": randomized_search.best_params_,
        "mse": mse,
        "r2": r2
    }


if __name__ == "__main__":
    main()
