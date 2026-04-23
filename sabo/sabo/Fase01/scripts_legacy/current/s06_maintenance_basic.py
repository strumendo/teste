"""
S06 - Predição de Manutenção (Básico)
=====================================
Predição do tempo restante para manutenção usando múltiplos algoritmos.

O QUE FAZ:
- Prepara dados para predição de manutenção
- Aplica one-hot encoding em variáveis categóricas
- Treina SVR, XGBoost, Random Forest, Gradient Boosting e Decision Tree
- Compara todos os modelos
- Salva o melhor modelo

QUANDO USAR:
- Para prever quando um equipamento precisará de manutenção
- Como ponto de partida para análise de manutenção preditiva
- Para comparar diferentes abordagens de ML

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Comparativo de performance dos modelos
- best_model.joblib: Melhor modelo serializado
"""

import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.tree import DecisionTreeRegressor
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """
    Carrega e pré-processa dados para predição de manutenção.

    Operações:
    - Renomeia colunas
    - Remove colunas não relevantes
    - Aplica one-hot encoding em Cod Produto
    """
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
    })

    # Remover colunas não necessárias
    df = df.drop([
        "Data de Produção Acumulada",
        "Cod. Ordem",
        "Cod Recurso",
        "Fator Un.",
        "Cód. Un.",
        "Descrição da massa (Composto)",
    ], axis=1)

    # One-hot encoding para Cod Produto
    df = pd.get_dummies(df, columns=["Cod Produto"])

    return df


def train_all_models(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina e avalia múltiplos modelos de regressão.

    Modelos:
    - SVR: Support Vector Regression
    - XGBoost: Extreme Gradient Boosting
    - RandomForest: Ensemble de árvores
    - GradientBoosting: Boosting sequencial
    - DecisionTree: Árvore simples
    """
    models = {
        "SVR": SVR(kernel="rbf"),
        "XGBoost": xgb.XGBRegressor(random_state=42),
        "RandomForest": RandomForestRegressor(n_estimators=100, random_state=42),
        "GradientBoosting": GradientBoostingRegressor(random_state=42),
        "DecisionTree": DecisionTreeRegressor(random_state=42),
    }

    results = {}

    print("\n" + "-" * 50)
    print("TREINAMENTO DOS MODELOS")
    print("-" * 50)

    for name, model in models.items():
        print(f"\nTreinando {name}...")
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"model": model, "mse": mse, "r2": r2}

        print(f"  MSE: {mse:.2f}")
        print(f"  R²:  {r2:.4f}")

    return results


def save_best_model(results: dict, output_path: str = "best_model.joblib") -> str:
    """Salva o melhor modelo baseado no MSE."""
    best_name = min(results, key=lambda x: results[x]["mse"])
    best_model = results[best_name]["model"]

    joblib.dump(best_model, output_path)
    print(f"\n✓ Melhor modelo ({best_name}) salvo em '{output_path}'")

    return best_name


def main():
    """Função principal."""
    print("=" * 60)
    print("S06 - PREDIÇÃO DE MANUTENÇÃO (BÁSICO)")
    print("Target: Tempo Restante para Manutenção")
    print("=" * 60)

    # Carregar dados
    print("\nCarregando dados...")
    df = load_and_preprocess("dados_manutencao.csv")

    print(f"Dataset: {len(df)} amostras, {len(df.columns)} features")

    # Separar features e target
    X = df.drop("Tempo Restante para Manutenção", axis=1)
    y = df["Tempo Restante para Manutenção"]

    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"Treino: {len(X_train)} | Teste: {len(X_test)}")

    # Treinar modelos
    results = train_all_models(X_train, X_test, y_train, y_test)

    # Ranking dos modelos
    print("\n" + "=" * 60)
    print("RANKING DOS MODELOS (por MSE)")
    print("=" * 60)

    sorted_results = sorted(results.items(), key=lambda x: x[1]["mse"])
    for i, (name, metrics) in enumerate(sorted_results, 1):
        print(f"{i}. {name}: MSE={metrics['mse']:.2f}, R²={metrics['r2']:.4f}")

    # Salvar melhor modelo
    best_name = save_best_model(results)

    print("\n" + "=" * 60)
    print(f"MELHOR MODELO: {best_name}")
    print("=" * 60)

    return results, best_name


if __name__ == "__main__":
    main()
