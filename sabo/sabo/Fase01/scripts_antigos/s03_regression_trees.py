"""
S03 - Regressão com Modelos Baseados em Árvores
===============================================
Predição de consumo de massa usando Decision Trees, Random Forest e XGBoost.

O QUE FAZ:
- Carrega dados pré-processados
- Treina modelos baseados em árvores de decisão
- Compara Decision Tree, Random Forest e XGBoost
- Avalia com MSE e R²

QUANDO USAR:
- Quando relações não-lineares são esperadas
- Para capturar interações entre features
- Quando interpretabilidade é menos crítica que performance

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Comparativo de performance entre modelos de árvore
- Identificação do melhor modelo
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """Carrega e pré-processa os dados."""
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada": "Consumo_Massa_Total",
    })

    df = df.drop([
        "Data de Produção Acumulada", "Cod. Ordem", "Cod Recurso",
        "Cod Produto", "Fator Un.", "Cód. Un.",
        "Descrição da massa (Composto)", "Tempo Restante para Manutenção",
    ], axis=1)

    return df


def train_tree_models(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina modelos baseados em árvores.

    Modelos:
    - DecisionTreeRegressor: Árvore simples (pode overfittar)
    - RandomForestRegressor: Ensemble de árvores (mais robusto)
    - GradientBoostingRegressor: Boosting sequencial
    - XGBRegressor: XGBoost (alta performance)
    """
    models = {
        "DecisionTree": DecisionTreeRegressor(random_state=42),
        "RandomForest": RandomForestRegressor(n_estimators=100, random_state=42),
        "GradientBoosting": GradientBoostingRegressor(random_state=42),
        "XGBoost": xgb.XGBRegressor(random_state=42),
    }

    results = {}

    print("\n" + "-" * 50)
    print("MODELOS BASEADOS EM ÁRVORES")
    print("-" * 50)

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"model": model, "mse": mse, "r2": r2}

        print(f"\n{name}:")
        print(f"  MSE: {mse:.4f}")
        print(f"  R²:  {r2:.4f}")

    return results


def get_feature_importance(model, feature_names: list) -> dict:
    """Extrai importância das features de um modelo de árvore."""
    if hasattr(model, 'feature_importances_'):
        importance = dict(zip(feature_names, model.feature_importances_))
        return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    return {}


def main():
    """Função principal."""
    print("=" * 60)
    print("S03 - REGRESSÃO COM MODELOS DE ÁRVORE")
    print("Target: Consumo_Massa_Total")
    print("=" * 60)

    # Carregar dados
    df = load_and_preprocess("dados_manutencao.csv")

    # Separar features e target
    X = df.drop("Consumo_Massa_Total", axis=1)
    y = df["Consumo_Massa_Total"]

    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nDataset: {len(df)} amostras")
    print(f"Treino: {len(X_train)} | Teste: {len(X_test)}")

    # Treinar modelos
    results = train_tree_models(X_train, X_test, y_train, y_test)

    # Melhor modelo
    best_name = min(results, key=lambda x: results[x]["mse"])

    print("\n" + "=" * 60)
    print(f"MELHOR MODELO: {best_name}")
    print(f"MSE: {results[best_name]['mse']:.4f}")
    print(f"R²:  {results[best_name]['r2']:.4f}")

    # Feature importance do melhor modelo
    importance = get_feature_importance(results[best_name]["model"], X.columns.tolist())
    if importance:
        print("\nImportância das Features:")
        for feat, imp in importance.items():
            print(f"  {feat}: {imp:.4f}")

    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
