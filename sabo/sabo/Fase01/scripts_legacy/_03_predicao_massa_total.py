"""
03. Predição de Massa Total
===========================
Usa Decision Tree, Random Forest e XGBoost para prever
o consumo total de massa.
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score


def load_and_preprocess_data(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """Carrega e pré-processa os dados."""
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada": "Consumo_Massa_Total",
    })

    df = df.drop([
        "Data de Produção Acumulada",
        "Cod. Ordem",
        "Cod Recurso",
        "Cod Produto",
        "Fator Un.",
        "Cód. Un.",
        "Descrição da massa (Composto)",
        "Tempo Restante para Manutenção",
    ], axis=1)

    return df


def train_tree_based_models(X_train, X_test, y_train, y_test) -> dict:
    """Treina modelos baseados em árvores."""
    models = {
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(n_estimators=100),
        "XGBRegressor": xgb.XGBRegressor(),
        "GradientBoostingRegressor": GradientBoostingRegressor(),
    }

    results = {}

    print("=" * 60)
    print("MODELOS BASEADOS EM ÁRVORES")
    print("Target: Consumo_Massa_Total")
    print("=" * 60)

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"model": model, "mse": mse, "r2": r2}

        print(f"\n{name}:")
        print(f"  Mean Squared Error: {mse:.4f}")
        print(f"  R-squared: {r2:.4f}")

    return results


def main():
    print("Carregando dados...")
    df = load_and_preprocess_data("dados_manutencao.csv")

    print("\nPrimeiras 5 linhas:")
    print(df.head().to_markdown(index=False, numalign="left", stralign="left"))

    # Dividir features e target
    X = df.drop("Consumo_Massa_Total", axis=1)
    y = df["Consumo_Massa_Total"]

    # Dividir em treino e teste
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Treinar modelos
    results = train_tree_based_models(X_train, X_test, y_train, y_test)

    # Melhor modelo
    best_model_name = min(results, key=lambda x: results[x]["mse"])
    print(f"\n{'=' * 60}")
    print(f"MELHOR MODELO: {best_model_name}")
    print(f"MSE: {results[best_model_name]['mse']:.4f}")
    print(f"R²: {results[best_model_name]['r2']:.4f}")

    return results


if __name__ == "__main__":
    main()
