"""
02. Classificação - Previsão de Consumo de Massa Total
======================================================
Usa algoritmos de regressão para prever o consumo de massa total
com base nas quantidades produzidas, refugadas e retrabalhadas.
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score


def load_and_preprocess_data(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """
    Carrega e pré-processa os dados para análise.

    Args:
        filepath: Caminho para o arquivo CSV

    Returns:
        DataFrame pré-processado
    """
    df = pd.read_csv(filepath)

    # Renomear colunas
    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada": "Consumo_Massa_Total",
    })

    # Remover colunas desnecessárias
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


def train_and_evaluate_models(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina e avalia múltiplos modelos de regressão.

    Returns:
        Dicionário com resultados de cada modelo
    """
    models = {
        "LinearRegression": LinearRegression(),
        "Ridge": Ridge(alpha=1.0),
        "Lasso": Lasso(alpha=1.0),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(n_estimators=100),
        "GradientBoostingRegressor": GradientBoostingRegressor(),
    }

    results = {}

    print("=" * 60)
    print("RESULTADOS DOS MODELOS DE REGRESSÃO")
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
    # Carregar dados
    print("Carregando dados...")
    df = load_and_preprocess_data("dados_manutencao.csv")

    print("\nPrimeiras 5 linhas do DataFrame:")
    print(df.head().to_markdown(index=False, numalign="left", stralign="left"))

    print("\nInformações do DataFrame:")
    print(df.info())

    # Dividir features e target
    X = df.drop("Consumo_Massa_Total", axis=1)
    y = df["Consumo_Massa_Total"]

    # Dividir em treino e teste
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Treinar e avaliar modelos
    results = train_and_evaluate_models(X_train, X_test, y_train, y_test)

    # Encontrar melhor modelo
    best_model_name = min(results, key=lambda x: results[x]["mse"])
    print(f"\n{'=' * 60}")
    print(f"MELHOR MODELO: {best_model_name}")
    print(f"MSE: {results[best_model_name]['mse']:.4f}")
    print(f"R²: {results[best_model_name]['r2']:.4f}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
