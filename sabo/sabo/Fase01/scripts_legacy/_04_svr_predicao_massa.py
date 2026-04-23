"""
04. SVR - Predição de Massa
===========================
Usa Support Vector Regression para prever o consumo de massa.
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
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


def train_svr_model(X_train, X_test, y_train, y_test, kernel: str = "rbf") -> dict:
    """
    Treina modelo SVR.

    Args:
        kernel: Tipo de kernel ('rbf', 'linear', 'poly')
    """
    print("=" * 60)
    print(f"SUPPORT VECTOR REGRESSION (kernel={kernel})")
    print("Target: Consumo_Massa_Total")
    print("=" * 60)

    model = SVR(kernel=kernel)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\nResultados:")
    print(f"  Mean Squared Error: {mse:.4f}")
    print(f"  R-squared: {r2:.4f}")

    return {"model": model, "mse": mse, "r2": r2}


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

    # Testar diferentes kernels
    kernels = ["rbf", "linear", "poly"]
    results = {}

    for kernel in kernels:
        print()
        results[kernel] = train_svr_model(X_train, X_test, y_train, y_test, kernel)

    # Melhor kernel
    best_kernel = min(results, key=lambda x: results[x]["mse"])
    print(f"\n{'=' * 60}")
    print(f"MELHOR KERNEL: {best_kernel}")
    print(f"MSE: {results[best_kernel]['mse']:.4f}")
    print(f"R²: {results[best_kernel]['r2']:.4f}")

    return results


if __name__ == "__main__":
    main()
