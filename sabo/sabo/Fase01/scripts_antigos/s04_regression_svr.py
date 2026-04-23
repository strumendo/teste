"""
S04 - Support Vector Regression (SVR)
=====================================
Predição de consumo de massa usando Support Vector Machines.

O QUE FAZ:
- Aplica SVR com diferentes kernels (rbf, linear, poly)
- Compara performance entre kernels
- Avalia com MSE e R²

QUANDO USAR:
- Datasets de tamanho médio (SVR é lento para grandes volumes)
- Quando se espera relações não-lineares complexas
- Para comparar com outros métodos de regressão

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Comparativo de kernels SVR
- Métricas de avaliação
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler
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


def train_svr_models(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina SVR com diferentes kernels.

    Kernels:
    - rbf: Radial Basis Function (não-linear, mais comum)
    - linear: Kernel linear (similar a regressão linear)
    - poly: Kernel polinomial (captura relações polinomiais)
    """
    # SVR requer normalização
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    kernels = ["rbf", "linear", "poly"]
    results = {}

    print("\n" + "-" * 50)
    print("SUPPORT VECTOR REGRESSION")
    print("-" * 50)

    for kernel in kernels:
        print(f"\nTreinando SVR com kernel={kernel}...")

        model = SVR(kernel=kernel)
        model.fit(X_train_scaled, y_train)
        y_pred = model.predict(X_test_scaled)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[f"SVR_{kernel}"] = {
            "model": model,
            "scaler": scaler,
            "mse": mse,
            "r2": r2
        }

        print(f"  MSE: {mse:.4f}")
        print(f"  R²:  {r2:.4f}")

    return results


def main():
    """Função principal."""
    print("=" * 60)
    print("S04 - SUPPORT VECTOR REGRESSION")
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

    # Treinar SVR com diferentes kernels
    results = train_svr_models(X_train, X_test, y_train, y_test)

    # Melhor kernel
    best_name = min(results, key=lambda x: results[x]["mse"])

    print("\n" + "=" * 60)
    print(f"MELHOR KERNEL: {best_name}")
    print(f"MSE: {results[best_name]['mse']:.4f}")
    print(f"R²:  {results[best_name]['r2']:.4f}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
