"""
06. Predição de Tempo Restante para Manutenção
==============================================
Usa SVR, XGBoost, Random Forest e outros para prever
o tempo restante até a próxima manutenção.
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.tree import DecisionTreeRegressor
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score
import joblib


def load_and_preprocess_data(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """Carrega e pré-processa os dados."""
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
    })

    # Remover colunas desnecessárias
    df = df.drop([
        "Data de Produção Acumulada",
        "Cod. Ordem",
        "Cod Recurso",
        "Fator Un.",
        "Cód. Un.",
        "Descrição da massa (Composto)",
    ], axis=1)

    # One-hot encode para Cod Produto
    df = pd.get_dummies(df, columns=["Cod Produto"])

    return df


def train_and_evaluate_models(X_train, X_test, y_train, y_test) -> dict:
    """Treina e avalia múltiplos modelos de regressão."""
    models = {
        "SVR": SVR(kernel="rbf"),
        "XGBoost": xgb.XGBRegressor(),
        "Random Forest": RandomForestRegressor(n_estimators=100),
        "Gradient Boosting": GradientBoostingRegressor(),
        "Decision Tree": DecisionTreeRegressor(),
    }

    results = {}

    print("=" * 60)
    print("PREDIÇÃO DE TEMPO RESTANTE PARA MANUTENÇÃO")
    print("=" * 60)

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"model": model, "mse": mse, "r2": r2}

        print(f"\n{name}:")
        print(f"  Mean Squared Error: {mse:.2f}")
        print(f"  R-squared: {r2:.4f}")

    return results


def save_best_model(results: dict, output_path: str = "best_model.joblib"):
    """Salva o melhor modelo."""
    best_model_name = min(results, key=lambda x: results[x]["mse"])
    best_model = results[best_model_name]["model"]

    joblib.dump(best_model, output_path)
    print(f"\n✓ Melhor modelo ({best_model_name}) salvo em '{output_path}'")

    return best_model_name, best_model


def main():
    print("Carregando dados...")
    df = load_and_preprocess_data("dados_manutencao.csv")

    print("\nPrimeiras 5 linhas:")
    print(df.head().to_markdown(index=False, numalign="left", stralign="left"))

    print("\nInformações do DataFrame:")
    print(df.info())

    # Dividir features e target
    X = df.drop("Tempo Restante para Manutenção", axis=1)
    y = df["Tempo Restante para Manutenção"]

    # Dividir em treino e teste
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nDados de treino: {len(X_train)} amostras")
    print(f"Dados de teste: {len(X_test)} amostras")

    # Treinar e avaliar modelos
    results = train_and_evaluate_models(X_train, X_test, y_train, y_test)

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO DOS RESULTADOS")
    print("=" * 60)

    # Ordenar por MSE
    sorted_results = sorted(results.items(), key=lambda x: x[1]["mse"])

    print("\nModelos ordenados por MSE (menor é melhor):")
    for i, (name, res) in enumerate(sorted_results, 1):
        print(f"{i}. {name}: MSE={res['mse']:.2f}, R²={res['r2']:.4f}")

    # Salvar melhor modelo
    best_name, best_model = save_best_model(results)

    return results, best_name


if __name__ == "__main__":
    main()
