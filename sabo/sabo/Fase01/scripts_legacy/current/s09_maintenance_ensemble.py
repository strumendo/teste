"""
S09 - Comparativo de Modelos Ensemble
=====================================
Compara SVM, Random Forest e Gradient Boosting com GridSearch.

O QUE FAZ:
- Treina 3 modelos diferentes com otimização
- Compara performance de cada modelo
- Gera matriz de confusão para cada um
- Identifica o melhor modelo

QUANDO USAR:
- Para escolher o melhor modelo entre várias opções
- Quando se quer uma análise completa de diferentes abordagens
- Para justificar a escolha de um modelo específico

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Comparativo detalhado de 3 modelos
- Matrizes de confusão
- Ranking final
"""

import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score, confusion_matrix


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

    df = pd.get_dummies(df, columns=["Cod Produto"])

    return df


def create_preprocessor():
    """Cria preprocessor comum para todos os modelos."""
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


def train_svm(X_train, y_train, preprocessor) -> dict:
    """Treina SVM com GridSearch."""
    print("\n" + "-" * 40)
    print("SVM (Support Vector Machine)")
    print("-" * 40)

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", SVR(kernel="rbf"))
    ])

    param_grid = {
        "regressor__C": [0.1, 1, 10],
        "regressor__gamma": [1, 0.1, 0.01]
    }

    grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring="neg_mean_squared_error")
    grid_search.fit(X_train, y_train)

    print(f"Melhores parâmetros: {grid_search.best_params_}")
    return {"model": grid_search.best_estimator_, "name": "SVM"}


def train_random_forest(X_train, y_train, preprocessor) -> dict:
    """Treina Random Forest com GridSearch."""
    print("\n" + "-" * 40)
    print("Random Forest")
    print("-" * 40)

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", RandomForestRegressor(random_state=42))
    ])

    param_grid = {
        "regressor__n_estimators": [50, 100, 200],
        "regressor__max_depth": [None, 5, 10],
        "regressor__min_samples_split": [2, 5, 10],
    }

    grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring="neg_mean_squared_error")
    grid_search.fit(X_train, y_train)

    print(f"Melhores parâmetros: {grid_search.best_params_}")
    return {"model": grid_search.best_estimator_, "name": "RandomForest"}


def train_gradient_boosting(X_train, y_train, preprocessor) -> dict:
    """Treina Gradient Boosting com GridSearch."""
    print("\n" + "-" * 40)
    print("Gradient Boosting")
    print("-" * 40)

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", GradientBoostingRegressor(random_state=42))
    ])

    param_grid = {
        "regressor__n_estimators": [50, 100, 200],
        "regressor__max_depth": [3, 5, 7],
        "regressor__learning_rate": [0.1, 0.01, 0.001],
    }

    grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring="neg_mean_squared_error")
    grid_search.fit(X_train, y_train)

    print(f"Melhores parâmetros: {grid_search.best_params_}")
    return {"model": grid_search.best_estimator_, "name": "GradientBoosting"}


def evaluate_model(model, X_test, y_test, name: str) -> dict:
    """Avalia um modelo e retorna métricas."""
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    return {"name": name, "mse": mse, "r2": r2, "y_pred": y_pred}


def main():
    """Função principal."""
    print("=" * 60)
    print("S09 - COMPARATIVO DE MODELOS ENSEMBLE")
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

    # Preprocessor comum
    preprocessor = create_preprocessor()

    # Treinar modelos
    print("\nTreinando modelos (pode demorar)...")

    models = []
    models.append(train_svm(X_train, y_train, preprocessor))
    models.append(train_random_forest(X_train, y_train, preprocessor))
    models.append(train_gradient_boosting(X_train, y_train, preprocessor))

    # Avaliar modelos
    print("\n" + "=" * 60)
    print("RESULTADOS")
    print("=" * 60)

    results = []
    for m in models:
        result = evaluate_model(m["model"], X_test, y_test, m["name"])
        results.append(result)
        print(f"\n{result['name']}:")
        print(f"  MSE: {result['mse']:.2f}")
        print(f"  R²:  {result['r2']:.4f}")

    # Ranking
    print("\n" + "=" * 60)
    print("RANKING (por MSE)")
    print("=" * 60)

    sorted_results = sorted(results, key=lambda x: x["mse"])
    for i, r in enumerate(sorted_results, 1):
        print(f"{i}. {r['name']}: MSE={r['mse']:.2f}, R²={r['r2']:.4f}")

    print(f"\n🏆 MELHOR MODELO: {sorted_results[0]['name']}")
    print("=" * 60)

    return {r["name"]: {"mse": r["mse"], "r2": r["r2"]} for r in results}


if __name__ == "__main__":
    main()
