"""
S10 - Predição com XGBoost + GridSearch
=======================================
XGBoost otimizado com comparativo SVM e Random Forest.

O QUE FAZ:
- Treina XGBoost com GridSearchCV
- Compara com SVM e Random Forest
- Gera matrizes de confusão
- Análise detalhada de cada modelo

QUANDO USAR:
- Quando XGBoost é candidato principal
- Para datasets tabulares médios/grandes
- Quando se busca alta performance

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Comparativo de 3 modelos
- Melhor modelo identificado
"""

import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
import xgboost as xgb
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
    """Cria preprocessor comum."""
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


def train_xgboost(X_train, y_train, preprocessor) -> dict:
    """
    Treina XGBoost com GridSearch.

    Hiperparâmetros:
    - n_estimators: número de árvores
    - max_depth: profundidade máxima
    - learning_rate: taxa de aprendizado
    """
    print("\n" + "-" * 40)
    print("XGBoost")
    print("-" * 40)

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", xgb.XGBRegressor(random_state=42))
    ])

    param_grid = {
        "regressor__n_estimators": [50, 100, 200],
        "regressor__max_depth": [3, 5, 7],
        "regressor__learning_rate": [0.1, 0.01, 0.001],
    }

    grid_search = GridSearchCV(
        pipeline, param_grid, cv=5,
        scoring="neg_mean_squared_error",
        n_jobs=-1
    )
    grid_search.fit(X_train, y_train)

    print(f"Melhores parâmetros: {grid_search.best_params_}")
    return grid_search


def train_svm(X_train, y_train, preprocessor) -> dict:
    """Treina SVM com GridSearch."""
    print("\n" + "-" * 40)
    print("SVM")
    print("-" * 40)

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", SVR(kernel="rbf"))
    ])

    param_grid = {
        "regressor__C": [0.1, 1, 10],
        "regressor__gamma": [1, 0.1, 0.01]
    }

    grid_search = GridSearchCV(
        pipeline, param_grid, cv=5,
        scoring="neg_mean_squared_error"
    )
    grid_search.fit(X_train, y_train)

    print(f"Melhores parâmetros: {grid_search.best_params_}")
    return grid_search


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

    grid_search = GridSearchCV(
        pipeline, param_grid, cv=5,
        scoring="neg_mean_squared_error",
        n_jobs=-1
    )
    grid_search.fit(X_train, y_train)

    print(f"Melhores parâmetros: {grid_search.best_params_}")
    return grid_search


def print_confusion_matrix(y_true, y_pred, name: str):
    """Exibe matriz de confusão discretizada."""
    bins = [-float("inf"), -200, -100, float("inf")]
    labels = ["Curto", "Médio", "Longo"]

    y_true_cat = pd.cut(y_true, bins=bins, labels=labels)
    y_pred_cat = pd.cut(y_pred, bins=bins, labels=labels)

    cm = confusion_matrix(y_true_cat, y_pred_cat, labels=labels)

    print(f"\nMatriz de Confusão - {name}:")
    print(f"           Curto   Médio   Longo")
    for i, label in enumerate(labels):
        print(f"{label:>10} {cm[i,0]:>6} {cm[i,1]:>7} {cm[i,2]:>7}")


def main():
    """Função principal."""
    print("=" * 60)
    print("S10 - XGBOOST COM GRIDSEARCH")
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

    # Treinar modelos
    print("\nTreinando modelos...")

    models = {
        "SVM": train_svm(X_train, y_train, preprocessor),
        "RandomForest": train_random_forest(X_train, y_train, preprocessor),
        "XGBoost": train_xgboost(X_train, y_train, preprocessor),
    }

    # Avaliar modelos
    print("\n" + "=" * 60)
    print("RESULTADOS")
    print("=" * 60)

    results = {}
    for name, model in models.items():
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"mse": mse, "r2": r2}

        print(f"\n{name}:")
        print(f"  MSE: {mse:.2f}")
        print(f"  R²:  {r2:.4f}")

        print_confusion_matrix(y_test, y_pred, name)

    # Melhor modelo
    best_name = min(results, key=lambda x: results[x]["mse"])

    print("\n" + "=" * 60)
    print(f"🏆 MELHOR MODELO: {best_name}")
    print(f"   MSE: {results[best_name]['mse']:.2f}")
    print(f"   R²:  {results[best_name]['r2']:.4f}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
