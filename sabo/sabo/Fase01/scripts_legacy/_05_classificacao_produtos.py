"""
05. Classificação de Produtos com Árvore de Decisão
===================================================
Classifica produtos usando DecisionTreeClassifier com
otimização de hiperparâmetros via GridSearchCV.
"""

import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report


def load_and_preprocess_data(filepath: str = "dados_manutencao.csv") -> tuple:
    """Carrega e pré-processa os dados para classificação."""
    df = pd.read_csv(filepath, header=0)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada": "Consumo_Massa_Total",
        "Tempo Restante para Manutenção": "Tempo_Restante_Manutencao",
    })

    X = df[[
        "Qtd_Produzida",
        "Qtd_Refugada",
        "Qtd_Retrabalhada",
        "Consumo_Massa_Total",
        "Tempo_Restante_Manutencao",
    ]]
    y = df["Cod Produto"]

    return X, y


def train_basic_model(X_train, X_test, y_train, y_test):
    """Treina modelo básico com pipeline."""
    print("=" * 60)
    print("CLASSIFICAÇÃO DE PRODUTOS - MODELO BÁSICO")
    print("=" * 60)

    # Criar transformador
    numeric_features = [
        "Qtd_Produzida",
        "Qtd_Refugada",
        "Qtd_Retrabalhada",
        "Consumo_Massa_Total",
        "Tempo_Restante_Manutencao",
    ]
    numeric_transformer = Pipeline(steps=[("scaler", StandardScaler())])

    preprocessor = ColumnTransformer(
        transformers=[("num", numeric_transformer, numeric_features)]
    )

    # Criar e treinar modelo
    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", DecisionTreeClassifier())
    ])
    model.fit(X_train, y_train)

    # Avaliar
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    print(f"\nAcurácia do modelo: {accuracy:.4f}")
    print("\nRelatório de Classificação:")
    print(classification_report(y_test, y_pred))

    return model, accuracy


def train_optimized_model(X_train, X_test, y_train, y_test):
    """Treina modelo otimizado com GridSearchCV."""
    print("=" * 60)
    print("CLASSIFICAÇÃO DE PRODUTOS - MODELO OTIMIZADO")
    print("=" * 60)

    # Padronizar features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Definir grid de parâmetros
    param_grid = {
        "criterion": ["gini", "entropy"],
        "max_depth": [2, 3, 4, 5, 10, 20, None],
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
    }

    # Criar modelo e GridSearch
    dtc = DecisionTreeClassifier(random_state=42)
    grid_search = GridSearchCV(estimator=dtc, param_grid=param_grid, cv=5)

    # Treinar
    print("\nExecutando GridSearchCV...")
    grid_search.fit(X_train_scaled, y_train)

    # Melhores parâmetros
    best_params = grid_search.best_params_
    print(f"\nMelhores parâmetros: {best_params}")

    # Avaliar melhor modelo
    best_model = grid_search.best_estimator_
    y_pred = best_model.predict(X_test_scaled)
    accuracy = accuracy_score(y_test, y_pred)

    print(f"\nAcurácia do melhor modelo: {accuracy:.4f}")
    print("\nRelatório de Classificação:")
    print(classification_report(y_test, y_pred))

    return best_model, scaler, best_params, accuracy


def main():
    print("Carregando dados...")
    X, y = load_and_preprocess_data("dados_manutencao.csv")

    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nDados de treino: {len(X_train)} amostras")
    print(f"Dados de teste: {len(X_test)} amostras")

    # Modelo básico
    basic_model, basic_accuracy = train_basic_model(
        X_train, X_test, y_train, y_test
    )

    print()

    # Modelo otimizado
    optimized_model, scaler, best_params, optimized_accuracy = train_optimized_model(
        X_train, X_test, y_train, y_test
    )

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"Acurácia modelo básico: {basic_accuracy:.4f}")
    print(f"Acurácia modelo otimizado: {optimized_accuracy:.4f}")
    print(f"Melhoria: {(optimized_accuracy - basic_accuracy) * 100:.2f}%")

    return {
        "basic_model": basic_model,
        "optimized_model": optimized_model,
        "scaler": scaler,
        "best_params": best_params,
    }


if __name__ == "__main__":
    main()
