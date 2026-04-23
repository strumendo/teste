"""
S05 - Classificação de Produtos
===============================
Classifica produtos usando Decision Tree com otimização de hiperparâmetros.

O QUE FAZ:
- Carrega dados e prepara para classificação
- Treina modelo básico de Decision Tree
- Aplica GridSearchCV para otimizar hiperparâmetros
- Avalia com acurácia, precision, recall e F1-score

QUANDO USAR:
- Para categorizar produtos automaticamente
- Para entender quais features determinam a classe do produto
- Quando se precisa de um modelo interpretável

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Modelo de classificação treinado
- Relatório de classificação com métricas
- Melhores hiperparâmetros encontrados
"""

import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> tuple:
    """
    Carrega e prepara dados para classificação.

    Returns:
        X: Features
        y: Target (Cod Produto)
    """
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


def train_basic_model(X_train, X_test, y_train, y_test) -> tuple:
    """
    Treina modelo básico com pipeline de pré-processamento.

    Pipeline:
    1. StandardScaler: Normaliza features numéricas
    2. DecisionTreeClassifier: Classificador
    """
    print("\n" + "-" * 50)
    print("MODELO BÁSICO (sem otimização)")
    print("-" * 50)

    # Pipeline com pré-processamento
    numeric_features = [
        "Qtd_Produzida", "Qtd_Refugada", "Qtd_Retrabalhada",
        "Consumo_Massa_Total", "Tempo_Restante_Manutencao"
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features)
        ]
    )

    model = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", DecisionTreeClassifier(random_state=42))
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    print(f"\nAcurácia: {accuracy:.4f}")
    print("\nRelatório de Classificação:")
    print(classification_report(y_test, y_pred))

    return model, accuracy


def train_optimized_model(X_train, X_test, y_train, y_test) -> tuple:
    """
    Treina modelo com GridSearchCV para otimização.

    Hiperparâmetros otimizados:
    - criterion: gini ou entropy
    - max_depth: profundidade máxima da árvore
    - min_samples_split: mínimo de amostras para split
    - min_samples_leaf: mínimo de amostras por folha
    """
    print("\n" + "-" * 50)
    print("MODELO OTIMIZADO (GridSearchCV)")
    print("-" * 50)

    # Normalizar features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Grid de hiperparâmetros
    param_grid = {
        "criterion": ["gini", "entropy"],
        "max_depth": [2, 3, 4, 5, 10, 20, None],
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
    }

    # GridSearchCV
    dtc = DecisionTreeClassifier(random_state=42)
    grid_search = GridSearchCV(
        estimator=dtc,
        param_grid=param_grid,
        cv=5,
        scoring="accuracy",
        n_jobs=-1
    )

    print("\nExecutando GridSearchCV (pode demorar)...")
    grid_search.fit(X_train_scaled, y_train)

    # Resultados
    best_params = grid_search.best_params_
    best_model = grid_search.best_estimator_

    y_pred = best_model.predict(X_test_scaled)
    accuracy = accuracy_score(y_test, y_pred)

    print(f"\nMelhores parâmetros: {best_params}")
    print(f"\nAcurácia: {accuracy:.4f}")
    print("\nRelatório de Classificação:")
    print(classification_report(y_test, y_pred))

    return best_model, scaler, best_params, accuracy


def main():
    """Função principal."""
    print("=" * 60)
    print("S05 - CLASSIFICAÇÃO DE PRODUTOS")
    print("Target: Cod Produto")
    print("=" * 60)

    # Carregar dados
    X, y = load_and_preprocess("dados_manutencao.csv")

    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nDataset: {len(X)} amostras")
    print(f"Classes: {y.nunique()} ({list(y.unique())})")
    print(f"Treino: {len(X_train)} | Teste: {len(X_test)}")

    # Modelo básico
    basic_model, basic_acc = train_basic_model(X_train, X_test, y_train, y_test)

    # Modelo otimizado
    opt_model, scaler, best_params, opt_acc = train_optimized_model(
        X_train, X_test, y_train, y_test
    )

    # Comparativo
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"Acurácia Básico:    {basic_acc:.4f}")
    print(f"Acurácia Otimizado: {opt_acc:.4f}")
    print(f"Melhoria: {(opt_acc - basic_acc) * 100:+.2f}%")
    print("=" * 60)

    return {
        "basic_model": basic_model,
        "optimized_model": opt_model,
        "scaler": scaler,
        "best_params": best_params,
        "basic_accuracy": basic_acc,
        "optimized_accuracy": opt_acc
    }


if __name__ == "__main__":
    main()
