"""
S07 - Predição de Manutenção com Decision Tree
==============================================
Predição específica usando Decision Tree Regressor.

O QUE FAZ:
- Foca exclusivamente em Decision Tree
- Avalia performance básica do modelo
- Serve como baseline simples e interpretável

QUANDO USAR:
- Quando se precisa de interpretabilidade
- Para visualizar a lógica de decisão
- Como baseline para comparar com modelos complexos

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Modelo Decision Tree treinado
- Métricas MSE e R²
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, r2_score


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """Carrega e pré-processa os dados."""
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
    })

    df = df.drop([
        "Data de Produção Acumulada", "Cod. Ordem", "Cod Recurso",
        "Fator Un.", "Cód. Un.", "Descrição da massa (Composto)",
    ], axis=1)

    # One-hot encoding
    df = pd.get_dummies(df, columns=["Cod Produto"])

    return df


def train_decision_tree(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina Decision Tree Regressor.

    Decision Tree:
    - Vantagem: Interpretável, visualizável
    - Desvantagem: Pode overfittar facilmente
    - Quando usar: Baseline, interpretabilidade necessária
    """
    print("\n" + "-" * 50)
    print("DECISION TREE REGRESSOR")
    print("-" * 50)

    model = DecisionTreeRegressor(random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\nResultados:")
    print(f"  MSE: {mse:.2f}")
    print(f"  R²:  {r2:.4f}")

    # Informações da árvore
    print(f"\nEstrutura da Árvore:")
    print(f"  Profundidade: {model.get_depth()}")
    print(f"  Número de folhas: {model.get_n_leaves()}")
    print(f"  Número de features: {model.n_features_in_}")

    return {"model": model, "mse": mse, "r2": r2}


def get_feature_importance(model, feature_names: list):
    """Exibe importância das features."""
    importance = dict(zip(feature_names, model.feature_importances_))
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)

    print("\nImportância das Features:")
    for feat, imp in sorted_imp[:10]:  # Top 10
        print(f"  {feat}: {imp:.4f}")


def main():
    """Função principal."""
    print("=" * 60)
    print("S07 - DECISION TREE PARA MANUTENÇÃO")
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

    # Treinar modelo
    result = train_decision_tree(X_train, X_test, y_train, y_test)

    # Feature importance
    get_feature_importance(result["model"], X.columns.tolist())

    print("\n" + "=" * 60)

    return result


if __name__ == "__main__":
    main()
