"""
S02 - Regressão com Modelos Lineares
====================================
Predição de consumo de massa usando modelos de regressão linear.

O QUE FAZ:
- Carrega e pré-processa os dados de manutenção
- Treina múltiplos modelos lineares (Linear, Ridge, Lasso)
- Treina modelos baseados em árvore para comparação
- Avalia cada modelo com MSE e R²
- Identifica o melhor modelo

QUANDO USAR:
- Para prever o consumo de massa total
- Como baseline para comparar com modelos mais complexos
- Quando se deseja interpretabilidade dos coeficientes

ENTRADA:
- dados_manutencao.csv

SAÍDA:
- Métricas de avaliação de cada modelo
- Identificação do melhor modelo
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score


def load_and_preprocess(filepath: str = "dados_manutencao.csv") -> pd.DataFrame:
    """
    Carrega e pré-processa os dados.

    Operações:
    - Renomeia colunas para nomes mais curtos
    - Remove colunas não relevantes para a previsão de consumo
    """
    df = pd.read_csv(filepath)

    # Renomear colunas
    df = df.rename(columns={
        "Qt. Total Acumulada Produzida até a data específica": "Qtd_Produzida",
        "Qt. Acumulada Refugada até a data específica": "Qtd_Refugada",
        "Qtd. Acumulada total Retrabalhada até a data específica": "Qtd_Retrabalhada",
        "Consumo total de Massa Acumulada": "Consumo_Massa_Total",
    })

    # Remover colunas não necessárias
    cols_to_drop = [
        "Data de Produção Acumulada",
        "Cod. Ordem",
        "Cod Recurso",
        "Cod Produto",
        "Fator Un.",
        "Cód. Un.",
        "Descrição da massa (Composto)",
        "Tempo Restante para Manutenção",
    ]
    df = df.drop(cols_to_drop, axis=1)

    return df


def train_and_evaluate(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina e avalia múltiplos modelos de regressão.

    Modelos avaliados:
    - LinearRegression: Regressão linear simples
    - Ridge: Regressão com regularização L2
    - Lasso: Regressão com regularização L1
    - DecisionTreeRegressor: Árvore de decisão
    - RandomForestRegressor: Ensemble de árvores
    - GradientBoostingRegressor: Boosting gradiente

    Returns:
        Dicionário com modelo, MSE e R² para cada algoritmo
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

    print("\n" + "-" * 50)
    print("RESULTADOS DOS MODELOS")
    print("-" * 50)

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"model": model, "mse": mse, "r2": r2}

        print(f"\n{name}:")
        print(f"  MSE: {mse:.4f}")
        print(f"  R²:  {r2:.4f}")

    return results


def main():
    """Função principal."""
    print("=" * 60)
    print("S02 - REGRESSÃO COM MODELOS LINEARES")
    print("Target: Consumo_Massa_Total")
    print("=" * 60)

    # Carregar dados
    print("\nCarregando dados...")
    df = load_and_preprocess("dados_manutencao.csv")

    print(f"Dataset: {len(df)} amostras, {len(df.columns)} features")
    print(f"Features: {list(df.columns)}")

    # Separar features e target
    X = df.drop("Consumo_Massa_Total", axis=1)
    y = df["Consumo_Massa_Total"]

    # Dividir em treino/teste
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"\nTreino: {len(X_train)} | Teste: {len(X_test)}")

    # Treinar e avaliar
    results = train_and_evaluate(X_train, X_test, y_train, y_test)

    # Identificar melhor modelo
    best_name = min(results, key=lambda x: results[x]["mse"])

    print("\n" + "=" * 60)
    print(f"MELHOR MODELO: {best_name}")
    print(f"MSE: {results[best_name]['mse']:.4f}")
    print(f"R²:  {results[best_name]['r2']:.4f}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
