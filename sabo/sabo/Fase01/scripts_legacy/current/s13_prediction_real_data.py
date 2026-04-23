"""
S13 - Predição com Dados Reais
==============================
Treinamento e predição usando dados reais de múltiplos equipamentos.

O QUE FAZ:
- Carrega dados processados de múltiplos equipamentos
- Aplica one-hot encoding em variáveis categóricas
- Treina Linear Regression, Random Forest, Decision Tree e XGBoost
- Compara performance entre modelos
- Gera exemplos de predições

QUANDO USAR:
- Quando dados reais de produção estão disponíveis
- Para treinar modelos de produção
- Para validar o pipeline com dados reais

ENTRADA:
- mnt-oficial-{equipamento}.csv (gerados pelo S12)

SAÍDA:
- Comparativo de modelos
- Métricas de avaliação
- Exemplos de predições
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score
from pathlib import Path


def load_multiple_equipment(data_dir: str, equipamentos: list) -> pd.DataFrame:
    """
    Carrega e concatena dados de múltiplos equipamentos.

    Args:
        data_dir: Diretório com arquivos CSV
        equipamentos: Lista de códigos de equipamentos

    Returns:
        DataFrame consolidado
    """
    all_dfs = []

    for eq in equipamentos:
        filepath = Path(data_dir) / f"mnt-oficial-{eq}.csv"

        if filepath.exists():
            df = pd.read_csv(filepath)
            all_dfs.append(df)
            print(f"  ✓ {eq}: {len(df)} registros")
        else:
            print(f"  ⚠ {eq}: arquivo não encontrado")

    if all_dfs:
        merged = pd.concat(all_dfs, ignore_index=True)
        return merged

    return pd.DataFrame()


def preprocess_real_data(df: pd.DataFrame) -> tuple:
    """
    Pré-processa dados reais para ML.

    Operações:
    - One-hot encoding para variáveis categóricas
    - Separação de features e target

    Returns:
        X: Features
        y: Target (Manutencao)
    """
    # Identificar colunas categóricas
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

    # Remover colunas que não devem ser encoded
    cols_to_drop = ["Cód. Un."] if "Cód. Un." in df.columns else []

    # One-hot encoding
    df_encoded = pd.get_dummies(
        df,
        columns=[c for c in categorical_cols if c not in cols_to_drop]
    )

    # Remover colunas não numéricas restantes
    for col in cols_to_drop:
        if col in df_encoded.columns:
            df_encoded = df_encoded.drop(col, axis=1)

    # Separar features e target
    if "Manutencao" in df_encoded.columns:
        X = df_encoded.drop("Manutencao", axis=1)
        y = df_encoded["Manutencao"]
    else:
        raise ValueError("Coluna 'Manutencao' não encontrada")

    return X, y


def train_all_models(X_train, X_test, y_train, y_test) -> dict:
    """
    Treina múltiplos modelos e retorna resultados.

    Modelos:
    - Linear Regression
    - Decision Tree
    - Random Forest
    - XGBoost
    """
    models = {
        "Linear Regression": LinearRegression(),
        "Decision Tree": DecisionTreeRegressor(random_state=42),
        "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
        "XGBoost": xgb.XGBRegressor(random_state=42, objective="reg:squarederror"),
    }

    results = {}

    print("\n" + "-" * 50)
    print("TREINAMENTO DOS MODELOS")
    print("-" * 50)

    for name, model in models.items():
        print(f"\nTreinando {name}...")
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Score no treino
        train_score = model.score(X_train, y_train)

        results[name] = {
            "model": model,
            "mse": mse,
            "r2": r2,
            "train_score": train_score,
            "y_pred": y_pred
        }

        print(f"  Train Score: {train_score:.4f}")
        print(f"  Test MSE: {mse:.4f}")
        print(f"  Test R²: {r2:.4f}")

    return results


def print_predictions(y_test, y_pred, model_name: str, n_samples: int = 10):
    """Exibe exemplos de predições."""
    print(f"\nExemplos de Predições ({model_name}):")
    print("-" * 40)
    print(f"{'Real':>10} {'Predito':>10} {'Erro':>10}")
    print("-" * 40)

    indices = np.random.choice(len(y_test), min(n_samples, len(y_test)), replace=False)

    for idx in indices:
        real = y_test.iloc[idx] if hasattr(y_test, 'iloc') else y_test[idx]
        pred = y_pred[idx]
        erro = abs(real - pred)
        print(f"{real:>10} {pred:>10.2f} {erro:>10.2f}")


def get_feature_importance(model, feature_names: list) -> dict:
    """Extrai importância das features."""
    if hasattr(model, 'feature_importances_'):
        importance = dict(zip(feature_names, model.feature_importances_))
        return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    elif hasattr(model, 'coef_'):
        importance = dict(zip(feature_names, np.abs(model.coef_)))
        return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    return {}


def find_processed_directory() -> str:
    """
    Procura o diretório de dados processados automaticamente.

    Returns:
        Caminho do diretório de dados ou None
    """
    # Possíveis localizações do diretório de dados processados
    possible_dirs = [
        "../../../new_data/processed",
        "../../new_data/processed",
        "../new_data/processed",
        "./new_data/processed",
        "./processed",
        ".",
    ]

    for d in possible_dirs:
        p = Path(d)
        if p.exists():
            # Verificar se há arquivos mnt-oficial
            mnt_files = list(p.glob("mnt-oficial-*.csv"))
            if mnt_files:
                return str(p.resolve())

    return None


def main():
    """Função principal."""
    print("=" * 60)
    print("S13 - PREDIÇÃO COM DADOS REAIS")
    print("Target: Manutencao (dias)")
    print("=" * 60)

    # Todos os equipamentos conhecidos
    equipamentos = [
        "IJ-044", "IJ-046", "IJ-117", "IJ-118", "IJ-119", "IJ-120",
        "IJ-121", "IJ-122", "IJ-123", "IJ-124", "IJ-125", "IJ-129",
        "IJ-130", "IJ-131", "IJ-132", "IJ-133", "IJ-134", "IJ-135",
        "IJ-136", "IJ-137", "IJ-138", "IJ-139", "IJ-151", "IJ-152",
        "IJ-155", "IJ-156", "IJ-164"
    ]

    print("\nProcurando dados processados...")

    # Encontrar diretório de dados processados
    data_dir = find_processed_directory()

    if data_dir is None:
        print("\n⚠ Dados processados não encontrados.")
        print("Execute primeiro o script S12 para processar os dados:")
        print("  python s12_gen_real_data.py")
        return {}

    print(f"Diretório encontrado: {data_dir}")
    print("\nCarregando dados...")

    df = load_multiple_equipment(data_dir, equipamentos)

    if df.empty:
        print("\n⚠ Dados reais não encontrados.")
        print("Este script requer arquivos mnt-oficial-*.csv")
        print("Gerados pelo script S12.")
        return {}

    print(f"\n✓ Dataset consolidado: {len(df)} registros")

    # Pré-processar
    X, y = preprocess_real_data(df)
    print(f"Features: {X.shape[1]} colunas")

    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"Treino: {len(X_train)} | Teste: {len(X_test)}")

    # Treinar modelos
    results = train_all_models(X_train, X_test, y_train, y_test)

    # Ranking
    print("\n" + "=" * 60)
    print("RANKING (por R²)")
    print("=" * 60)

    sorted_results = sorted(results.items(), key=lambda x: x[1]["r2"], reverse=True)
    for i, (name, metrics) in enumerate(sorted_results, 1):
        print(f"{i}. {name}")
        print(f"   R²: {metrics['r2']:.4f} | MSE: {metrics['mse']:.4f}")

    # Melhor modelo
    best_name = sorted_results[0][0]
    best_metrics = sorted_results[0][1]

    print(f"\n🏆 MELHOR MODELO: {best_name}")
    print(f"   R²: {best_metrics['r2']:.4f}")
    print(f"   MSE: {best_metrics['mse']:.4f}")

    # Exemplos de predições do melhor modelo
    print_predictions(y_test, best_metrics["y_pred"], best_name)

    # Feature importance
    importance = get_feature_importance(best_metrics["model"], X.columns.tolist())
    if importance:
        print(f"\nTop 10 Features Importantes ({best_name}):")
        for i, (feat, imp) in enumerate(list(importance.items())[:10], 1):
            print(f"  {i}. {feat}: {imp:.4f}")

    print("\n" + "=" * 60)

    return results


if __name__ == "__main__":
    main()
