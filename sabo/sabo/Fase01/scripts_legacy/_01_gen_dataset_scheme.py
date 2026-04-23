"""
01. Gerar conjunto de dados com base na estrutura proposta à SABO
==================================================================
Gera um dataset sintético para testes do pipeline de ML.
O dataset real deve substituir este arquivo posteriormente.
"""

import pandas as pd
import numpy as np


def generate_synthetic_dataset(n_samples: int = 1000, output_path: str = "dados_manutencao.csv"):
    """
    Gera um DataFrame sintético com a estrutura de dados proposta.

    Args:
        n_samples: Número de amostras a gerar
        output_path: Caminho para salvar o CSV

    Returns:
        DataFrame com os dados gerados
    """
    df = pd.DataFrame({
        "Data de Produção Acumulada": pd.to_datetime(
            np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), size=n_samples)
        ),
        "Cod. Ordem": np.random.randint(1000000, 9999999, size=n_samples),
        "Cod Recurso": np.random.choice(["IJ-046", "IJ-117", "IJ-118"], size=n_samples),
        "Cod Produto": np.random.choice(["SA05780", "SA02961", "SA02004"], size=n_samples),
        "Qt. Total Acumulada Produzida até a data específica": np.random.randint(1, 1000, size=n_samples),
        "Qt. Acumulada Refugada até a data específica": np.random.randint(1, 100, size=n_samples),
        "Qtd. Acumulada total Retrabalhada até a data específica": np.random.randint(1, 50, size=n_samples),
        "Fator Un.": np.ones(n_samples),
        "Cód. Un.": np.full(n_samples, "PC"),
        "Descrição da massa (Composto)": np.random.choice(["N-142/67", "P212/1"], size=n_samples),
        "Consumo total de Massa Acumulada": np.random.uniform(0.5, 1.5, size=n_samples),
    })

    # Calcular Tempo Restante para Manutenção
    df["Tempo Restante para Manutenção"] = (
        350 - (pd.to_datetime("today") - df["Data de Produção Acumulada"]).dt.days
    )

    # Salvar o DataFrame
    df.to_csv(output_path, index=False)

    return df


def display_dataset_info(df: pd.DataFrame):
    """Exibe informações sobre o dataset."""
    print("=" * 60)
    print("INFORMAÇÕES DO DATASET GERADO")
    print("=" * 60)

    print("\nPrimeiras 5 linhas:")
    print(df.head().to_markdown(index=False, numalign="left", stralign="left"))

    print("\nÚltimas 5 linhas:")
    print(df.tail().to_markdown(index=False, numalign="left", stralign="left"))

    print("\nInformações do DataFrame:")
    print(f"Total de linhas: {len(df)}")
    print(f"Total de colunas: {len(df.columns)}")

    print("\nNúmero de valores únicos por coluna:")
    print(df.nunique())

    print("\nTipos de dados:")
    print(df.dtypes)


if __name__ == "__main__":
    # Gerar dataset
    df = generate_synthetic_dataset(n_samples=1000, output_path="dados_manutencao.csv")

    # Exibir informações
    display_dataset_info(df)

    print("\n✓ Dataset salvo em 'dados_manutencao.csv'")
