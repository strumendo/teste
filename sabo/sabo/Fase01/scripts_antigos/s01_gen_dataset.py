"""
S01 - Geração de Dataset
========================
Gera dataset sintético ou carrega dados reais para o pipeline de ML.

O QUE FAZ:
- Cria um DataFrame com estrutura padronizada para produção
- Gera dados sintéticos para testes (1000 amostras por padrão)
- Calcula o tempo restante para manutenção baseado na data de produção
- Exporta o dataset consolidado em CSV

QUANDO USAR:
- Primeira etapa do pipeline
- Para testes quando não há dados reais disponíveis
- Para validar a estrutura do pipeline

ENTRADA:
- Nenhuma (gera dados sintéticos) ou arquivo CSV com dados reais

SAÍDA:
- dados_manutencao.csv: Dataset consolidado pronto para ML
"""

import pandas as pd
import numpy as np
from pathlib import Path


def generate_synthetic_dataset(
    n_samples: int = 1000,
    output_path: str = "dados_manutencao.csv",
    equipamentos: list = None,
    produtos: list = None
) -> pd.DataFrame:
    """
    Gera um DataFrame sintético com a estrutura de dados proposta.

    Args:
        n_samples: Número de amostras a gerar
        output_path: Caminho para salvar o CSV
        equipamentos: Lista de códigos de equipamentos (opcional)
        produtos: Lista de códigos de produtos (opcional)

    Returns:
        DataFrame com os dados gerados
    """
    if equipamentos is None:
        equipamentos = ["IJ-046", "IJ-117", "IJ-118"]

    if produtos is None:
        produtos = ["SA05780", "SA02961", "SA02004"]

    df = pd.DataFrame({
        "Data de Produção Acumulada": pd.to_datetime(
            np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), size=n_samples)
        ),
        "Cod. Ordem": np.random.randint(1000000, 9999999, size=n_samples),
        "Cod Recurso": np.random.choice(equipamentos, size=n_samples),
        "Cod Produto": np.random.choice(produtos, size=n_samples),
        "Qt. Total Acumulada Produzida até a data específica": np.random.randint(1, 1000, size=n_samples),
        "Qt. Acumulada Refugada até a data específica": np.random.randint(1, 100, size=n_samples),
        "Qtd. Acumulada total Retrabalhada até a data específica": np.random.randint(1, 50, size=n_samples),
        "Fator Un.": np.ones(n_samples),
        "Cód. Un.": np.full(n_samples, "PC"),
        "Descrição da massa (Composto)": np.random.choice(["N-142/67", "P212/1"], size=n_samples),
        "Consumo total de Massa Acumulada": np.random.uniform(0.5, 1.5, size=n_samples),
    })

    # Calcular Tempo Restante para Manutenção (350 dias - dias desde produção)
    df["Tempo Restante para Manutenção"] = (
        350 - (pd.to_datetime("today") - df["Data de Produção Acumulada"]).dt.days
    )

    # Salvar o DataFrame
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"✓ Dataset salvo em '{output_path}'")

    return df


def load_real_dataset(filepath: str) -> pd.DataFrame:
    """
    Carrega um dataset real de um arquivo CSV.

    Args:
        filepath: Caminho para o arquivo CSV

    Returns:
        DataFrame com os dados carregados
    """
    df = pd.read_csv(filepath)
    print(f"✓ Dataset carregado de '{filepath}'")
    print(f"  Linhas: {len(df)}, Colunas: {len(df.columns)}")
    return df


def display_dataset_info(df: pd.DataFrame):
    """Exibe informações detalhadas sobre o dataset."""
    print("=" * 60)
    print("INFORMAÇÕES DO DATASET")
    print("=" * 60)

    print(f"\nDimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")

    print("\nPrimeiras 5 linhas:")
    print(df.head().to_markdown(index=False, numalign="left", stralign="left"))

    print("\nTipos de dados:")
    print(df.dtypes)

    print("\nValores únicos por coluna:")
    for col in df.columns:
        print(f"  {col}: {df[col].nunique()} valores únicos")

    print("\nEstatísticas descritivas (numéricas):")
    print(df.describe())


def main():
    """Função principal - executa geração de dataset."""
    print("=" * 60)
    print("S01 - GERAÇÃO DE DATASET")
    print("=" * 60)

    # Gerar dataset sintético
    df = generate_synthetic_dataset(
        n_samples=1000,
        output_path="dados_manutencao.csv"
    )

    # Exibir informações
    display_dataset_info(df)

    return df


if __name__ == "__main__":
    main()
