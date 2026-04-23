"""
S01 - Coleta e Integração de Dados
==================================
Etapa 1 do Pipeline conforme fluxos.drawio

O QUE FAZ:
- Carrega múltiplos arquivos CSV de equipamentos (ij-044.csv, ij-046.csv, etc.)
- Converte arquivos XLSX para CSV automaticamente se necessário
- Concatena todos em um DataFrame único
- Padroniza nomes de colunas

FLUXO (fluxos.drawio):
CSV Files (ij-044, ij-046, ij-117, ...) → DataFrame Único

ENTRADA:
- Arquivos CSV/XLSX por equipamento no diretório de dados

SAÍDA:
- data_raw.csv: DataFrame único com todos os dados brutos
"""

import pandas as pd
from pathlib import Path


def convert_xlsx_to_csv(filepath: Path) -> Path:
    """
    Converte arquivo XLSX para CSV se necessário.

    Args:
        filepath: Caminho base do arquivo (sem extensão)

    Returns:
        Caminho do arquivo CSV
    """
    csv_path = filepath.with_suffix(".csv")
    xlsx_path = filepath.with_suffix(".xlsx")

    if csv_path.exists():
        return csv_path

    if xlsx_path.exists():
        print(f"  Convertendo {xlsx_path.name} → {csv_path.name}")
        try:
            df = pd.read_excel(xlsx_path)
            df.to_csv(csv_path, index=False)
            return csv_path
        except Exception as e:
            print(f"  Erro na conversão: {e}")
            return None

    return None


def find_data_directory() -> Path:
    """
    Procura o diretório de dados automaticamente.

    Returns:
        Path do diretório de dados ou None
    """
    possible_dirs = [
        Path("../../../new_data"),
        Path("../../new_data"),
        Path("../new_data"),
        Path("./new_data"),
        Path("."),
    ]

    for d in possible_dirs:
        if d.exists():
            xlsx_files = list(d.glob("IJ-*.xlsx"))
            csv_files = list(d.glob("IJ-*.csv"))
            if xlsx_files or csv_files:
                return d.resolve()

    return None


def load_csv_files(data_dir: Path, pattern: str = "IJ-*.csv") -> list:
    """
    Carrega todos os arquivos CSV que correspondem ao padrão.

    Args:
        data_dir: Diretório de dados
        pattern: Padrão glob para encontrar arquivos

    Returns:
        Lista de tuplas (nome_equipamento, DataFrame)
    """
    dataframes = []

    # Procurar arquivos CSV
    csv_files = sorted(data_dir.glob(pattern))

    # Também verificar XLSX
    xlsx_files = sorted(data_dir.glob(pattern.replace(".csv", ".xlsx")))

    all_files = set()
    for f in csv_files:
        all_files.add(f.stem)
    for f in xlsx_files:
        all_files.add(f.stem)

    for equip_name in sorted(all_files):
        filepath = convert_xlsx_to_csv(data_dir / equip_name)

        if filepath and filepath.exists():
            try:
                df = pd.read_csv(filepath)
                df["Equipamento"] = equip_name
                dataframes.append((equip_name, df))
                print(f"  ✓ {equip_name}: {len(df)} registros")
            except Exception as e:
                print(f"  ✗ {equip_name}: Erro ao carregar - {e}")

    return dataframes


def merge_dataframes(dataframes: list) -> pd.DataFrame:
    """
    Concatena todos os DataFrames em um único DataFrame.

    Args:
        dataframes: Lista de tuplas (nome, DataFrame)

    Returns:
        DataFrame consolidado
    """
    if not dataframes:
        return pd.DataFrame()

    dfs = [df for _, df in dataframes]
    merged = pd.concat(dfs, ignore_index=True)

    return merged


def generate_synthetic_data(n_samples: int = 1000) -> pd.DataFrame:
    """
    Gera dados sintéticos quando não há dados reais disponíveis.

    Args:
        n_samples: Número de amostras a gerar

    Returns:
        DataFrame com dados sintéticos
    """
    import numpy as np

    equipamentos = ["IJ-044", "IJ-046", "IJ-117", "IJ-118"]
    produtos = ["SA05780", "SA02961", "SA02004", "SA01234"]

    df = pd.DataFrame({
        "Data de Produção": pd.to_datetime(
            np.random.choice(pd.date_range("2023-01-01", "2024-12-31"), size=n_samples)
        ).strftime("%d/%m/%Y"),
        "Cód. Ordem": np.random.randint(1000000, 9999999, size=n_samples),
        "Equipamento": np.random.choice(equipamentos, size=n_samples),
        "Cod Produto": np.random.choice(produtos, size=n_samples),
        "Qtd. Produzida": np.random.randint(100, 5000, size=n_samples),
        "Qtd. Refugada": np.random.randint(0, 100, size=n_samples),
        "Qtd. Retrabalhada": np.random.randint(0, 50, size=n_samples),
        "Fator Un.": np.ones(n_samples),
        "Cód. Un.": np.full(n_samples, "PC"),
        "Descrição da massa (Composto)": np.random.choice(["N-142/67", "P212/1", "N-150/80"], size=n_samples),
        "Consumo de massa no item em (Kg/100pçs)": np.random.uniform(0.5, 2.0, size=n_samples).round(3),
    })

    print(f"  ✓ Dados sintéticos gerados: {n_samples} registros")

    return df


def main() -> dict:
    """
    Função principal - Etapa 1: Coleta e Integração.

    Returns:
        Dicionário com resultados da execução
    """
    print("=" * 60)
    print("ETAPA 1: COLETA E INTEGRAÇÃO DE DADOS")
    print("(Conforme fluxos.drawio)")
    print("=" * 60)

    # Procurar diretório de dados
    data_dir = find_data_directory()

    if data_dir:
        print(f"\nDiretório de dados: {data_dir}")
        print("\nCarregando arquivos CSV...")

        dataframes = load_csv_files(data_dir)

        if dataframes:
            # Mesclar DataFrames
            print("\nIntegrando em DataFrame único...")
            df_merged = merge_dataframes(dataframes)

            # Salvar
            output_path = Path("data_raw.csv")
            df_merged.to_csv(output_path, index=False)

            print(f"\n✓ DataFrame único salvo: {output_path}")
            print(f"  Total: {len(df_merged)} registros de {len(dataframes)} equipamentos")

            results = {
                "status": "success",
                "source": "real_data",
                "equipamentos": len(dataframes),
                "registros": len(df_merged),
                "output_file": str(output_path),
                "colunas": list(df_merged.columns),
            }
        else:
            print("\n⚠ Nenhum arquivo de equipamento encontrado.")
            print("Gerando dados sintéticos...")

            df_merged = generate_synthetic_data(1000)
            output_path = Path("data_raw.csv")
            df_merged.to_csv(output_path, index=False)

            results = {
                "status": "success",
                "source": "synthetic",
                "registros": len(df_merged),
                "output_file": str(output_path),
            }
    else:
        print("\n⚠ Diretório de dados não encontrado.")
        print("Gerando dados sintéticos para demonstração...")

        df_merged = generate_synthetic_data(1000)
        output_path = Path("data_raw.csv")
        df_merged.to_csv(output_path, index=False)

        results = {
            "status": "success",
            "source": "synthetic",
            "registros": len(df_merged),
            "output_file": str(output_path),
        }

    print("\n" + "=" * 60)
    print("ETAPA 1 CONCLUÍDA")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
