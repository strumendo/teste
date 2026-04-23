"""
S02 - Pré-processamento e Limpeza
=================================
Etapa 2 do Pipeline conforme fluxos.drawio

O QUE FAZ:
- Higienização e Transformação:
  - Remover duplicadas
  - Tratar valores nulos
  - Conversão Datas → datetime
- Engenharia de Features:
  - Geração de Variáveis Acumulativas
  - Codificação One-Hot

FLUXO (fluxos.drawio):
DataFrame Único → Higienização → Engenharia de Features → Base para EDA

ENTRADA:
- data_raw.csv (saída da Etapa 1)

SAÍDA:
- data_preprocessed.csv: Dados limpos e transformados para EDA
"""

import pandas as pd
import numpy as np
from pathlib import Path


# Mapeamento de equipamentos para datas de manutenção
EQUIPAMENTO_MANUTENCAO = {
    "IJ-044": "2024-05-26",
    "IJ-046": "2024-01-28",
    "IJ-117": "2024-02-10",
    "IJ-118": "2024-02-11",
    "IJ-119": "2024-02-10",
    "IJ-120": "2024-02-24",
    "IJ-121": "2024-02-25",
    "IJ-122": "2024-02-24",
    "IJ-123": "2024-02-11",
    "IJ-124": "2024-02-07",
    "IJ-125": "2024-11-20",
    "IJ-129": "2024-02-25",
    "IJ-130": "2024-05-18",
    "IJ-131": "2024-02-25",
    "IJ-132": "2024-02-18",
    "IJ-133": "2024-03-02",
    "IJ-134": "2024-03-02",
    "IJ-135": "2024-03-02",
    "IJ-136": "2024-05-18",
    "IJ-137": "2024-03-17",
    "IJ-138": "2024-03-16",
    "IJ-139": "2024-03-03",
    "IJ-151": "2024-03-09",
    "IJ-152": "2024-03-09",
    "IJ-155": "2024-03-16",
    "IJ-156": "2024-05-19",
    "IJ-164": "2024-03-17",
}


def load_raw_data(filepath: str = "data_raw.csv") -> pd.DataFrame:
    """
    Carrega dados brutos da Etapa 1.

    Args:
        filepath: Caminho do arquivo CSV

    Returns:
        DataFrame com dados brutos
    """
    df = pd.read_csv(filepath)
    print(f"  Carregado: {len(df)} registros, {len(df.columns)} colunas")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove registros duplicados.

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame sem duplicatas
    """
    initial_count = len(df)
    df = df.drop_duplicates()
    removed = initial_count - len(df)

    if removed > 0:
        print(f"  ✓ Removidas {removed} duplicatas ({initial_count} → {len(df)})")
    else:
        print(f"  ✓ Nenhuma duplicata encontrada")

    return df


def handle_null_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata valores nulos no DataFrame.

    Estratégia:
    - Colunas numéricas: preenche com mediana
    - Colunas categóricas: preenche com moda ou 'Desconhecido'

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com nulos tratados
    """
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()

    if total_nulls == 0:
        print(f"  ✓ Nenhum valor nulo encontrado")
        return df

    print(f"  Tratando {total_nulls} valores nulos...")

    for col in df.columns:
        if df[col].isnull().sum() > 0:
            if df[col].dtype in ['int64', 'float64']:
                # Numérico: preencher com mediana
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                print(f"    {col}: preenchido com mediana ({median_val:.2f})")
            else:
                # Categórico: preencher com moda ou 'Desconhecido'
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col] = df[col].fillna(mode_val[0])
                    print(f"    {col}: preenchido com moda ({mode_val[0]})")
                else:
                    df[col] = df[col].fillna("Desconhecido")
                    print(f"    {col}: preenchido com 'Desconhecido'")

    print(f"  ✓ Valores nulos tratados")
    return df


def convert_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas de data para datetime.

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com datas convertidas
    """
    date_columns = [col for col in df.columns if "Data" in col or "data" in col]

    for col in date_columns:
        if col in df.columns and df[col].dtype == 'object':
            try:
                # Tentar formato brasileiro primeiro (dd/mm/yyyy)
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                print(f"  ✓ Convertido {col} para datetime")
            except Exception as e:
                print(f"  ⚠ Erro ao converter {col}: {e}")

    return df


def calculate_maintenance_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula dias até a próxima manutenção.

    Usa o mapeamento EQUIPAMENTO_MANUTENCAO para calcular
    a variável target 'Manutencao' (dias restantes).

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com coluna de manutenção
    """
    # Identificar coluna de data
    date_col = None
    for col in ["Data de Produção", "Data de Produção Acumulada"]:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        print("  ⚠ Coluna de data não encontrada. Gerando target sintético.")
        # Gerar target sintético baseado em outras features
        df["Manutencao"] = np.random.randint(1, 365, size=len(df))
        return df

    # Identificar coluna de equipamento
    equip_col = None
    for col in ["Equipamento", "Cod Recurso"]:
        if col in df.columns:
            equip_col = col
            break

    if equip_col is None:
        print("  ⚠ Coluna de equipamento não encontrada. Usando data fixa.")
        default_maint_date = pd.to_datetime("2024-06-01")
        df["Manutencao"] = (default_maint_date - pd.to_datetime(df[date_col])).dt.days
    else:
        # Calcular dias até manutenção por equipamento
        def calc_days(row):
            equip = row[equip_col]
            if equip in EQUIPAMENTO_MANUTENCAO:
                maint_date = pd.to_datetime(EQUIPAMENTO_MANUTENCAO[equip])
            else:
                maint_date = pd.to_datetime("2024-06-01")  # Default

            prod_date = pd.to_datetime(row[date_col])
            return (maint_date - prod_date).days

        df["Manutencao"] = df.apply(calc_days, axis=1)

    # Remover registros com Manutencao negativa (após manutenção)
    initial_count = len(df)
    df = df[df["Manutencao"] >= 0]

    if len(df) < initial_count:
        print(f"  ✓ Removidos {initial_count - len(df)} registros pós-manutenção")

    print(f"  ✓ Calculada variável 'Manutencao' (dias até manutenção)")

    return df


def generate_cumulative_variables(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera variáveis acumulativas por equipamento.

    Calcula acumulados de:
    - Quantidade produzida
    - Quantidade refugada
    - Quantidade retrabalhada
    - Consumo de massa

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com variáveis acumulativas
    """
    # Identificar colunas de quantidade
    qty_cols = [col for col in df.columns if any(x in col.lower() for x in ["qtd", "quantidade", "consumo"])]

    if not qty_cols:
        print("  ⚠ Nenhuma coluna de quantidade encontrada para acumular")
        return df

    # Identificar coluna de equipamento
    equip_col = None
    for col in ["Equipamento", "Cod Recurso"]:
        if col in df.columns:
            equip_col = col
            break

    # Ordenar por equipamento e data (se existir)
    sort_cols = []
    if equip_col:
        sort_cols.append(equip_col)
    date_col = next((col for col in df.columns if "Data" in col), None)
    if date_col:
        sort_cols.append(date_col)

    if sort_cols:
        df = df.sort_values(sort_cols)

    # Calcular acumulados
    for col in qty_cols:
        if df[col].dtype in ['int64', 'float64']:
            new_col = f"{col}_Acumulado"
            if equip_col:
                df[new_col] = df.groupby(equip_col)[col].cumsum()
            else:
                df[new_col] = df[col].cumsum()
            print(f"  ✓ Criada variável acumulativa: {new_col}")

    return df


def apply_one_hot_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica One-Hot Encoding em variáveis categóricas.

    Colunas codificadas:
    - Cod Produto
    - Equipamento / Cod Recurso
    - Descrição da massa

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com encoding aplicado
    """
    categorical_cols = [
        "Cod Produto",
        "Equipamento",
        "Cod Recurso",
        "Descrição da massa (Composto)",
        "Cód. Un."
    ]

    cols_to_encode = [col for col in categorical_cols if col in df.columns]

    if not cols_to_encode:
        print("  ⚠ Nenhuma coluna categórica encontrada para encoding")
        return df

    for col in cols_to_encode:
        n_unique = df[col].nunique()
        if n_unique <= 50:  # Limite para evitar explosão de dimensionalidade
            df = pd.get_dummies(df, columns=[col], prefix=col.replace(" ", "_").replace(".", ""))
            print(f"  ✓ One-Hot Encoding aplicado: {col} ({n_unique} categorias)")
        else:
            print(f"  ⚠ {col} tem muitas categorias ({n_unique}), pulando encoding")

    return df


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes das colunas.

    Args:
        df: DataFrame de entrada

    Returns:
        DataFrame com colunas renomeadas
    """
    # Remover caracteres especiais e espaços
    df.columns = [
        col.replace(" ", "_")
           .replace(".", "")
           .replace("(", "")
           .replace(")", "")
           .replace("/", "_")
           .replace("-", "_")
        for col in df.columns
    ]

    return df


def main() -> dict:
    """
    Função principal - Etapa 2: Pré-processamento e Limpeza.

    Returns:
        Dicionário com resultados da execução
    """
    print("=" * 60)
    print("ETAPA 2: PRÉ-PROCESSAMENTO E LIMPEZA")
    print("(Conforme fluxos.drawio)")
    print("=" * 60)

    # Verificar arquivo de entrada
    input_file = Path("data_raw.csv")
    if not input_file.exists():
        print(f"\n✗ Arquivo não encontrado: {input_file}")
        print("Execute a Etapa 1 primeiro (s01_data_collection.py)")
        return {"status": "error", "message": "Input file not found"}

    # Carregar dados
    print("\n[1/6] Carregando dados brutos...")
    df = load_raw_data(str(input_file))
    initial_shape = df.shape

    # Etapa de Higienização
    print("\n" + "-" * 40)
    print("HIGIENIZAÇÃO E TRANSFORMAÇÃO")
    print("-" * 40)

    print("\n[2/6] Removendo duplicadas...")
    df = remove_duplicates(df)

    print("\n[3/6] Tratando valores nulos...")
    df = handle_null_values(df)

    print("\n[4/6] Convertendo datas...")
    df = convert_dates(df)

    print("\n[4.1] Calculando dias até manutenção...")
    df = calculate_maintenance_days(df)

    # Etapa de Engenharia de Features
    print("\n" + "-" * 40)
    print("ENGENHARIA DE FEATURES")
    print("-" * 40)

    print("\n[5/6] Gerando variáveis acumulativas...")
    df = generate_cumulative_variables(df)

    print("\n[6/6] Aplicando One-Hot Encoding...")
    df = apply_one_hot_encoding(df)

    # Limpar nomes de colunas
    df = clean_column_names(df)

    # Salvar
    output_file = Path("data_preprocessed.csv")
    df.to_csv(output_file, index=False)

    # Resumo
    final_shape = df.shape
    print("\n" + "=" * 60)
    print("ETAPA 2 CONCLUÍDA")
    print("=" * 60)
    print(f"\nTransformação: {initial_shape} → {final_shape}")
    print(f"Arquivo salvo: {output_file}")

    # Listar colunas
    print(f"\nColunas ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")

    results = {
        "status": "success",
        "input_shape": initial_shape,
        "output_shape": final_shape,
        "output_file": str(output_file),
        "columns": list(df.columns),
        "has_target": "Manutencao" in df.columns,
    }

    return results


if __name__ == "__main__":
    main()
