"""
S12 - Geração de Dados Reais por Equipamento
============================================
Processa dados reais de produção por equipamento.

O QUE FAZ:
- Detecta e converte arquivos .xlsx para .csv automaticamente
- Carrega dados CSV de cada equipamento (IJ-044, IJ-046, etc.)
- Calcula dias até a manutenção baseado na data real
- Agrupa por dia de manutenção
- Calcula valores acumulados (produção, refugo, retrabalho)
- Gera arquivos processados para cada equipamento

QUANDO USAR:
- Quando dados reais de produção estão disponíveis
- Para preparar dados históricos para treinamento
- Para consolidar dados de múltiplos equipamentos

ENTRADA:
- Arquivos CSV ou XLSX por equipamento (ex: IJ-044.csv ou IJ-044.xlsx)
- Datas de manutenção por equipamento

SAÍDA:
- mnt-{equipamento}.csv: Dados com dias para manutenção
- mnt-grouped-{equipamento}.csv: Agrupado por dia
- mnt-oficial-{equipamento}.csv: Com acumulados totais
"""

import pandas as pd
from pathlib import Path


def convert_xlsx_if_needed(filepath: Path) -> Path:
    """
    Converte arquivo xlsx para csv se necessário.

    Args:
        filepath: Caminho do arquivo (pode ser .csv ou .xlsx)

    Returns:
        Caminho do arquivo .csv (convertido ou original)
    """
    csv_path = filepath.with_suffix(".csv")
    xlsx_path = filepath.with_suffix(".xlsx")

    # Se CSV existe, usar ele
    if csv_path.exists():
        return csv_path

    # Se XLSX existe, converter
    if xlsx_path.exists():
        print(f"  Convertendo {xlsx_path.name} -> {csv_path.name}")
        try:
            df = pd.read_excel(xlsx_path)
            df.to_csv(csv_path, index=False)
            return csv_path
        except ValueError as e:
            # Tentar ler com engine alternativo se houver erro de data
            if "Invalid datetime" in str(e):
                print(f"  Tentando engine alternativo...")
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
                    ws = wb.active
                    data = list(ws.values)
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df.to_csv(csv_path, index=False)
                    return csv_path
                except Exception as e2:
                    print(f"  Erro na conversão alternativa: {e2}")
                    print(f"  AVISO: Arquivo {xlsx_path.name} precisa ser corrigido manualmente")
                    return None
            else:
                print(f"  Erro na conversão: {e}")
                return None
        except Exception as e:
            print(f"  Erro na conversão: {e}")
            return None

    return None


# Mapeamento de equipamentos para datas de manutenção
EQUIPAMENTO_DATES = {
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


def process_equipment_data(
    filepath: str,
    equipamento: str,
    data_manutencao: str,
    output_dir: str = "."
) -> pd.DataFrame:
    """
    Processa dados de um equipamento específico.

    Args:
        filepath: Caminho para o CSV do equipamento
        equipamento: Código do equipamento (ex: IJ-044)
        data_manutencao: Data da próxima manutenção (YYYY-MM-DD)
        output_dir: Diretório para salvar os arquivos processados

    Returns:
        DataFrame processado com acumulados
    """
    print(f"\nProcessando {equipamento}...")

    # Carregar dados
    df = pd.read_csv(filepath)

    # Converter data de produção (formato brasileiro: dd/mm/yyyy)
    df["Data de Produção"] = pd.to_datetime(df["Data de Produção"], dayfirst=True)

    # Calcular dias até manutenção
    df["Manutencao"] = (
        pd.to_datetime(data_manutencao) - df["Data de Produção"]
    ).dt.days

    # Remover coluna de data original
    df = df.drop("Data de Produção", axis=1)

    # Salvar versão básica
    df.to_csv(f"{output_dir}/mnt-{equipamento}.csv", index=False)

    # Ordenar por dias de manutenção
    df.sort_values(by="Manutencao", inplace=True)

    # Converter consumo de massa para numérico
    if "Consumo de massa no item em (Kg/100pçs)" in df.columns:
        df["Consumo de massa no item em (Kg/100pçs)"] = (
            df["Consumo de massa no item em (Kg/100pçs)"]
            .astype(str)
            .str.replace("[^0-9.]", "", regex=True)
        )
        df["Consumo de massa no item em (Kg/100pçs)"] = pd.to_numeric(
            df["Consumo de massa no item em (Kg/100pçs)"], errors="coerce"
        )

    # Calcular acumulados por dia de manutenção
    cols_to_cumsum = ["Qtd. Produzida", "Qtd. Refugada", "Qtd. Retrabalhada", "Fator Un."]
    for col in cols_to_cumsum:
        if col in df.columns:
            df[col] = df.groupby("Manutencao")[col].cumsum()

    if "Consumo de massa no item em (Kg/100pçs)" in df.columns:
        df["Consumo de massa no item em (Kg/100pçs)"] = df.groupby("Manutencao")[
            "Consumo de massa no item em (Kg/100pçs)"
        ].cumsum()

    # Agrupar por dia de manutenção (pegar último valor de cada dia)
    df_grouped = df.groupby("Manutencao").last().reset_index()

    # Reordenar colunas (Manutencao no final)
    cols = df_grouped.columns.tolist()
    cols.remove("Manutencao")
    cols.append("Manutencao")
    df_grouped = df_grouped[cols]

    # Ordenar por manutenção decrescente
    df_grouped = df_grouped.sort_values(by="Manutencao", ascending=False).reset_index(drop=True)

    # Salvar versão agrupada
    df_grouped.to_csv(f"{output_dir}/mnt-grouped-{equipamento}.csv", index=False)

    # Calcular acumulados totais
    df_result = df_grouped.copy()

    if "Qtd. Produzida" in df_result.columns:
        df_result["Qtd. Produzida Acumulada Total"] = df_result["Qtd. Produzida"].cumsum()
    if "Qtd. Refugada" in df_result.columns:
        df_result["Qtd. Refugada Acumulada Total"] = df_result["Qtd. Refugada"].cumsum()
    if "Qtd. Retrabalhada" in df_result.columns:
        df_result["Qtd. Retrabalhada Acumulada Total"] = df_result["Qtd. Retrabalhada"].cumsum()
    if "Fator Un." in df_result.columns:
        df_result["Fator Un. Acumulado Total"] = df_result["Fator Un."].cumsum()
    if "Consumo de massa no item em (Kg/100pçs)" in df_result.columns:
        df_result["Consumo de massa no item em (Kg/100pçs) Acumulado Total"] = (
            df_result["Consumo de massa no item em (Kg/100pçs)"].cumsum()
        )

    # Reordenar (Manutencao no final)
    cols = df_result.columns.tolist()
    if "Manutencao" in cols:
        cols.remove("Manutencao")
        cols.append("Manutencao")
        df_result = df_result[cols]

    # Salvar versão oficial
    df_result.to_csv(f"{output_dir}/mnt-oficial-{equipamento}.csv", index=False)

    print(f"  ✓ {len(df_result)} registros processados")

    return df_result


def process_all_equipment(input_dir: str, output_dir: str = ".") -> dict:
    """
    Processa todos os equipamentos disponíveis.

    Detecta automaticamente arquivos .csv ou .xlsx e converte se necessário.

    Args:
        input_dir: Diretório com arquivos CSV/XLSX dos equipamentos
        output_dir: Diretório para salvar arquivos processados

    Returns:
        Dicionário com DataFrames processados
    """
    results = {}
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    # Criar diretório de saída se necessário
    output_path.mkdir(parents=True, exist_ok=True)

    for equipamento, data_manut in EQUIPAMENTO_DATES.items():
        # Tentar encontrar arquivo (CSV ou XLSX)
        filepath = convert_xlsx_if_needed(input_path / equipamento)

        if filepath and filepath.exists():
            df = process_equipment_data(
                str(filepath),
                equipamento,
                data_manut,
                str(output_path)
            )
            results[equipamento] = df
        else:
            print(f"  ⚠ Arquivo não encontrado: {equipamento}.csv ou .xlsx")

    return results


def merge_all_equipment(data_dir: str = ".") -> pd.DataFrame:
    """
    Concatena dados de todos os equipamentos.

    Args:
        data_dir: Diretório com arquivos mnt-oficial-*.csv

    Returns:
        DataFrame consolidado
    """
    all_dfs = []

    for equipamento in EQUIPAMENTO_DATES.keys():
        filepath = Path(data_dir) / f"mnt-oficial-{equipamento}.csv"

        if filepath.exists():
            df = pd.read_csv(filepath)
            all_dfs.append(df)

    if all_dfs:
        merged = pd.concat(all_dfs, ignore_index=True)
        print(f"\n✓ Dataset consolidado: {len(merged)} registros de {len(all_dfs)} equipamentos")
        return merged

    return pd.DataFrame()


def find_data_directory() -> str:
    """
    Procura o diretório de dados automaticamente.

    Returns:
        Caminho do diretório de dados ou None
    """
    # Possíveis localizações do diretório de dados
    possible_dirs = [
        "../../../new_data",
        "../../new_data",
        "../new_data",
        "./new_data",
        ".",
    ]

    for d in possible_dirs:
        p = Path(d)
        if p.exists():
            # Verificar se há arquivos de equipamento
            xlsx_files = list(p.glob("IJ-*.xlsx"))
            csv_files = list(p.glob("IJ-*.csv"))
            if xlsx_files or csv_files:
                return str(p.resolve())

    return None


def main():
    """Função principal."""
    print("=" * 60)
    print("S12 - GERAÇÃO DE DADOS REAIS POR EQUIPAMENTO")
    print("=" * 60)

    print(f"\nEquipamentos configurados: {len(EQUIPAMENTO_DATES)}")

    # Encontrar diretório de dados
    data_dir = find_data_directory()

    if data_dir is None:
        print("\n⚠ Diretório de dados não encontrado.")
        print("Procurado em: new_data/, ./")
        print("\nEquipamentos esperados:")
        for eq, data in EQUIPAMENTO_DATES.items():
            print(f"  {eq}: Manutenção em {data}")
        return {}

    print(f"\nDiretório de dados: {data_dir}")

    # Criar diretório de saída
    output_dir = Path(data_dir) / "processed"
    output_dir.mkdir(exist_ok=True)

    print(f"Diretório de saída: {output_dir}")

    # Processar todos os equipamentos
    print("\n" + "-" * 60)
    print("PROCESSANDO EQUIPAMENTOS")
    print("-" * 60)

    results = process_all_equipment(data_dir, str(output_dir))

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"Equipamentos processados: {len(results)}")
    print(f"Equipamentos configurados: {len(EQUIPAMENTO_DATES)}")

    if results:
        total_records = sum(len(df) for df in results.values())
        print(f"Total de registros: {total_records}")

        # Consolidar
        print("\nConsolidando dados...")
        merged = merge_all_equipment(str(output_dir))

        if not merged.empty:
            merged_path = output_dir / "mnt-oficial-all.csv"
            merged.to_csv(merged_path, index=False)
            print(f"Dataset consolidado salvo: {merged_path}")

    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
