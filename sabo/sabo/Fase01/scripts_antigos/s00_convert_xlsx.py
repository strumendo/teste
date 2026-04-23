"""
S00 - Conversor Excel para CSV
==============================
Converte arquivos .xlsx para .csv automaticamente.

O QUE FAZ:
- Detecta arquivos .xlsx em um diretório
- Converte para .csv preservando estrutura
- Suporta múltiplas planilhas (usa a primeira por padrão)
- Gera relatório de conversão

QUANDO USAR:
- Antes de executar S12 (dados reais)
- Quando receber novos dados em Excel
- Para padronizar formato de entrada

ENTRADA:
- Arquivos .xlsx (ex: IJ-044.xlsx, IJ-046.xlsx)

SAÍDA:
- Arquivos .csv correspondentes (ex: IJ-044.csv, IJ-046.csv)

DEPENDÊNCIAS:
- openpyxl (pip install openpyxl)
"""

import pandas as pd
from pathlib import Path


def convert_xlsx_to_csv(
    input_path: str,
    output_path: str = None,
    sheet_name: int = 0
) -> str:
    """
    Converte um arquivo Excel para CSV.

    Args:
        input_path: Caminho do arquivo .xlsx
        output_path: Caminho do arquivo .csv (opcional, usa mesmo nome)
        sheet_name: Índice da planilha (0 = primeira)

    Returns:
        Caminho do arquivo CSV gerado
    """
    input_file = Path(input_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")

    if input_file.suffix.lower() != ".xlsx":
        raise ValueError(f"Arquivo não é .xlsx: {input_path}")

    # Definir caminho de saída
    if output_path is None:
        output_path = input_file.with_suffix(".csv")
    else:
        output_path = Path(output_path)

    # Ler Excel
    df = pd.read_excel(input_path, sheet_name=sheet_name)

    # Salvar CSV
    df.to_csv(output_path, index=False)

    return str(output_path)


def convert_directory(
    input_dir: str,
    output_dir: str = None,
    pattern: str = "*.xlsx"
) -> dict:
    """
    Converte todos os arquivos Excel de um diretório.

    Args:
        input_dir: Diretório com arquivos .xlsx
        output_dir: Diretório de saída (opcional, usa mesmo)
        pattern: Padrão de arquivos (default: *.xlsx)

    Returns:
        Dicionário com resultados {arquivo: status}
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir else input_path

    if not input_path.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {input_dir}")

    # Criar diretório de saída se necessário
    output_path.mkdir(parents=True, exist_ok=True)

    # Encontrar arquivos
    xlsx_files = list(input_path.glob(pattern))

    if not xlsx_files:
        print(f"Nenhum arquivo {pattern} encontrado em {input_dir}")
        return {}

    results = {}

    print(f"\nConvertendo {len(xlsx_files)} arquivos...")
    print("-" * 50)

    for xlsx_file in sorted(xlsx_files):
        csv_file = output_path / xlsx_file.with_suffix(".csv").name

        try:
            df = pd.read_excel(xlsx_file)
            df.to_csv(csv_file, index=False)

            results[xlsx_file.name] = {
                "status": "success",
                "output": str(csv_file),
                "rows": len(df),
                "columns": len(df.columns)
            }

            print(f"  OK  {xlsx_file.name} -> {csv_file.name} ({len(df)} linhas)")

        except Exception as e:
            results[xlsx_file.name] = {
                "status": "error",
                "error": str(e)
            }
            print(f"  ERRO  {xlsx_file.name}: {e}")

    return results


def check_and_convert(
    data_dir: str,
    equipamentos: list = None
) -> dict:
    """
    Verifica e converte arquivos conforme necessário.

    Se arquivo .csv existe, não converte.
    Se apenas .xlsx existe, converte.

    Args:
        data_dir: Diretório de dados
        equipamentos: Lista de equipamentos (opcional)

    Returns:
        Dicionário com status de cada equipamento
    """
    data_path = Path(data_dir)
    results = {}

    # Se não especificou equipamentos, detectar automaticamente
    if equipamentos is None:
        xlsx_files = list(data_path.glob("IJ-*.xlsx"))
        equipamentos = [f.stem for f in xlsx_files]

    print(f"\nVerificando {len(equipamentos)} equipamentos...")
    print("-" * 50)

    for eq in equipamentos:
        csv_file = data_path / f"{eq}.csv"
        xlsx_file = data_path / f"{eq}.xlsx"

        if csv_file.exists():
            # CSV já existe
            df = pd.read_csv(csv_file)
            results[eq] = {
                "status": "exists",
                "file": str(csv_file),
                "rows": len(df)
            }
            print(f"  CSV  {eq}.csv já existe ({len(df)} linhas)")

        elif xlsx_file.exists():
            # Converter Excel para CSV
            try:
                df = pd.read_excel(xlsx_file)
                df.to_csv(csv_file, index=False)

                results[eq] = {
                    "status": "converted",
                    "file": str(csv_file),
                    "rows": len(df)
                }
                print(f"  CONV {eq}.xlsx -> {eq}.csv ({len(df)} linhas)")

            except Exception as e:
                results[eq] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"  ERRO {eq}: {e}")

        else:
            results[eq] = {
                "status": "not_found"
            }
            print(f"  N/A  {eq}: nenhum arquivo encontrado")

    return results


def main():
    """Função principal."""
    print("=" * 60)
    print("S00 - CONVERSOR EXCEL PARA CSV")
    print("=" * 60)

    # Diretório padrão de dados
    # Ajustar conforme estrutura do projeto
    default_dirs = [
        "../../../new_data",
        "../../new_data",
        "../new_data",
        "./new_data",
        "."
    ]

    data_dir = None
    for d in default_dirs:
        p = Path(d)
        if p.exists() and list(p.glob("*.xlsx")):
            data_dir = str(p.resolve())
            break

    if data_dir is None:
        print("\nDiretório de dados não encontrado.")
        print("Execute o script a partir do diretório correto ou")
        print("especifique o caminho usando as funções do módulo.")
        return {}

    print(f"\nDiretório de dados: {data_dir}")

    # Verificar e converter
    results = check_and_convert(data_dir)

    # Resumo
    converted = sum(1 for r in results.values() if r["status"] == "converted")
    existing = sum(1 for r in results.values() if r["status"] == "exists")
    errors = sum(1 for r in results.values() if r["status"] == "error")
    not_found = sum(1 for r in results.values() if r["status"] == "not_found")

    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Convertidos: {converted}")
    print(f"  Já existiam: {existing}")
    print(f"  Erros:       {errors}")
    print(f"  Não encontrados: {not_found}")
    print(f"  Total:       {len(results)}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
