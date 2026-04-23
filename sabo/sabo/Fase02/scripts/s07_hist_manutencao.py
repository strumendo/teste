"""
S07 - Histórico de Manutenção e Cruzamento com Produção
========================================================
Etapa complementar (Item 7.4 do escopo).

O QUE FAZ:
- Lê TODOS os arquivos "Dados Manut*.xlsx" em data/manutencao/ (2025, 2026, ...)
  preservando-os como fotografias históricas por data de leitura.
- Converte seriais Excel em datas (dd/mm/yyyy) e estrutura Medições
  Cilindro/Fuso + amplitudes (máx-mín) e desvio do nominal (20.0 mm).
- Cruza com data/raw/IJ-*.csv (produção) e calcula, por equipamento, as janelas
  entre início de produção, penúltima substituição e última substituição,
  agregando: Qtd. Produzida, Qtd. Refugada, Qtd. Retrabalhada, compostos
  utilizados, massa em kg e dias ociosos no final da série.

SAÍDAS (em outputs/):
- equipamentos_historico_completo.csv  (todas as leituras de todos os xlsx)
- equipamentos_historico_recente.csv   (apenas a leitura mais recente por equip.)
- equipamentos_janelas_manutencao.csv  (cruzamento produção × manutenção)
- equipamentos_ociosidade.csv          (dias sem produzir no final da série)

Regra de consolidação (decisão do stakeholder):
- Histórico: manter TODAS as leituras dos xlsx (mesmo equip. em 2025 e 2026 é
  mantido como duas linhas distintas, rotuladas por "arquivo_origem").
- Recente: apresentar a fotografia mais recente (arquivo com maior mtime) por
  equipamento.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# Config / paths
sys.path.insert(0, str(Path(__file__).parent.parent / "config"))
try:
    from paths import (
        DATA_RAW_DIR,
        DATA_MANUTENCAO_DIR,
        OUTPUTS_DIR,
        get_all_maintenance_xlsx_files,
    )
except ImportError:
    BASE = Path(__file__).parent.parent
    DATA_RAW_DIR = BASE / "data" / "raw"
    DATA_MANUTENCAO_DIR = BASE / "data" / "manutencao"
    OUTPUTS_DIR = BASE / "outputs"

    def get_all_maintenance_xlsx_files():
        files = list(DATA_MANUTENCAO_DIR.glob("Dados Manut*.xlsx"))
        return sorted(files, key=lambda f: f.stat().st_mtime)


# Epoch do Excel (Windows): 1899-12-30 trata corretamente o bug dos 29/02/1900.
_EXCEL_EPOCH = datetime(1899, 12, 30)
# Medida nominal do cilindro e fuso, em mm.
NOMINAL_MM = 20.0


def _excel_serial_to_date(value) -> date | None:
    """Converte um serial Excel (int/float ou string numérica) para date."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date() if hasattr(value, "date") else value
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f <= 0:
        return None
    return (_EXCEL_EPOCH + timedelta(days=f)).date()


def read_xlsx_manutencao(xlsx_path: Path) -> pd.DataFrame:
    """Lê UM arquivo 'Dados Manut*.xlsx' e devolve um DataFrame normalizado.

    Estrutura do xlsx:
        linha 0 → grupo (Histórico Manutenções / Medições Cilindro / Medições Fuso)
        linha 1 → cabeçalho de colunas
        linha 2+ → dados (coluna B = equipamento)
    """
    raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)
    header_row_idx = 1
    data = raw.iloc[header_row_idx + 1:].copy()

    # Mapa fixo por posição — o schema 2025 e 2026 é idêntico.
    cols = {
        1: "equipamento",           # B
        2: "data_ultima_sub_serial",  # C
        3: "data_penultima_sub_serial",  # D
        4: "dias_em_operacao",      # E
        5: "observacoes",           # F
        6: "cil_a",                 # G
        7: "cil_b",                 # H
        8: "cil_c",                 # I
        9: "cil_d",                 # J
        10: "cil_e",                # K
        11: "cil_max",              # L
        12: "cil_min",              # M
        13: "fuso_a",               # N
        14: "fuso_b",               # O
        15: "fuso_c",               # P
        16: "fuso_d",               # Q
        17: "fuso_max",             # R
        18: "fuso_min",             # S
    }
    data = data.rename(columns={k: v for k, v in cols.items()})
    data = data[list(cols.values())]
    data = data.dropna(subset=["equipamento"])
    data = data[data["equipamento"].astype(str).str.startswith("IJ-")]

    data["data_ultima_sub"] = data["data_ultima_sub_serial"].apply(_excel_serial_to_date)
    data["data_penultima_sub"] = data["data_penultima_sub_serial"].apply(_excel_serial_to_date)
    data = data.drop(columns=["data_ultima_sub_serial", "data_penultima_sub_serial"])

    for c in ["dias_em_operacao",
              "cil_a", "cil_b", "cil_c", "cil_d", "cil_e", "cil_max", "cil_min",
              "fuso_a", "fuso_b", "fuso_c", "fuso_d", "fuso_max", "fuso_min"]:
        data[c] = pd.to_numeric(data[c], errors="coerce")

    data["cil_amplitude"] = data["cil_max"] - data["cil_min"]
    data["fuso_amplitude"] = data["fuso_max"] - data["fuso_min"]
    data["desgaste_cil_max"] = (data["cil_max"] - NOMINAL_MM).abs()
    data["desgaste_fuso_min"] = (NOMINAL_MM - data["fuso_min"]).abs()

    data["arquivo_origem"] = xlsx_path.name
    data["arquivo_mtime"] = datetime.fromtimestamp(xlsx_path.stat().st_mtime)

    ordered = [
        "equipamento", "arquivo_origem", "arquivo_mtime",
        "data_penultima_sub", "data_ultima_sub", "dias_em_operacao", "observacoes",
        "cil_a", "cil_b", "cil_c", "cil_d", "cil_e",
        "cil_max", "cil_min", "cil_amplitude", "desgaste_cil_max",
        "fuso_a", "fuso_b", "fuso_c", "fuso_d",
        "fuso_max", "fuso_min", "fuso_amplitude", "desgaste_fuso_min",
    ]
    return data[ordered].reset_index(drop=True)


def build_historico_completo(xlsx_files: list[Path]) -> pd.DataFrame:
    """Concatena TODOS os xlsx, preservando cada leitura como um registro."""
    parts = [read_xlsx_manutencao(p) for p in xlsx_files]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df = df.sort_values(["equipamento", "arquivo_mtime"]).reset_index(drop=True)
    return df


def build_historico_recente(hist_completo: pd.DataFrame) -> pd.DataFrame:
    """Para cada equipamento, mantém apenas a leitura do xlsx mais recente."""
    if hist_completo.empty:
        return hist_completo
    idx = hist_completo.groupby("equipamento")["arquivo_mtime"].idxmax()
    return hist_completo.loc[idx].sort_values("equipamento").reset_index(drop=True)


def _read_producao(equipamento: str) -> pd.DataFrame | None:
    """Carrega data/raw/<equip>.csv e normaliza colunas."""
    path = DATA_RAW_DIR / f"{equipamento}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    # As colunas dos CSV de produção têm acentuação. Mapeia defensivamente.
    rename = {
        "Data de Produção": "data_producao",
        "Qtd. Produzida": "qtd_produzida",
        "Qtd. Refugada": "qtd_refugada",
        "Qtd. Retrabalhada": "qtd_retrabalhada",
        "Descrição da massa (Composto)": "composto",
        "Consumo de massa no item em (Kg/100pçs)": "kg_por_100pc",
        "Cód. Produto": "cod_produto",
    }
    df = df.rename(columns=rename)
    # Os CSVs em data/raw/ já são persistidos pelo s01 em ISO (yyyy-mm-dd).
    # Tenta ISO primeiro; se falhar majoritariamente, cai para dd/mm/yyyy.
    parsed = pd.to_datetime(df["data_producao"], format="ISO8601", errors="coerce")
    if parsed.isna().mean() > 0.5:
        parsed = pd.to_datetime(df["data_producao"], dayfirst=True, errors="coerce")
    df["data_producao"] = parsed.dt.date
    for c in ["qtd_produzida", "qtd_refugada", "qtd_retrabalhada", "kg_por_100pc"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0
    if "composto" not in df.columns:
        df["composto"] = None
    df["massa_kg"] = df["qtd_produzida"] * df["kg_por_100pc"] / 100.0
    return df.dropna(subset=["data_producao"])


def _agg_janela(df: pd.DataFrame, inicio: date | None, fim: date | None) -> dict:
    """Agrega métricas da janela [inicio, fim) — meias aberturas para evitar duplo-conte."""
    sub = df
    if inicio is not None:
        sub = sub[sub["data_producao"] >= inicio]
    if fim is not None:
        sub = sub[sub["data_producao"] < fim]
    if sub.empty:
        return {
            "qtd_produzida": 0.0, "qtd_refugada": 0.0, "qtd_retrabalhada": 0.0,
            "massa_kg": 0.0, "compostos": "", "dias_producao_distintos": 0,
        }
    compostos = sub["composto"].dropna().astype(str).unique().tolist()
    return {
        "qtd_produzida": float(sub["qtd_produzida"].sum()),
        "qtd_refugada": float(sub["qtd_refugada"].sum()),
        "qtd_retrabalhada": float(sub["qtd_retrabalhada"].sum()),
        "massa_kg": float(sub["massa_kg"].sum()),
        "compostos": ";".join(sorted(compostos)),
        "dias_producao_distintos": int(sub["data_producao"].nunique()),
    }


def build_janelas_manutencao(hist_recente: pd.DataFrame,
                             hist_completo: pd.DataFrame) -> pd.DataFrame:
    """Para cada equipamento da fotografia recente, calcula métricas por janela.

    Janelas possíveis:
        J1 = [primeira produção, penúltima_sub)        → ciclo até a penúltima troca
        J2 = [penúltima_sub, última_sub)                → ciclo entre trocas
        J3 = [última_sub, última produção + 1 dia]      → ciclo atual (pós-última troca)
    """
    rows = []
    for _, r in hist_recente.iterrows():
        equip = r["equipamento"]
        prod = _read_producao(equip)
        if prod is None or prod.empty:
            continue

        primeiro = prod["data_producao"].min()
        ultimo = prod["data_producao"].max()
        d_pen = r["data_penultima_sub"]
        d_ult = r["data_ultima_sub"]

        janelas = [
            ("J1_inicio_ate_penultima", primeiro, d_pen),
            ("J2_entre_trocas", d_pen, d_ult),
            ("J3_pos_ultima_troca", d_ult, ultimo + timedelta(days=1) if ultimo else None),
        ]
        for nome, ini, fim in janelas:
            agg = _agg_janela(prod, ini, fim)
            dias_total = (fim - ini).days if (ini is not None and fim is not None) else None
            rows.append({
                "equipamento": equip,
                "janela": nome,
                "inicio": ini,
                "fim": fim,
                "dias_calendario": dias_total,
                **agg,
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["equipamento", "janela"]).reset_index(drop=True)
    return df


def build_ociosidade(hist_recente: pd.DataFrame, today: date | None = None) -> pd.DataFrame:
    """Calcula dias sem produção entre a última produção no raw e a data de referência."""
    if today is None:
        today = date.today()
    rows = []
    for _, r in hist_recente.iterrows():
        equip = r["equipamento"]
        prod = _read_producao(equip)
        if prod is None or prod.empty:
            rows.append({"equipamento": equip, "ultima_producao": None,
                         "data_ref": today, "dias_ociosidade": None})
            continue
        ultimo = prod["data_producao"].max()
        rows.append({
            "equipamento": equip,
            "ultima_producao": ultimo,
            "data_ref": today,
            "dias_ociosidade": (today - ultimo).days,
        })
    return pd.DataFrame(rows)


def _format_dates_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas date para strings dd/mm/yyyy antes de salvar em CSV."""
    out = df.copy()
    for c in out.columns:
        # date/datetime puro
        if out[c].dtype == object:
            sample = out[c].dropna().head(1).tolist()
            if sample and isinstance(sample[0], (date, datetime, pd.Timestamp)):
                out[c] = out[c].apply(
                    lambda v: v.strftime("%d/%m/%Y") if v is not None and not pd.isna(v) else ""
                )
    return out


def main(**pipeline_context) -> dict:
    """Ponto de entrada chamado por run_pipeline.py ou execução direta."""
    xlsx_files = get_all_maintenance_xlsx_files()
    if not xlsx_files:
        print("[s07] Nenhum 'Dados Manut*.xlsx' encontrado em data/manutencao/")
        return {"ok": False, "motivo": "sem xlsx de manutenção"}

    print(f"[s07] Lendo {len(xlsx_files)} arquivo(s) de manutenção:")
    for p in xlsx_files:
        print(f"       - {p.name}")

    hist_completo = build_historico_completo(xlsx_files)
    hist_recente = build_historico_recente(hist_completo)
    janelas = build_janelas_manutencao(hist_recente, hist_completo)
    ociosidade = build_ociosidade(hist_recente)

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_completo = OUTPUTS_DIR / "equipamentos_historico_completo.csv"
    out_recente = OUTPUTS_DIR / "equipamentos_historico_recente.csv"
    out_janelas = OUTPUTS_DIR / "equipamentos_janelas_manutencao.csv"
    out_ocio = OUTPUTS_DIR / "equipamentos_ociosidade.csv"

    _format_dates_for_csv(hist_completo).to_csv(out_completo, index=False)
    _format_dates_for_csv(hist_recente).to_csv(out_recente, index=False)
    _format_dates_for_csv(janelas).to_csv(out_janelas, index=False)
    _format_dates_for_csv(ociosidade).to_csv(out_ocio, index=False)

    print(f"[s07] {len(hist_completo)} leituras históricas → {out_completo.name}")
    print(f"[s07] {len(hist_recente)} equipamentos (mais recente) → {out_recente.name}")
    print(f"[s07] {len(janelas)} janelas de manutenção → {out_janelas.name}")
    print(f"[s07] {len(ociosidade)} registros de ociosidade → {out_ocio.name}")

    return {
        "ok": True,
        "historico_completo_linhas": int(len(hist_completo)),
        "historico_recente_linhas": int(len(hist_recente)),
        "janelas_linhas": int(len(janelas)),
        "ociosidade_linhas": int(len(ociosidade)),
    }


if __name__ == "__main__":
    main()
