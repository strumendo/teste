"""
S08 - Prescrição de Próxima Manutenção por Equipamento
=======================================================
Etapa complementar (Item 7.4 do escopo).

O QUE FAZ:
- Para cada equipamento da fotografia mais recente, calcula a data prescrita
  da próxima manutenção integrando três sinais:
    (1) Baseline: mediana dos "dias em operação" do histórico do equipamento.
    (2) Fator de desgaste: amplitude/desvio das medições cilindro e fuso da
        leitura mais recente comparado ao histórico do próprio equipamento.
    (3) Fator de consumo de massa: kg de massa consumidos na janela atual
        (pós-última troca) comparados à mediana das janelas anteriores do
        mesmo equipamento, ponderado pelo kg/100pçs.
- Acrescenta dias de ociosidade (última produção → data de referência) à data
  prescrita — máquina parada não desgasta, então o prazo "desliza" para frente.

ENTRADAS (geradas pelo s07_hist_manutencao.py):
- outputs/equipamentos_historico_completo.csv
- outputs/equipamentos_historico_recente.csv
- outputs/equipamentos_janelas_manutencao.csv
- outputs/equipamentos_ociosidade.csv

SAÍDAS:
- outputs/prescricao_manutencao.csv

Fórmula resumida:
    T_prescrito = T_base * fator_desgaste * fator_massa
    data_prescrita = data_ultima_sub + T_prescrito + dias_ociosidade

Clamps aplicados (para proteger contra outliers em histórico curto):
    fator_desgaste ∈ [0.60, 1.20]
    fator_massa    ∈ [0.70, 1.30]
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "config"))
try:
    from paths import OUTPUTS_DIR
except ImportError:
    OUTPUTS_DIR = Path(__file__).parent.parent / "outputs"


FATOR_DESGASTE_MIN = 0.60
FATOR_DESGASTE_MAX = 1.20
FATOR_MASSA_MIN = 0.70
FATOR_MASSA_MAX = 1.30
T_BASE_FALLBACK_DIAS = 450  # Se o equipamento tem só uma leitura e sem dias_em_operacao


def _parse_date(s) -> date | None:
    if s is None or (isinstance(s, float) and pd.isna(s)) or s == "":
        return None
    if isinstance(s, (date, datetime, pd.Timestamp)):
        return s.date() if hasattr(s, "date") else s
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except ValueError:
            continue
    return None


def _clamp(x: float, lo: float, hi: float) -> float:
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return 1.0
    return max(lo, min(hi, x))


def _urgencia(dias_restantes: int | None) -> str:
    if dias_restantes is None:
        return "INDEFINIDO"
    if dias_restantes < 0:
        return "ATRASADO"
    if dias_restantes < 30:
        return "URGENTE"
    if dias_restantes < 90:
        return "ATENÇÃO"
    return "OK"


def calcular_prescricao_para_equip(
    equip: str,
    hist_recente_row: pd.Series,
    hist_completo: pd.DataFrame,
    janelas: pd.DataFrame,
    ociosidade_row: pd.Series | None,
    today: date,
) -> dict:
    # (1) Baseline: mediana de dias_em_operacao do histórico próprio.
    hist_equip = hist_completo[hist_completo["equipamento"] == equip]
    dias_op_hist = pd.to_numeric(hist_equip["dias_em_operacao"], errors="coerce").dropna()
    if len(dias_op_hist) > 0:
        t_base = float(dias_op_hist.median())
    else:
        t_base = float(T_BASE_FALLBACK_DIAS)

    # (2) Fator de desgaste — usa amplitude atual vs. mediana histórica.
    def _safe_num(col):
        v = pd.to_numeric(hist_equip[col], errors="coerce") if col in hist_equip.columns else pd.Series([], dtype=float)
        return v.dropna()

    amp_cil_hist = _safe_num("cil_amplitude")
    amp_fuso_hist = _safe_num("fuso_amplitude")

    amp_cil_atual = pd.to_numeric(pd.Series([hist_recente_row.get("cil_amplitude")]), errors="coerce").dropna()
    amp_fuso_atual = pd.to_numeric(pd.Series([hist_recente_row.get("fuso_amplitude")]), errors="coerce").dropna()

    def _fator_componente(atual: pd.Series, hist: pd.Series) -> float:
        if len(atual) == 0 or len(hist) == 0:
            return 1.0
        at = float(atual.iloc[0])
        med = float(hist.median())
        if med <= 0:
            return 1.0
        # >1 = mais amplitude que típico ⇒ mais desgaste ⇒ encurtar prazo
        # fator = inverso suave da razão, centrado em 1.0
        ratio = at / med if med > 0 else 1.0
        return 1.0 / max(ratio, 1e-6)

    f_cil = _fator_componente(amp_cil_atual, amp_cil_hist)
    f_fuso = _fator_componente(amp_fuso_atual, amp_fuso_hist)
    fator_desgaste = _clamp((f_cil + f_fuso) / 2.0, FATOR_DESGASTE_MIN, FATOR_DESGASTE_MAX)

    # (3) Fator de consumo de massa — janela atual vs. janelas anteriores do equip.
    jeq = janelas[janelas["equipamento"] == equip]
    j_atual = jeq[jeq["janela"] == "J3_pos_ultima_troca"]
    j_prev = jeq[jeq["janela"].isin(["J1_inicio_ate_penultima", "J2_entre_trocas"])]

    massa_atual = float(j_atual["massa_kg"].sum()) if not j_atual.empty else 0.0
    massa_prev_valida = j_prev[j_prev["dias_calendario"].astype(float) > 0]
    massa_mediana_prev = (
        float(massa_prev_valida["massa_kg"].median()) if len(massa_prev_valida) > 0 else None
    )

    if massa_mediana_prev is None or massa_mediana_prev <= 0 or massa_atual <= 0:
        fator_massa = 1.0
    else:
        ratio_massa = massa_atual / massa_mediana_prev
        # >1 = máquina consumiu mais massa no ciclo atual do que o típico ⇒
        # antecipar a próxima troca. Fator inverso da razão, suavizado.
        fator_massa = _clamp(1.0 / max(ratio_massa, 1e-6), FATOR_MASSA_MIN, FATOR_MASSA_MAX)

    # (4) T_prescrito
    t_prescrito = t_base * fator_desgaste * fator_massa

    # (5) Ociosidade (sem produzir no final da série ⇒ prazo desliza para frente)
    dias_ocio = None
    if ociosidade_row is not None:
        v = ociosidade_row.get("dias_ociosidade")
        if v is not None and not (isinstance(v, float) and pd.isna(v)):
            dias_ocio = max(0, int(v))  # negativos = futuro; tratamos como 0.

    d_ult = _parse_date(hist_recente_row.get("data_ultima_sub"))
    if d_ult is None:
        data_prescrita = None
        dias_restantes = None
    else:
        data_prescrita = d_ult + timedelta(days=int(round(t_prescrito))) + timedelta(days=dias_ocio or 0)
        dias_restantes = (data_prescrita - today).days

    return {
        "equipamento": equip,
        "data_ultima_sub": d_ult,
        "dias_operacao_mediana_hist": round(t_base, 1),
        "cil_amplitude_atual": float(amp_cil_atual.iloc[0]) if len(amp_cil_atual) > 0 else None,
        "cil_amplitude_mediana_hist": float(amp_cil_hist.median()) if len(amp_cil_hist) > 0 else None,
        "fuso_amplitude_atual": float(amp_fuso_atual.iloc[0]) if len(amp_fuso_atual) > 0 else None,
        "fuso_amplitude_mediana_hist": float(amp_fuso_hist.median()) if len(amp_fuso_hist) > 0 else None,
        "fator_desgaste": round(fator_desgaste, 4),
        "massa_kg_janela_atual": round(massa_atual, 2),
        "massa_kg_mediana_prev": round(massa_mediana_prev, 2) if massa_mediana_prev is not None else None,
        "fator_massa": round(fator_massa, 4),
        "t_base_dias": round(t_base, 1),
        "t_prescrito_dias": round(t_prescrito, 1),
        "dias_ociosidade": dias_ocio,
        "data_prescrita": data_prescrita,
        "data_ref": today,
        "dias_restantes": dias_restantes,
        "urgencia": _urgencia(dias_restantes),
    }


def _format_dates_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object:
            sample = out[c].dropna().head(1).tolist()
            if sample and isinstance(sample[0], (date, datetime, pd.Timestamp)):
                out[c] = out[c].apply(
                    lambda v: v.strftime("%d/%m/%Y") if v is not None and not pd.isna(v) else ""
                )
    return out


def main(**pipeline_context) -> dict:
    hist_completo_path = OUTPUTS_DIR / "equipamentos_historico_completo.csv"
    hist_recente_path = OUTPUTS_DIR / "equipamentos_historico_recente.csv"
    janelas_path = OUTPUTS_DIR / "equipamentos_janelas_manutencao.csv"
    ocio_path = OUTPUTS_DIR / "equipamentos_ociosidade.csv"

    missing = [p.name for p in [hist_completo_path, hist_recente_path, janelas_path, ocio_path] if not p.exists()]
    if missing:
        print(f"[s08] Faltam insumos do s07: {missing}. Rode s07_hist_manutencao.py antes.")
        return {"ok": False, "motivo": f"faltam insumos: {missing}"}

    hist_completo = pd.read_csv(hist_completo_path)
    hist_recente = pd.read_csv(hist_recente_path)
    janelas = pd.read_csv(janelas_path)
    ocio = pd.read_csv(ocio_path)

    # Normaliza datas para comparações (mantemos strings no CSV, mas aqui viram date).
    for df in (hist_completo, hist_recente):
        for c in ("data_ultima_sub", "data_penultima_sub"):
            if c in df.columns:
                df[c] = df[c].apply(_parse_date)

    today_ctx = pipeline_context.get("today")
    today = _parse_date(today_ctx) if today_ctx else date.today()

    linhas = []
    for _, row in hist_recente.iterrows():
        equip = row["equipamento"]
        oc_rows = ocio[ocio["equipamento"] == equip]
        oc_row = oc_rows.iloc[0] if not oc_rows.empty else None
        linhas.append(
            calcular_prescricao_para_equip(equip, row, hist_completo, janelas, oc_row, today)
        )

    df_out = pd.DataFrame(linhas).sort_values("dias_restantes", na_position="last").reset_index(drop=True)

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUTS_DIR / "prescricao_manutencao.csv"
    _format_dates_for_csv(df_out).to_csv(out_path, index=False)

    print(f"[s08] Prescrições para {len(df_out)} equipamentos → {out_path.name}")
    if not df_out.empty:
        print("[s08] Top 5 por urgência:")
        cols = ["equipamento", "data_prescrita", "dias_restantes", "urgencia",
                "fator_desgaste", "fator_massa", "dias_ociosidade"]
        print(_format_dates_for_csv(df_out[cols]).head(5).to_string(index=False))

    return {"ok": True, "equipamentos": int(len(df_out))}


if __name__ == "__main__":
    main()
