"""
Gera, para cada equipamento, um relatório mensal de produção segmentado por
composto (massa) com percentuais e intercalado com as datas de manutenção:

- Preventivas RM.195 (Histórico Geral Preventivas RM.195 - 27 Equip IRP´s.xlsx)
- Trocas registradas (equipamentos_historico_completo.csv → data_penultima_sub /
  data_ultima_sub).

Saídas:
- outputs/relatorios_mensais_componentes/IJ-XXX.md  (um por equipamento)
- outputs/relatorios_mensais_componentes/INDEX.md   (índice geral)
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR.parent / "config"))
try:
    from config import paths  # type: ignore
except Exception:
    import paths  # type: ignore

OUT_DIR = paths.OUTPUTS_DIR / "relatorios_mensais_componentes"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MESES_PT = {
    1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun",
    7: "jul", 8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez",
}


def _fmt_int(v) -> str:
    if pd.isna(v):
        return "—"
    return f"{int(v):,}".replace(",", ".")


def _fmt_pct(v) -> str:
    if pd.isna(v):
        return "—"
    return f"{v:.2f} %".replace(".", ",")


def _parse_date_br_or_iso(s) -> pd.Timestamp | None:
    if pd.isna(s) or s == "":
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            continue
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def carregar_preventivas() -> pd.DataFrame:
    """Carrega o histórico de preventivas RM.195 por equipamento."""
    f = paths.get_preventive_history_file() if hasattr(paths, "get_preventive_history_file") else None
    if f is None or not Path(f).exists():
        return pd.DataFrame(columns=["equipamento", "data", "ordem", "n"])
    df = pd.read_excel(f)
    df = df.rename(columns={
        "Equipamento": "equipamento",
        "Dta.iníc.progr.": "data",
        "Nº solicitação": "n",
        "Ordem": "ordem",
    })
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data"]).sort_values(["equipamento", "data"])
    return df[["equipamento", "data", "n", "ordem"]]


def carregar_prescricoes() -> pd.DataFrame:
    """Carrega prescricao_manutencao.csv (gerado por s08_prescricao_manutencao.py)."""
    f = paths.OUTPUTS_DIR / "prescricao_manutencao.csv"
    if not f.exists():
        return pd.DataFrame()
    return pd.read_csv(f)


def carregar_trocas_registradas() -> pd.DataFrame:
    """Carrega trocas (penúltima/última substituição) de equipamentos_historico_completo.csv."""
    f = paths.OUTPUTS_DIR / "equipamentos_historico_completo.csv"
    if not f.exists():
        return pd.DataFrame(columns=["equipamento", "data", "tipo"])
    df = pd.read_csv(f)
    eventos = []
    for _, row in df.drop_duplicates("equipamento").iterrows():
        for col, tipo in (("data_penultima_sub", "Penúltima troca"),
                          ("data_ultima_sub", "Última troca")):
            d = _parse_date_br_or_iso(row.get(col))
            if d is not None:
                eventos.append({"equipamento": row["equipamento"], "data": d, "tipo": tipo})
    return pd.DataFrame(eventos)


def _fmt_num(v, casas: int = 2) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
        return "—"
    try:
        return f"{float(v):,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)


def secao_prescricao(eq: str, prescricoes: pd.DataFrame) -> list[str]:
    """Monta a seção de prescrição da próxima manutenção para o equipamento."""
    linhas: list[str] = ["## Prescrição da próxima manutenção\n"]
    if prescricoes.empty:
        linhas.append("_`outputs/prescricao_manutencao.csv` não encontrado — rode `s08_prescricao_manutencao.py`._\n")
        return linhas
    sub = prescricoes[prescricoes["equipamento"] == eq]
    if sub.empty:
        linhas.append("_Equipamento sem prescrição registrada (sem leitura recente em s07)._\n")
        return linhas
    r = sub.iloc[0]

    # Identifica componentes que disparam ajuste
    def _f(v): return None if pd.isna(v) else float(v)
    fd = _f(r.get("fator_desgaste"))
    fm = _f(r.get("fator_massa"))
    t_base = _f(r.get("t_base_dias"))
    t_pres = _f(r.get("t_prescrito_dias"))
    ocio = _f(r.get("dias_ociosidade")) or 0
    cil_at = _f(r.get("cil_amplitude_atual"))
    cil_h = _f(r.get("cil_amplitude_mediana_hist"))
    fus_at = _f(r.get("fuso_amplitude_atual"))
    fus_h = _f(r.get("fuso_amplitude_mediana_hist"))
    massa_at = _f(r.get("massa_kg_janela_atual"))
    massa_h = _f(r.get("massa_kg_mediana_prev"))

    # Cabeçalho com a recomendação
    urg = r.get("urgencia", "?")
    dr = r.get("dias_restantes")
    linhas.append(f"**Data prescrita:** `{r.get('data_prescrita', '—')}`  ")
    linhas.append(f"**Data da última troca:** `{r.get('data_ultima_sub', '—')}`  ")
    linhas.append(f"**Data de referência (hoje):** `{r.get('data_ref', '—')}`  ")
    linhas.append(f"**Dias restantes:** `{dr}`  ")
    linhas.append(f"**Urgência:** **{urg}**\n")

    # Como a data foi calculada
    linhas.append("### Como foi calculada\n")
    linhas.append("A prescrição combina três sinais do próprio equipamento. As fórmulas (de `s08_prescricao_manutencao.py`):\n")
    linhas.append("```text")
    linhas.append("T_base          = mediana(dias_em_operacao do histórico do equipamento)")
    linhas.append("fator_desgaste  = clamp( média( T_base/amp_atual_cil , T_base/amp_atual_fuso ) ,")
    linhas.append("                         0.60 , 1.20 )           # baseado nas amplitudes")
    linhas.append("                  → forma usada: 1 / (amp_atual / amp_mediana_hist)")
    linhas.append("fator_massa     = clamp( 1 / (massa_atual / massa_mediana_janelas_anteriores) ,")
    linhas.append("                         0.70 , 1.30 )")
    linhas.append("T_prescrito     = T_base × fator_desgaste × fator_massa")
    linhas.append("data_prescrita  = data_ultima_sub + T_prescrito + dias_ociosidade")
    linhas.append("```\n")

    # Tabela com os números reais usados
    linhas.append("### Valores usados para este equipamento\n")
    linhas.append("| Componente | Valor atual | Referência histórica | Fator | Efeito |")
    linhas.append("|------------|------------:|---------------------:|------:|--------|")
    # Linha T_base
    linhas.append(
        f"| **T_base** (dias) | {_fmt_num(t_base, 1)} | mediana de `dias_em_operacao` | — "
        f"| ponto de partida do prazo |"
    )
    # Linha desgaste
    if cil_at is not None or fus_at is not None:
        ref_desg = (
            f"cil. {_fmt_num(cil_h, 3)} / fuso {_fmt_num(fus_h, 3)}"
            if (cil_h is not None or fus_h is not None) else "—"
        )
        atual_desg = f"cil. {_fmt_num(cil_at, 3)} / fuso {_fmt_num(fus_at, 3)}"
        if fd is not None and fd < 1.0:
            efeito = f"desgaste **acima** do típico → encurta prazo (×{_fmt_num(fd, 3)})"
        elif fd is not None and fd > 1.0:
            efeito = f"desgaste **abaixo** do típico → estende prazo (×{_fmt_num(fd, 3)})"
        else:
            efeito = "neutro / sem dado"
        linhas.append(f"| **Amplitude cil./fuso** | {atual_desg} | {ref_desg} | {_fmt_num(fd, 4)} | {efeito} |")
    else:
        linhas.append(f"| **Amplitude cil./fuso** | — | — | {_fmt_num(fd, 4)} | sem leitura — fator neutro 1,00 |")
    # Linha massa
    if massa_at is not None and massa_h is not None:
        ratio = massa_at / massa_h if massa_h > 0 else None
        if fm is not None and fm < 1.0:
            efeito_m = (
                f"consumiu **{_fmt_num(ratio, 2)}×** a massa típica → "
                f"antecipa a próxima troca (×{_fmt_num(fm, 3)})"
            )
        elif fm is not None and fm > 1.0:
            efeito_m = (
                f"consumiu **{_fmt_num(ratio, 2)}×** a massa típica → "
                f"adia a próxima troca (×{_fmt_num(fm, 3)})"
            )
        else:
            efeito_m = "consumo dentro do esperado"
        linhas.append(
            f"| **Massa consumida pós-última troca (kg)** | {_fmt_num(massa_at, 2)} "
            f"| mediana janelas anteriores: {_fmt_num(massa_h, 2)} | {_fmt_num(fm, 4)} | {efeito_m} |"
        )
    else:
        linhas.append(
            f"| **Massa consumida pós-última troca (kg)** | {_fmt_num(massa_at, 2)} "
            f"| {_fmt_num(massa_h, 2)} | {_fmt_num(fm, 4)} | sem histórico de janela anterior — fator 1,00 |"
        )
    # Linha ociosidade
    if ocio > 0:
        ef_oc = f"máquina parada **{int(ocio)} dia(s)** → desliza a data prescrita para frente"
    else:
        ef_oc = "sem deslocamento"
    linhas.append(f"| **Dias de ociosidade** | {int(ocio)} | última produção → hoje | — | {ef_oc} |")
    linhas.append("")

    # Substituição numérica explícita
    linhas.append("### Substituindo na fórmula\n")
    if t_base is not None and fd is not None and fm is not None and t_pres is not None:
        linhas.append("```text")
        linhas.append(
            f"T_prescrito = {_fmt_num(t_base, 1)} × {_fmt_num(fd, 4)} × {_fmt_num(fm, 4)} "
            f"= {_fmt_num(t_pres, 1)} dias"
        )
        linhas.append(
            f"data_prescrita = {r.get('data_ultima_sub', '—')}"
            f" + {_fmt_num(t_pres, 0)} dias + {int(ocio)} dia(s) ociosidade"
            f" = {r.get('data_prescrita', '—')}"
        )
        linhas.append("```\n")

    # Por que essa urgência
    linhas.append("### Por que a urgência ficou **" + str(urg) + "**\n")
    if dr is None or pd.isna(dr):
        linhas.append("Sem `dias_restantes` calculados.\n")
    else:
        try:
            dr_int = int(dr)
        except Exception:
            dr_int = None
        regras = (
            "- `dias_restantes < 0`  → **ATRASADO** (data prescrita já passou)\n"
            "- `0 ≤ dias_restantes < 30`  → **URGENTE**\n"
            "- `30 ≤ dias_restantes < 90`  → **ATENÇÃO**\n"
            "- `dias_restantes ≥ 90`  → **OK**"
        )
        linhas.append(regras + "\n")
        if dr_int is not None:
            linhas.append(f"Para este equipamento, `dias_restantes = {dr_int}` ⇒ **{urg}**.\n")

    # Notas e clamps
    linhas.append("### Observações sobre a fórmula\n")
    linhas.append("- `fator_desgaste` é limitado ao intervalo **[0,60 ; 1,20]** para evitar que outliers em históricos curtos puxem a data demais.")
    linhas.append("- `fator_massa` é limitado ao intervalo **[0,70 ; 1,30]** pelo mesmo motivo.")
    linhas.append("- Quando não há leitura histórica suficiente, o fator vira **1,00** (neutro) — o sinal correspondente é silenciado, não chuta.")
    linhas.append("- `dias_ociosidade` só **adiciona** prazo (máquina parada não desgasta); valores negativos são tratados como zero.\n")

    return linhas


def gerar_relatorio_equipamento(eq: str, df_eq: pd.DataFrame,
                                preventivas: pd.DataFrame,
                                trocas: pd.DataFrame,
                                prescricoes: pd.DataFrame) -> str:
    df = df_eq.copy()
    df["data"] = pd.to_datetime(df["Data de Produção"], errors="coerce")
    df = df.dropna(subset=["data"])
    df["ano_mes"] = df["data"].dt.to_period("M")
    df["composto"] = df["Descrição da massa (Composto)"].fillna("(sem composto)").astype(str)
    df["qtd"] = pd.to_numeric(df["Qtd. Produzida"], errors="coerce").fillna(0)
    df["refugo"] = pd.to_numeric(df["Qtd. Refugada"], errors="coerce").fillna(0)

    total_geral = df["qtd"].sum()
    total_refugo = df["refugo"].sum()
    inicio = df["data"].min()
    fim = df["data"].max()

    # Pivot mensal × composto
    pv = df.groupby(["ano_mes", "composto"], as_index=False).agg(
        qtd=("qtd", "sum"),
        refugo=("refugo", "sum"),
        dias=("data", lambda s: s.dt.normalize().nunique()),
    )

    # Total mensal e %
    tot_mes = df.groupby("ano_mes")["qtd"].sum().rename("qtd_mes")
    pv = pv.merge(tot_mes, on="ano_mes")
    pv["pct_mes"] = pv["qtd"] / pv["qtd_mes"].replace(0, pd.NA) * 100
    pv["pct_total"] = pv["qtd"] / (total_geral or 1) * 100
    pv = pv.sort_values(["ano_mes", "qtd"], ascending=[True, False])

    # Eventos de manutenção (preventivas + trocas)
    eventos = []
    for _, r in preventivas[preventivas["equipamento"] == eq].iterrows():
        eventos.append({"data": r["data"], "tipo": f"Preventiva RM.195 (#{int(r['n'])})"})
    for _, r in trocas[trocas["equipamento"] == eq].iterrows():
        eventos.append({"data": r["data"], "tipo": r["tipo"]})
    eventos_df = pd.DataFrame(eventos).drop_duplicates().sort_values("data") if eventos else pd.DataFrame()

    # ---- Markdown ----
    linhas: list[str] = []
    linhas.append(f"# Relatório mensal por componente — {eq}\n")
    linhas.append(f"**Fonte:** `data/raw/{eq}.csv`  ")
    linhas.append(f"**Período coberto:** {inicio:%d/%m/%Y} → {fim:%d/%m/%Y}  ")
    linhas.append(f"**Total produzido:** {_fmt_int(total_geral)} peças  ")
    pct_ref = (total_refugo / total_geral * 100) if total_geral else 0
    linhas.append(f"**Total refugado:** {_fmt_int(total_refugo)} peças ({_fmt_pct(pct_ref)})  ")
    linhas.append(f"**Compostos distintos utilizados:** {df['composto'].nunique()}\n")

    # Tabela de manutenções
    linhas.append("## Manutenções registradas\n")
    if eventos_df.empty:
        linhas.append("_Nenhum evento de manutenção registrado para este equipamento._\n")
    else:
        linhas.append("| Data | Evento | Dentro do período de produção? |")
        linhas.append("|------|--------|-------------------------------|")
        for _, ev in eventos_df.iterrows():
            dentro = "✅ sim" if inicio <= ev["data"] <= fim else "—"
            linhas.append(f"| {ev['data']:%d/%m/%Y} | {ev['tipo']} | {dentro} |")
        linhas.append("")

    # Tabela detalhada mensal × composto
    linhas.append("## Produção mensal por composto\n")
    linhas.append("> `% do mês` = participação do composto naquele mês.  ")
    linhas.append("> `% do total` = participação do composto no período inteiro do equipamento.  ")
    linhas.append("> Linhas 🔧 marcam eventos de manutenção (preventivas / trocas).\n")
    linhas.append("| Ano-Mês | Composto | Qtd. produzida | % do mês | % do total | Refugo | Dias com produção |")
    linhas.append("|---------|----------|---------------:|---------:|-----------:|-------:|------------------:|")

    eventos_por_mes: dict[pd.Period, list[tuple[pd.Timestamp, str]]] = {}
    if not eventos_df.empty:
        for _, ev in eventos_df.iterrows():
            p = ev["data"].to_period("M")
            eventos_por_mes.setdefault(p, []).append((ev["data"], ev["tipo"]))

    # Cobrir todo o intervalo (inclusive meses sem produção, p.ex. paradas)
    todos_meses = pd.period_range(inicio.to_period("M"), fim.to_period("M"), freq="M")
    pv_por_mes = {m: g for m, g in pv.groupby("ano_mes")}
    ultimo_mes_rotulo = None
    for mes in todos_meses:
        rotulo_mes = f"{mes.year}-{MESES_PT[mes.month]}"
        if mes in pv_por_mes:
            for _, r in pv_por_mes[mes].iterrows():
                rot = rotulo_mes if rotulo_mes != ultimo_mes_rotulo else ""
                ultimo_mes_rotulo = rotulo_mes
                linhas.append(
                    f"| {rot} | {r['composto']} | {_fmt_int(r['qtd'])} "
                    f"| {_fmt_pct(r['pct_mes'])} | {_fmt_pct(r['pct_total'])} "
                    f"| {_fmt_int(r['refugo'])} | {int(r['dias'])} |"
                )
        else:
            rot = rotulo_mes if rotulo_mes != ultimo_mes_rotulo else ""
            ultimo_mes_rotulo = rotulo_mes
            linhas.append(f"| {rot} | _(sem produção)_ | 0 | — | — | 0 | 0 |")

        if mes in eventos_por_mes:
            for d, tipo in sorted(eventos_por_mes[mes]):
                linhas.append(f"|  | 🔧 **{d:%d/%m/%Y}** — _{tipo}_ |  |  |  |  |  |")
    linhas.append("")

    # Resumo por composto (período inteiro)
    linhas.append("## Resumo por composto (período inteiro)\n")
    resumo = df.groupby("composto", as_index=False).agg(
        qtd=("qtd", "sum"),
        refugo=("refugo", "sum"),
        meses=("ano_mes", "nunique"),
    ).sort_values("qtd", ascending=False)
    resumo["pct_total"] = resumo["qtd"] / (total_geral or 1) * 100
    linhas.append("| Composto | Qtd. produzida | % do total | Refugo | Meses com uso |")
    linhas.append("|----------|---------------:|-----------:|-------:|--------------:|")
    for _, r in resumo.iterrows():
        linhas.append(
            f"| {r['composto']} | {_fmt_int(r['qtd'])} "
            f"| {_fmt_pct(r['pct_total'])} | {_fmt_int(r['refugo'])} | {int(r['meses'])} |"
        )
    linhas.append(
        f"| **TOTAL** | **{_fmt_int(total_geral)}** | **100,00 %** "
        f"| **{_fmt_int(total_refugo)}** | **{df['ano_mes'].nunique()}** |"
    )
    linhas.append("")

    # Prescrição da próxima manutenção (com motivo e fórmulas)
    linhas.extend(secao_prescricao(eq, prescricoes))

    return "\n".join(linhas)


def main(**kwargs):
    arquivos = sorted(paths.DATA_RAW_DIR.glob("IJ-*.csv"))
    if not arquivos:
        print("[ERRO] Nenhum CSV em data/raw/IJ-*.csv")
        return {}

    preventivas = carregar_preventivas()
    trocas = carregar_trocas_registradas()
    prescricoes = carregar_prescricoes()

    gerados = []
    for f in arquivos:
        eq = f.stem
        df = pd.read_csv(f)
        if df.empty:
            continue
        if "Descrição da massa (Composto)" not in df.columns:
            print(f"  ⚠ {eq}: arquivo sem coluna de composto — ignorado ({f.name})")
            continue
        md = gerar_relatorio_equipamento(eq, df, preventivas, trocas, prescricoes)
        out = OUT_DIR / f"{eq}.md"
        out.write_text(md, encoding="utf-8")
        gerados.append((eq, out, len(df)))
        print(f"  ✓ {eq}: {len(df):,} apontamentos → {out.name}")

    # Índice
    idx = ["# Índice — relatórios mensais por componente\n",
           f"_Gerado em {datetime.now():%d/%m/%Y %H:%M}._  ",
           f"_Equipamentos: {len(gerados)}._\n",
           "| Equipamento | Apontamentos | Arquivo |",
           "|-------------|-------------:|---------|"]
    for eq, out, n in gerados:
        idx.append(f"| {eq} | {n:,} | [{out.name}]({out.name}) |".replace(",", "."))
    (OUT_DIR / "INDEX.md").write_text("\n".join(idx) + "\n", encoding="utf-8")

    print(f"\n[OK] {len(gerados)} relatórios em {OUT_DIR}")
    return {"equipamentos": len(gerados), "diretorio": str(OUT_DIR)}


if __name__ == "__main__":
    main()
