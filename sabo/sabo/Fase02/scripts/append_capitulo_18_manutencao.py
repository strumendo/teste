"""
Anexa o Capítulo 18 (Histórico de Manutenção + Prescrição por Equipamento)
ao final de qualquer PDF de relatório SABO.

Conteúdo do Cap. 18:
  - 18.1 Introdução e metodologia (s07/s08)
  - 18.2 Tabela consolidada de prescrições (ordenada por urgência)
  - 18.3 Detalhe por equipamento (uma seção por equipamento):
         resumo, produção/refugo/retrabalho por composto na janela atual,
         perfil de desgaste cilindro/fuso, data prescrita e urgência.

Depende dos artefatos produzidos por:
  - s07_hist_manutencao.py  (equipamentos_historico_recente.csv,
                             equipamentos_historico_completo.csv,
                             equipamentos_janelas_manutencao.csv,
                             equipamentos_ociosidade.csv)
  - s08_prescricao_manutencao.py  (prescricao_manutencao.csv)

Uso:
    python append_capitulo_18_manutencao.py                   # último Relatorio_SABO_R*.pdf in-place
    python append_capitulo_18_manutencao.py --input X.pdf
    python append_capitulo_18_manutencao.py --input X.pdf --output Y.pdf
    python append_capitulo_18_manutencao.py --keep-original   # salva como <input>_com_cap18.pdf
"""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pypdf import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
)


SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"


SABO_ORANGE = colors.HexColor("#F77F00")
SABO_NAVY = colors.HexColor("#003366")
SABO_CARD_BG = colors.HexColor("#FFF8F0")


URGENCIA_COLOR = {
    "ATRASADO": colors.HexColor("#B00020"),
    "URGENTE": colors.HexColor("#E25822"),
    "ATENÇÃO": colors.HexColor("#F2A516"),
    "OK": colors.HexColor("#2E7D32"),
    "INDEFINIDO": colors.HexColor("#777777"),
}


def find_latest_report() -> Path:
    """Retorna o Relatorio_SABO_R<N>.pdf de maior N em outputs/."""
    pattern = re.compile(r"Relatorio_SABO_R(\d+)(?:_com_cap17)?\.pdf$")
    candidates = []
    for p in OUTPUTS_DIR.glob("Relatorio_SABO_R*.pdf"):
        m = pattern.search(p.name)
        if m:
            candidates.append((int(m.group(1)), "com_cap17" in p.name, p))
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum Relatorio_SABO_R*.pdf encontrado em {OUTPUTS_DIR}"
        )
    # Prefere o de maior N e, com N igual, o já com cap17.
    candidates.sort(key=lambda t: (t[0], t[1]))
    return candidates[-1][2]


def _load_artefatos() -> dict:
    """Carrega os CSVs necessários; aborta com erro explícito se faltar."""
    required = {
        "hist_recente": "equipamentos_historico_recente.csv",
        "hist_completo": "equipamentos_historico_completo.csv",
        "janelas": "equipamentos_janelas_manutencao.csv",
        "ociosidade": "equipamentos_ociosidade.csv",
        "prescricao": "prescricao_manutencao.csv",
    }
    out = {}
    missing = []
    for key, name in required.items():
        path = OUTPUTS_DIR / name
        if not path.exists():
            missing.append(name)
        else:
            out[key] = pd.read_csv(path)
    if missing:
        raise FileNotFoundError(
            f"Artefatos ausentes em {OUTPUTS_DIR}: {missing}. "
            "Execute s07_hist_manutencao.py e s08_prescricao_manutencao.py antes."
        )
    return out


# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------

def _grafico_producao_por_composto(janelas: pd.DataFrame, equip: str, tmpdir: Path) -> Path | None:
    """Barras agrupadas: Qtd Produzida / Refugada / Retrabalhada por composto na janela J3."""
    j3 = janelas[(janelas["equipamento"] == equip) & (janelas["janela"] == "J3_pos_ultima_troca")]
    if j3.empty or not j3.iloc[0].get("compostos"):
        return None
    compostos = [c for c in str(j3.iloc[0]["compostos"]).split(";") if c]
    if not compostos:
        return None

    # Como o s07 agregou por janela no nível total, aqui reabrimos o raw do equip
    # para gerar o detalhe por composto.
    raw_csv = OUTPUTS_DIR.parent / "data" / "raw" / f"{equip}.csv"
    if not raw_csv.exists():
        return None
    df = pd.read_csv(raw_csv)
    df = df.rename(columns={
        "Data de Produção": "data",
        "Qtd. Produzida": "produzida",
        "Qtd. Refugada": "refugada",
        "Qtd. Retrabalhada": "retrabalhada",
        "Descrição da massa (Composto)": "composto",
    })
    df["data"] = pd.to_datetime(df["data"], errors="coerce", format="ISO8601")
    if df["data"].isna().all():
        df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    # Filtra janela J3
    inicio = pd.to_datetime(j3.iloc[0]["inicio"], dayfirst=True, errors="coerce")
    fim = pd.to_datetime(j3.iloc[0]["fim"], dayfirst=True, errors="coerce")
    if pd.notna(inicio):
        df = df[df["data"] >= inicio]
    if pd.notna(fim):
        df = df[df["data"] < fim]
    if df.empty:
        return None
    agg = df.groupby("composto")[["produzida", "refugada", "retrabalhada"]].sum()
    agg = agg.sort_values("produzida", ascending=False).head(10)  # top-10
    if agg.empty:
        return None

    fig, ax = plt.subplots(figsize=(8.0, 3.8))
    x = range(len(agg.index))
    w = 0.26
    ax.bar([i - w for i in x], agg["produzida"], width=w, label="Produzida", color="#003366")
    ax.bar([i for i in x], agg["refugada"], width=w, label="Refugada", color="#F77F00")
    ax.bar([i + w for i in x], agg["retrabalhada"], width=w, label="Retrabalhada", color="#B00020")
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(c)[:22] for c in agg.index], rotation=30, ha="right", fontsize=7)
    ax.set_ylabel("Quantidade (peças)")
    ax.set_title(f"{equip} — Produção por Composto (janela atual, pós-última troca)", fontsize=10)
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = tmpdir / f"{equip}_prod_composto.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def _grafico_desgaste(hist_recente: pd.DataFrame, equip: str, tmpdir: Path) -> Path | None:
    """Barras das medições cilindro A-E e fuso A-D com linha da nominal (20 mm)."""
    row = hist_recente[hist_recente["equipamento"] == equip]
    if row.empty:
        return None
    r = row.iloc[0]
    cil = [("A", r.get("cil_a")), ("B", r.get("cil_b")), ("C", r.get("cil_c")),
           ("D", r.get("cil_d")), ("E", r.get("cil_e"))]
    fuso = [("A", r.get("fuso_a")), ("B", r.get("fuso_b")),
            ("C", r.get("fuso_c")), ("D", r.get("fuso_d"))]

    cil_vals = [(k, v) for k, v in cil if pd.notna(v)]
    fuso_vals = [(k, v) for k, v in fuso if pd.notna(v)]
    if not cil_vals and not fuso_vals:
        return None

    fig, axes = plt.subplots(1, 2, figsize=(8.0, 3.2), sharey=True)
    for ax, vals, titulo, cor in (
        (axes[0], cil_vals, "Cilindro (mm)", "#003366"),
        (axes[1], fuso_vals, "Fuso (mm)", "#F77F00"),
    ):
        if not vals:
            ax.set_visible(False)
            continue
        labels = [v[0] for v in vals]
        data = [float(v[1]) for v in vals]
        ax.bar(labels, data, color=cor)
        ax.axhline(20.0, color="#B00020", linestyle="--", linewidth=1, label="Nominal 20 mm")
        ax.set_title(titulo, fontsize=10)
        ax.set_ylim(min([19.0] + data) - 0.05, max([21.0] + data) + 0.05)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(loc="lower right", fontsize=7)
    fig.suptitle(f"{equip} — Perfil de Desgaste (leitura mais recente)", fontsize=10)
    fig.tight_layout()
    out = tmpdir / f"{equip}_desgaste.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _fmt_data(s) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)) or s == "":
        return "—"
    if isinstance(s, (date, datetime, pd.Timestamp)):
        return s.strftime("%d/%m/%Y")
    try:
        return datetime.strptime(str(s), "%d/%m/%Y").strftime("%d/%m/%Y")
    except ValueError:
        try:
            return datetime.strptime(str(s), "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            return str(s)


def _fmt_num(v, digits: int = 0) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    return f"{f:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _styles():
    base = getSampleStyleSheet()
    return {
        "base": base,
        "h1": ParagraphStyle(
            "CapH1", parent=base["Heading1"], fontSize=16, spaceBefore=20, spaceAfter=10,
            textColor=SABO_ORANGE,
        ),
        "h2": ParagraphStyle(
            "CapH2", parent=base["Heading2"], fontSize=13, spaceBefore=15, spaceAfter=8,
            textColor=SABO_NAVY,
        ),
        "h3": ParagraphStyle(
            "CapH3", parent=base["Heading3"], fontSize=11, spaceBefore=10, spaceAfter=6,
            textColor=SABO_NAVY,
        ),
        "body": ParagraphStyle(
            "CapBody", parent=base["Normal"], fontSize=10, alignment=TA_JUSTIFY,
            spaceAfter=8, leading=13,
        ),
        "small": ParagraphStyle(
            "CapSmall", parent=base["Normal"], fontSize=8, leading=10,
        ),
        "caption": ParagraphStyle(
            "CapCaption", parent=base["Normal"], fontSize=9, alignment=TA_CENTER,
            textColor=colors.HexColor("#555555"), spaceAfter=6,
        ),
    }


def _tabela_consolidada(prescricao: pd.DataFrame, styles) -> Table:
    header = ["Equip.", "Última troca", "T_base (d)", "Fat. desgaste",
              "Fat. massa", "Ociosid. (d)", "Data prescrita", "Dias rest.", "Urgência"]
    rows = [header]
    for _, r in prescricao.iterrows():
        rows.append([
            r.get("equipamento", ""),
            _fmt_data(r.get("data_ultima_sub")),
            _fmt_num(r.get("t_base_dias"), 0),
            _fmt_num(r.get("fator_desgaste"), 3),
            _fmt_num(r.get("fator_massa"), 3),
            _fmt_num(r.get("dias_ociosidade"), 0),
            _fmt_data(r.get("data_prescrita")),
            _fmt_num(r.get("dias_restantes"), 0),
            r.get("urgencia", "—"),
        ])
    t = Table(rows, colWidths=[1.6*cm, 2.2*cm, 1.8*cm, 2.0*cm, 1.8*cm, 2.0*cm, 2.4*cm, 1.8*cm, 2.0*cm])
    ts = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), SABO_ORANGE),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])
    # Colorir coluna urgência (última) por severidade
    for i, r in enumerate(rows[1:], start=1):
        cor = URGENCIA_COLOR.get(r[-1], colors.HexColor("#777777"))
        ts.add("BACKGROUND", (-1, i), (-1, i), cor)
        ts.add("TEXTCOLOR", (-1, i), (-1, i), colors.white)
        ts.add("FONTNAME", (-1, i), (-1, i), "Helvetica-Bold")
    t.setStyle(ts)
    return t


def _secao_equipamento(equip: str, idx: int, artefatos: dict, tmpdir: Path, styles) -> list:
    hist_rec = artefatos["hist_recente"]
    janelas = artefatos["janelas"]
    ociosidade = artefatos["ociosidade"]
    prescr = artefatos["prescricao"]

    row_rec = hist_rec[hist_rec["equipamento"] == equip]
    row_pre = prescr[prescr["equipamento"] == equip]
    jeq = janelas[janelas["equipamento"] == equip]
    ocio_row = ociosidade[ociosidade["equipamento"] == equip]

    if row_rec.empty or row_pre.empty:
        return []
    r = row_rec.iloc[0]
    p = row_pre.iloc[0]

    story = []
    story.append(Paragraph(f"18.3.{idx} — Equipamento {equip}", styles["h3"]))

    j1 = jeq[jeq["janela"] == "J1_inicio_ate_penultima"]
    j2 = jeq[jeq["janela"] == "J2_entre_trocas"]
    j3 = jeq[jeq["janela"] == "J3_pos_ultima_troca"]

    def _g(sub, col, default=0):
        if sub.empty:
            return default
        v = sub.iloc[0].get(col, default)
        return v if v is not None and not (isinstance(v, float) and pd.isna(v)) else default

    # Tabela-resumo do equipamento
    header = ["Marco / Métrica", "Valor"]
    linhas = [
        ["Data de início de produção (raw)", _fmt_data(_g(j1, "inicio") or _g(j2, "inicio"))],
        ["Data da penúltima substituição", _fmt_data(r.get("data_penultima_sub"))],
        ["Data da última substituição", _fmt_data(r.get("data_ultima_sub"))],
        ["Dias em operação (última janela fechada)", _fmt_num(r.get("dias_em_operacao"), 0)],
        ["Última data de produção no arquivo", _fmt_data(ocio_row.iloc[0]["ultima_producao"]) if not ocio_row.empty else "—"],
        ["Dias de ociosidade (até data de referência)", _fmt_num(p.get("dias_ociosidade"), 0)],
        ["Data prescrita da próxima manutenção", _fmt_data(p.get("data_prescrita"))],
        ["Dias restantes até a prescrição", _fmt_num(p.get("dias_restantes"), 0)],
        ["Urgência", p.get("urgencia", "—")],
    ]
    t_sum = Table([header] + linhas, colWidths=[8*cm, 8*cm])
    t_sum.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), SABO_NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BACKGROUND", (0, 1), (-1, -1), SABO_CARD_BG),
    ]))
    urg_idx = len(linhas)  # última linha (0-based: urg_idx == linhas.index + 1 na tabela com cabeçalho)
    t_sum.setStyle(TableStyle([
        ("BACKGROUND", (1, urg_idx), (1, urg_idx),
         URGENCIA_COLOR.get(p.get("urgencia", "—"), colors.HexColor("#777777"))),
        ("TEXTCOLOR", (1, urg_idx), (1, urg_idx), colors.white),
        ("FONTNAME", (1, urg_idx), (1, urg_idx), "Helvetica-Bold"),
    ]))
    story.append(t_sum)
    story.append(Spacer(1, 0.3*cm))

    # Tabela com totais por janela (produção/refugo/retrabalho/massa)
    jheader = ["Janela", "Período", "Dias", "Produzida", "Refugada", "Retrab.", "Massa (kg)"]
    jrows = [jheader]
    for nome, sub in (("Início → penúlt.", j1), ("Penúlt. → última", j2), ("Pós última troca", j3)):
        if sub.empty:
            jrows.append([nome, "—", "—", "—", "—", "—", "—"])
            continue
        s = sub.iloc[0]
        periodo = f"{_fmt_data(s.get('inicio'))} → {_fmt_data(s.get('fim'))}"
        jrows.append([
            nome, periodo,
            _fmt_num(s.get("dias_calendario"), 0),
            _fmt_num(s.get("qtd_produzida"), 0),
            _fmt_num(s.get("qtd_refugada"), 0),
            _fmt_num(s.get("qtd_retrabalhada"), 0),
            _fmt_num(s.get("massa_kg"), 1),
        ])
    t_jan = Table(jrows, colWidths=[3.2*cm, 4.4*cm, 1.4*cm, 2.0*cm, 1.8*cm, 1.8*cm, 2.0*cm])
    t_jan.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), SABO_ORANGE),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
        ("BACKGROUND", (0, 1), (-1, -1), SABO_CARD_BG),
    ]))
    story.append(t_jan)
    story.append(Spacer(1, 0.3*cm))

    # Gráficos
    img_prod = _grafico_producao_por_composto(janelas, equip, tmpdir)
    if img_prod:
        story.append(Image(str(img_prod), width=16*cm, height=7.6*cm))
        story.append(Paragraph(
            f"Figura {equip}-1: Produção/Refugo/Retrabalho por composto na janela atual (top-10 compostos).",
            styles["caption"]))

    img_des = _grafico_desgaste(artefatos["hist_recente"], equip, tmpdir)
    if img_des:
        story.append(Image(str(img_des), width=16*cm, height=6.4*cm))
        story.append(Paragraph(
            f"Figura {equip}-2: Perfil de desgaste do cilindro e fuso na leitura mais recente.",
            styles["caption"]))

    story.append(PageBreak())
    return story


def build_chapter_18_pdf(output_path: Path, tmpdir: Path) -> None:
    artefatos = _load_artefatos()
    prescricao = artefatos["prescricao"]

    # Ordena prescrição por urgência visual: ATRASADO > URGENTE > ATENÇÃO > OK > INDEFINIDO.
    ordem = {"ATRASADO": 0, "URGENTE": 1, "ATENÇÃO": 2, "OK": 3, "INDEFINIDO": 4}
    prescricao = prescricao.copy()
    prescricao["_o"] = prescricao["urgencia"].map(lambda u: ordem.get(u, 5))
    prescricao = prescricao.sort_values(["_o", "dias_restantes"]).drop(columns=["_o"]).reset_index(drop=True)
    artefatos["prescricao"] = prescricao

    doc = SimpleDocTemplate(
        str(output_path), pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
    )
    styles = _styles()
    story = []

    # ------------------ 18.1 Introdução ------------------
    story.append(Paragraph(
        "18. Histórico de Manutenção e Prescrição por Equipamento",
        styles["h1"],
    ))
    story.append(Paragraph(
        "Este capítulo complementa o relatório principal com uma visão por equipamento "
        "das substituições efetivas de peças (cilindro e fuso) e dos dados de produção "
        "associados a cada ciclo de manutenção. As datas são cruzadas com os arquivos "
        "de produção em <i>data/raw/IJ-*.csv</i> e com os arquivos de manutenção em "
        "<i>data/manutencao/Dados Manut*.xlsx</i>, incluindo o histórico de medições "
        "das peças substituídas. Todas as datas seguem o formato dd/mm/aaaa.",
        styles["body"]
    ))
    story.append(Paragraph("18.1 Metodologia", styles["h2"]))
    story.append(Paragraph(
        "O pipeline de manutenção é composto por dois scripts desacoplados:",
        styles["body"]
    ))
    story.append(Paragraph(
        "• <b>s07_hist_manutencao.py</b> — lê todos os arquivos "
        "<i>Dados Manut*.xlsx</i> (2025, 2026, …), preserva cada leitura como uma "
        "fotografia histórica, converte seriais Excel em datas e gera amplitudes e "
        "desvios das medições de cilindro/fuso. Em seguida cruza com os CSVs de "
        "produção para obter, por equipamento, as agregações de cada janela: "
        "<i>Início→Penúltima</i>, <i>Penúltima→Última</i> e <i>Pós última troca</i>.",
        styles["body"]
    ))
    story.append(Paragraph(
        "• <b>s08_prescricao_manutencao.py</b> — produz a data prescrita da próxima "
        "manutenção integrando três sinais: (1) baseline, dada pela mediana dos "
        "dias em operação do histórico do próprio equipamento; (2) fator de desgaste, "
        "derivado da comparação entre a amplitude atual (máx − mín) das medições de "
        "cilindro/fuso e a amplitude mediana histórica do mesmo equipamento; (3) fator "
        "de consumo de massa, razão entre o kg de massa consumido na janela atual e "
        "a mediana de kg das janelas anteriores. Ao resultado soma-se o número de "
        "dias de ociosidade no final da série — máquina parada não desgasta, então "
        "o prazo desliza para frente.",
        styles["body"]
    ))
    story.append(Paragraph(
        "<b>Fórmula resumida:</b><br/>"
        "<i>T_prescrito = mediana(dias_operação) × fator_desgaste × fator_massa</i><br/>"
        "<i>data_prescrita = data_última_substituição + T_prescrito + dias_ociosidade</i><br/>"
        "Os fatores são suavizados e clampados em "
        "[0,60; 1,20] (desgaste) e [0,70; 1,30] (massa), de modo que extremos de "
        "histórico curto ou de variação esporádica não produzam prazos irrealistas.",
        styles["body"]
    ))

    story.append(PageBreak())

    # ------------------ 18.2 Tabela consolidada ------------------
    story.append(Paragraph("18.2 Prescrição Consolidada (todos os equipamentos)", styles["h2"]))
    story.append(Paragraph(
        "Tabela ordenada por urgência (equipamentos atrasados primeiro). "
        "Fatores próximos de 1,0 indicam que o ciclo atual está alinhado com o "
        "histórico do próprio equipamento; valores abaixo de 1,0 encurtam o prazo "
        "prescrito (mais desgaste ou mais consumo de massa que o típico).",
        styles["body"]
    ))
    story.append(_tabela_consolidada(prescricao, styles))
    story.append(PageBreak())

    # ------------------ 18.3 Detalhe por equipamento ------------------
    story.append(Paragraph("18.3 Detalhamento por Equipamento", styles["h2"]))
    story.append(Paragraph(
        "Cada subseção apresenta, para um equipamento: um resumo com as datas de "
        "substituição e a data prescrita; os totais de produção/refugo/retrabalho e "
        "massa por janela; um gráfico comparando Produzida/Refugada/Retrabalhada por "
        "composto na janela atual (pós-última troca); e o perfil atual de desgaste do "
        "cilindro e do fuso.",
        styles["body"]
    ))

    # Segue a ordem da prescrição consolidada (urgência primeiro).
    for i, equip in enumerate(prescricao["equipamento"].tolist(), start=1):
        story.extend(_secao_equipamento(equip, i, artefatos, tmpdir, styles))

    doc.build(story)


def merge_pdfs(base_pdf: Path, addition_pdf: Path, output_pdf: Path) -> None:
    writer = PdfWriter()
    for page in PdfReader(str(base_pdf)).pages:
        writer.add_page(page)
    for page in PdfReader(str(addition_pdf)).pages:
        writer.add_page(page)
    with open(output_pdf, "wb") as f:
        writer.write(f)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Anexa o Capítulo 18 (manutenção por equipamento) ao final de um PDF SABO."
    )
    parser.add_argument("--input", type=Path, default=None,
                        help="PDF base. Default: último Relatorio_SABO_R*.pdf em outputs/.")
    parser.add_argument("--output", type=Path, default=None,
                        help="PDF de saída. Default: sobrescreve --input.")
    parser.add_argument("--keep-original", action="store_true",
                        help="Não sobrescreve o input; salva como <input>_com_cap18.pdf.")
    args = parser.parse_args()

    input_pdf = args.input or find_latest_report()
    if not input_pdf.exists():
        print(f"PDF de entrada não encontrado: {input_pdf}", file=sys.stderr)
        return 1

    if args.output:
        output_pdf = args.output
    elif args.keep_original:
        output_pdf = input_pdf.with_name(input_pdf.stem + "_com_cap18.pdf")
    else:
        output_pdf = input_pdf

    print(f"  Entrada: {input_pdf}")
    print(f"  Saída:   {output_pdf}")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        cap18_pdf = tmp_path / "_cap18.pdf"
        build_chapter_18_pdf(cap18_pdf, tmp_path)

        merged_tmp = tmp_path / "_merged.pdf"
        merge_pdfs(input_pdf, cap18_pdf, merged_tmp)
        merged_tmp.replace(output_pdf)

    print("  OK — Capítulo 18 anexado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
