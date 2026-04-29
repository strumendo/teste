"""
Anexa o Capítulo 17 (Detalhamento das Fórmulas do Capítulo 11) ao final de
qualquer PDF de relatório SABO.

Importante: o conteúdo do Capítulo 17 é FIXO e referente ao snapshot do R21
(modelo XGBoost, R² = 0,9538). Não é regenerado a partir do estado atual do
pipeline — isto é intencional, para que mudanças de modelo em execuções
futuras (ex.: XGBoost deixar de ser o melhor) não tornem o capítulo
inconsistente.

Uso:
    python append_capitulo_17.py                    # Detecta o último Relatorio_SABO_R*.pdf e anexa in-place
    python append_capitulo_17.py --input X.pdf      # Anexa ao PDF informado (sobrescreve)
    python append_capitulo_17.py --input X.pdf --output Y.pdf
    python append_capitulo_17.py --keep-original    # Não sobrescreve; salva com sufixo _com_cap17.pdf
"""

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.platypus import (
    Paragraph, SimpleDocTemplate, Spacer, PageBreak, Table, TableStyle
)
from pypdf import PdfReader, PdfWriter


# Caminho para outputs/ (relativo ao script)
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"


def find_latest_report() -> Path:
    """Retorna o Relatorio_SABO_R<N>.pdf de maior N em outputs/."""
    pattern = re.compile(r"Relatorio_SABO_R(\d+)\.pdf$")
    candidates = []
    for p in OUTPUTS_DIR.glob("Relatorio_SABO_R*.pdf"):
        m = pattern.search(p.name)
        if m:
            candidates.append((int(m.group(1)), p))
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum Relatorio_SABO_R*.pdf encontrado em {OUTPUTS_DIR}"
        )
    candidates.sort()
    return candidates[-1][1]


def build_chapter_17_pdf(output_path: Path) -> None:
    """Gera um PDF contendo apenas o Capítulo 17."""
    doc = SimpleDocTemplate(
        str(output_path), pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )
    styles = getSampleStyleSheet()

    heading1_style = ParagraphStyle(
        'CustomH1', parent=styles['Heading1'], fontSize=16,
        spaceBefore=20, spaceAfter=10,
        textColor=colors.HexColor('#F77F00'),
    )
    heading2_style = ParagraphStyle(
        'CustomH2', parent=styles['Heading2'], fontSize=14,
        spaceBefore=15, spaceAfter=8,
        textColor=colors.HexColor('#333333'),
    )
    body_style = ParagraphStyle(
        'CustomBody', parent=styles['Normal'], fontSize=11,
        alignment=TA_JUSTIFY, spaceAfter=12, leading=14,
    )
    formula_style = ParagraphStyle(
        'FormulaStyle', parent=body_style,
        leftIndent=20, rightIndent=20,
        spaceBefore=6, spaceAfter=10,
        fontName='Helvetica-Oblique',
        textColor=colors.HexColor('#003366'),
        backColor=colors.HexColor('#F0F4FA'),
        borderPadding=8,
    )

    story = []

    story.append(Paragraph("17. Detalhamento das Fórmulas do Capítulo 11", heading1_style))
    story.append(Paragraph(
        "Esta seção complementa o Capítulo 11 detalhando, em linguagem descritiva, todas as "
        "fórmulas usadas para gerar as prescrições de troca de peças. <b>Conteúdo fixo, baseado no "
        "snapshot do Relatório R20 (modelo XGBoost, R² = 0,9534, MSE = 1094,50, MAE = 20,02).</b> "
        "Cada fórmula é apresentada com seu propósito, descrição em linguagem natural e exemplo "
        "numérico baseado nos dados reais daquele relatório.",
        body_style
    ))

    # ---------- 17.1 ----------
    story.append(Paragraph("17.1 Fórmulas da Prescrição Histórica (referente a 11.1)", heading2_style))
    story.append(Paragraph(
        "<b>Para que serve:</b> Estimar a próxima troca usando apenas o histórico de intervalos "
        "entre trocas anteriores, sem considerar o estado atual do equipamento. Funciona como "
        "linha de base de comparação.",
        body_style
    ))

    story.append(Paragraph("<b>Fórmula 1 — Data prescrita da próxima troca</b>", body_style))
    story.append(Paragraph(
        "Data Prescrita da Próxima Troca é igual à Data da Última Troca já realizada no equipamento, "
        "somada ao Intervalo Médio Histórico em dias (a média de dias entre as trocas anteriores "
        "daquele mesmo equipamento).",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 2 — Quantos dias ainda faltam</b>", body_style))
    story.append(Paragraph(
        "Dias Restantes até a Próxima Troca é igual à diferença entre a Data Prescrita da Próxima "
        "Troca (Fórmula 1) e a Data de Hoje, expressa em número inteiro de dias. Quando o "
        "resultado é negativo, o equipamento está em situação de ATRASADO.",
        formula_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-044 (situação OK):</b>", body_style))
    story.append(Paragraph(
        "Última Troca = 08/11/2025; Intervalo Médio Histórico = 531 dias; Data de Hoje = 14/04/2026.<br/>"
        "Aplicando a Fórmula 1: 08/11/2025 mais 531 dias dá <b>23/04/2027</b>.<br/>"
        "Aplicando a Fórmula 2: de 14/04/2026 até 23/04/2027 são <b>373 dias</b> restantes (positivo, "
        "logo dentro do prazo).",
        body_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-046 (situação ATRASADO):</b>", body_style))
    story.append(Paragraph(
        "Última Troca = 05/01/2025; Intervalo Médio Histórico = 343 dias; Data de Hoje = 14/04/2026.<br/>"
        "Aplicando a Fórmula 1: 05/01/2025 mais 343 dias dá <b>14/12/2025</b> (data já passou).<br/>"
        "Aplicando a Fórmula 2: de 14/04/2026 até 14/12/2025 são <b>−122 dias</b> (troca atrasada em "
        "122 dias segundo o critério histórico).",
        body_style
    ))

    story.append(PageBreak())

    # ---------- 17.2 ----------
    story.append(Paragraph("17.2 Fórmulas da Prescrição via Modelo ML (referente a 11.2)", heading2_style))
    story.append(Paragraph(
        "<b>Para que serve:</b> Prescrever a próxima troca usando o modelo XGBoost (R² = 0,9534) "
        "treinado sobre o histórico de operação. Considera o estado atual do equipamento — "
        "produção acumulada, desgastes, refugo, medições de cilindro e fuso.",
        body_style
    ))

    story.append(Paragraph("<b>Fórmula 3 — Dias prescritos pelo modelo</b>", body_style))
    story.append(Paragraph(
        "Dias Prescritos pelo Modelo ML é o valor numérico devolvido pelo modelo XGBoost quando "
        "recebe como entrada o último estado conhecido do equipamento (último registro disponível "
        "no arquivo data_eda.csv, contendo as 22 variáveis usadas no treinamento). O modelo "
        "aprendeu, durante o treino, a relação entre essas variáveis e o número de dias até a "
        "próxima manutenção. Caso o modelo retorne valor negativo, ele é forçado para zero.",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 4 — Data prescrita pelo modelo</b>", body_style))
    story.append(Paragraph(
        "Data Prescrita da Próxima Troca pelo Modelo é igual à Data de Hoje somada ao número de "
        "Dias Prescritos pelo Modelo ML (Fórmula 3).",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 5 — Classificação de status</b>", body_style))
    story.append(Paragraph(
        "Se o número de dias prescritos é <b>menor que zero</b>, o status é <b>ATRASADO</b> "
        "(a troca já deveria ter ocorrido).<br/>"
        "Se o número de dias prescritos é <b>igual ou menor que 30</b>, o status é "
        "<b>URGENTE</b> (resta menos de um mês).<br/>"
        "Se o número de dias prescritos é <b>maior que 30 e menor ou igual a 90</b>, o status é "
        "<b>ATENÇÃO</b> (entre um e três meses).<br/>"
        "Se o número de dias prescritos é <b>maior que 90</b>, o status é <b>OK</b> (mais de três "
        "meses de margem).",
        formula_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-119 (URGENTE):</b>", body_style))
    story.append(Paragraph(
        "O modelo XGBoost analisou o último registro do IJ-119 e devolveu <b>3 dias</b>.<br/>"
        "Aplicando a Fórmula 4: 14/04/2026 mais 3 dias dá <b>17/04/2026</b>.<br/>"
        "Aplicando a Fórmula 5: 3 dias está entre 0 e 30 → status <b>URGENTE</b>.",
        body_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-044 (OK):</b>", body_style))
    story.append(Paragraph(
        "O modelo devolveu <b>380 dias</b> para o IJ-044.<br/>"
        "Aplicando a Fórmula 4: 14/04/2026 mais 380 dias dá <b>29/04/2027</b>.<br/>"
        "Aplicando a Fórmula 5: 380 dias é maior que 90 → status <b>OK</b>.",
        body_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-118 (URGENTE):</b>", body_style))
    story.append(Paragraph(
        "O modelo devolveu <b>18 dias</b> para o IJ-118.<br/>"
        "Aplicando a Fórmula 4: 14/04/2026 mais 18 dias dá <b>02/05/2026</b>.<br/>"
        "Aplicando a Fórmula 5: 18 dias está entre 0 e 30 → status <b>URGENTE</b>.",
        body_style
    ))

    story.append(PageBreak())

    # ---------- 17.3 ----------
    story.append(Paragraph("17.3 Fórmulas da Comparação Histórico vs. ML (referente a 11.3)", heading2_style))
    story.append(Paragraph(
        "<b>Para que serve:</b> Confrontar os dois métodos para identificar quando o modelo "
        "recomenda antecipar ou adiar a manutenção em relação ao calendário histórico, e quando "
        "ambos concordam.",
        body_style
    ))

    story.append(Paragraph("<b>Fórmula 6 — Diferença entre os dois métodos</b>", body_style))
    story.append(Paragraph(
        "Diferença em Dias é igual ao número de Dias Prescritos pelo Modelo ML (Fórmula 3) menos "
        "o número de Dias Restantes pelo Histórico (Fórmula 2). Resultado positivo significa "
        "que o modelo prescreve mais tempo de operação que o histórico; negativo, o oposto.",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 7 — Recomendação operacional</b>", body_style))
    story.append(Paragraph(
        "Se a Diferença em Dias é <b>menor que −30</b>, a recomendação é <b>Antecipar</b> "
        "(o modelo aponta degradação acima do esperado).<br/>"
        "Se a Diferença em Dias é <b>maior que +30</b>, a recomendação é <b>Pode adiar</b> "
        "(equipamento em condições melhores que o histórico sugere).<br/>"
        "Se a Diferença em Dias está <b>entre −30 e +30 inclusive</b>, a recomendação é "
        "<b>Conforme</b> (os dois métodos concordam dentro de uma tolerância de um mês).",
        formula_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-046 (Pode adiar):</b>", body_style))
    story.append(Paragraph(
        "Dias Prescritos pelo Modelo ML = 18; Dias Restantes pelo Histórico = −122.<br/>"
        "Aplicando a Fórmula 6: 18 menos (−122) dá <b>+140 dias</b> de diferença.<br/>"
        "Aplicando a Fórmula 7: +140 é maior que +30 → recomendação <b>Pode adiar</b>.<br/>"
        "<i>Interpretação: o histórico marcaria o IJ-046 como atrasado, mas o modelo, ao analisar "
        "o estado real do equipamento, percebe que ele ainda tem margem de operação.</i>",
        body_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Equipamento IJ-134 (Conforme):</b>", body_style))
    story.append(Paragraph(
        "Dias Prescritos pelo Modelo ML = 301; Dias Restantes pelo Histórico = 272.<br/>"
        "Aplicando a Fórmula 6: 301 menos 272 dá <b>+29 dias</b> de diferença.<br/>"
        "Aplicando a Fórmula 7: +29 está entre −30 e +30 → recomendação <b>Conforme</b>.",
        body_style
    ))

    story.append(PageBreak())

    # ---------- 17.4 ----------
    story.append(Paragraph("17.4 Fórmulas dos Fatores Considerados pelo Modelo (referente a 11.4)", heading2_style))
    story.append(Paragraph(
        "Esta seção descreve as fórmulas das variáveis derivadas que alimentam o modelo XGBoost. "
        "São os \"olhos\" do modelo: cada uma traduz um aspecto físico do equipamento em um "
        "número que o algoritmo consegue processar.",
        body_style
    ))

    story.append(Paragraph("<b>Fórmula 8 — Desgaste do Cilindro</b>", body_style))
    story.append(Paragraph(
        "Desgaste do Cilindro (em milímetros) é igual à Maior Medição de Cilindro registrada na "
        "ordem de produção menos o valor nominal de referência <b>20,0 mm</b> (diâmetro de "
        "fábrica considerado como ponto zero do desgaste).",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 9 — Desgaste do Fuso</b>", body_style))
    story.append(Paragraph(
        "Desgaste do Fuso (em milímetros) é igual ao valor nominal de referência <b>20,0 mm</b> "
        "menos a Menor Medição de Fuso registrada na ordem de produção. A lógica é inversa à do "
        "cilindro: o fuso novo tem 20 mm e vai diminuindo com o uso.",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 10 — Variação dimensional</b>", body_style))
    story.append(Paragraph(
        "Variação do Cilindro é igual à Maior Medição menos a Menor Medição de cilindro daquela "
        "ordem de produção. A Variação do Fuso é calculada da mesma forma para o fuso. Mede a "
        "oscilação dimensional dentro de uma mesma ordem.",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 11 — Taxa de Desgaste</b>", body_style))
    story.append(Paragraph(
        "Taxa de Desgaste do Cilindro é igual ao Desgaste do Cilindro (Fórmula 8) dividido pelo "
        "Intervalo de Manutenção em dias daquele equipamento. A Taxa de Desgaste do Fuso segue a "
        "mesma lógica usando a Fórmula 9. Resultado: desgaste por dia de operação.",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 12 — Índice de Desgaste Combinado (escala 0 a 100)</b>", body_style))
    story.append(Paragraph(
        "Índice de Desgaste é a soma ponderada de dois sub-scores:<br/>"
        "1. <b>Sub-score do Cilindro</b>: Desgaste do Cilindro (Fórmula 8) dividido por <b>0,6 mm</b>, "
        "multiplicado por 100, limitado em 100 (qualquer desgaste acima de 0,6 mm é considerado 100% — crítico).<br/>"
        "2. <b>Sub-score do Fuso</b>: Desgaste do Fuso (Fórmula 9) dividido por <b>2,0 mm</b>, "
        "multiplicado por 100, limitado em 100.<br/>"
        "3. O índice final dá <b>peso 60% para o cilindro</b> e <b>peso 40% para o fuso</b>, "
        "refletindo a maior criticidade do desgaste do cilindro nas extrusoras Y125.",
        formula_style
    ))

    story.append(Paragraph("<b>Exemplo numérico — Cálculo do Índice de Desgaste:</b>", body_style))
    story.append(Paragraph(
        "Suponha um equipamento com leituras: cilindro_max = 20,30 mm e fuso_min = 19,40 mm.<br/>"
        "Aplicando a Fórmula 8: 20,30 menos 20,0 dá <b>0,30 mm</b> de desgaste do cilindro.<br/>"
        "Aplicando a Fórmula 9: 20,0 menos 19,40 dá <b>0,60 mm</b> de desgaste do fuso.<br/>"
        "Sub-score do cilindro: 0,30 dividido por 0,6 dá 0,5; vezes 100 dá <b>50</b>.<br/>"
        "Sub-score do fuso: 0,60 dividido por 2,0 dá 0,3; vezes 100 dá <b>30</b>.<br/>"
        "Índice combinado: 50 vezes 0,6 dá 30; mais 30 vezes 0,4 dá 12; somando dá "
        "<b>Índice de Desgaste = 42</b> (desgaste moderado).",
        body_style
    ))

    story.append(PageBreak())

    story.append(Paragraph("<b>Fórmula 13 — Desgaste por 1000 peças</b>", body_style))
    story.append(Paragraph(
        "Desgaste por 1000 peças é igual ao Desgaste acumulado do equipamento (medido pelas "
        "Fórmulas 8 e 9 no último registro) dividido pela Quantidade Produzida Acumulada desde a "
        "última troca, multiplicado por 1000 para normalizar. Permite comparar equipamentos com "
        "volumes de produção muito diferentes.",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 14 — Variáveis Acumuladas (4 features)</b>", body_style))
    story.append(Paragraph(
        "Para cada uma das variáveis Quantidade Produzida, Quantidade Refugada, Quantidade "
        "Retrabalhada e Consumo de Massa em Kg por 100 peças, a versão Acumulada é a soma "
        "cumulativa, ordenada por data, agrupada por equipamento.<br/>"
        "<i>Exemplo: se o IJ-044 produziu 1000 peças em 01/01, 800 em 02/01 e 1200 em 03/01, a "
        "coluna Qtd_Produzida_Acumulado terá 1000, 1800 e 3000 respectivamente. Essas variáveis "
        "dão ao modelo a noção de \"quanto desse equipamento já foi gasto desde a última troca\".</i>",
        formula_style
    ))

    story.append(Paragraph("<b>Fórmula 15 — Taxa de Refugo / Retrabalho</b>", body_style))
    story.append(Paragraph(
        "Taxa de Refugo é a razão entre a Quantidade Refugada Acumulada e a Quantidade Produzida "
        "Acumulada, geralmente expressa em percentual (multiplicada por 100). A Taxa de "
        "Retrabalho segue a mesma fórmula usando a Quantidade Retrabalhada Acumulada no "
        "numerador. Refugo e retrabalho crescentes ao longo do tempo sinalizam degradação do "
        "equipamento.",
        formula_style
    ))

    story.append(Paragraph(
        "<b>Resumo dos 6 fatores principais (como aparecem no Capítulo 11.4):</b>",
        body_style
    ))

    fatores_table_data = [
        ["Fator (PDF Cap. 11.4)", "Fórmula derivada", "O que capta"],
        ["Produção Acumulada", "Fórmula 14", "Volume total operado desde a troca"],
        ["Índice de Desgaste", "Fórmula 12", "Score 0–100 de desgaste físico"],
        ["Medições de Cilindro", "Fórmulas 8 e 10", "Estado dimensional do cilindro"],
        ["Medições de Fuso", "Fórmulas 9 e 10", "Estado dimensional do fuso"],
        ["Taxa de Refugo/Retrabalho", "Fórmula 15", "Indicador de qualidade"],
        ["Consumo de Massa", "Fórmula 14", "Eficiência de matéria-prima"],
        ["Taxa de Desgaste", "Fórmulas 11 e 13", "Desgaste por dia ou por peça"],
    ]
    fatores_table = Table(fatores_table_data, colWidths=[5*cm, 4*cm, 7*cm])
    fatores_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F77F00')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#FFF8F0')),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(fatores_table)
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph(
        "<b>Origem das fórmulas no código:</b><br/>"
        "• Fórmulas 1, 2, 3, 4, 5, 6, 7 → Fase02/scripts/s06_generate_report.py "
        "(função generate_chapter_11 e predict_maintenance_with_ml).<br/>"
        "• Fórmulas 8 a 15 → Fase02/scripts/s02_preprocessing.py "
        "(funções _calc_indice_desgaste, _calc_desgaste_por_pecas e bloco de cálculo das "
        "medições de cilindro/fuso).",
        body_style
    ))

    doc.build(story)


def merge_pdfs(base_pdf: Path, addition_pdf: Path, output_pdf: Path) -> None:
    """Mescla addition_pdf ao final de base_pdf, salvando em output_pdf."""
    writer = PdfWriter()
    for page in PdfReader(str(base_pdf)).pages:
        writer.add_page(page)
    for page in PdfReader(str(addition_pdf)).pages:
        writer.add_page(page)
    with open(output_pdf, "wb") as f:
        writer.write(f)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Anexa o Capítulo 17 (fórmulas do snapshot R21) ao final de um PDF SABO."
    )
    parser.add_argument(
        "--input", type=Path, default=None,
        help="PDF base. Default: último Relatorio_SABO_R*.pdf em outputs/."
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="PDF de saída. Default: sobrescreve --input."
    )
    parser.add_argument(
        "--keep-original", action="store_true",
        help="Não sobrescreve o input; salva como <input>_com_cap17.pdf."
    )
    args = parser.parse_args()

    input_pdf = args.input or find_latest_report()
    if not input_pdf.exists():
        print(f"❌ PDF de entrada não encontrado: {input_pdf}", file=sys.stderr)
        return 1

    if args.output:
        output_pdf = args.output
    elif args.keep_original:
        output_pdf = input_pdf.with_name(input_pdf.stem + "_com_cap17.pdf")
    else:
        output_pdf = input_pdf

    print(f"  Entrada: {input_pdf}")
    print(f"  Saída:   {output_pdf}")

    with tempfile.TemporaryDirectory() as tmp:
        cap17_pdf = Path(tmp) / "_cap17.pdf"
        build_chapter_17_pdf(cap17_pdf)

        # Mesclar para um arquivo temporário primeiro (caso input == output)
        merged_tmp = Path(tmp) / "_merged.pdf"
        merge_pdfs(input_pdf, cap17_pdf, merged_tmp)

        os.replace(merged_tmp, output_pdf)

    print(f"  ✓ Capítulo 17 anexado com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
