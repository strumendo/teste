"""
S06 - Geração de Relatório PDF
==============================
Etapa 6 do Pipeline - Geração do Relatório de Entrega

O QUE FAZ:
- Gera relatório PDF completo no formato padrão SABO
- Inclui todas as seções: Resumo, Metodologia, Modelos, Métricas, Resultados
- Incorpora gráficos gerados nas etapas anteriores
- Documenta variáveis, desempenho e recomendações

ENTRADA:
- best_model.joblib (modelo selecionado)
- evaluation_report.txt (métricas)
- eda_report.txt (estatísticas)
- eda_plots/*.png (gráficos)
- data_eda.csv (dados processados)

SAÍDA:
- Relatorio_SABO_RX.pdf: Relatório completo de entrega
"""

import os
import json
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Verificar disponibilidade de bibliotecas para PDF
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm, mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, Image, ListFlowable, ListItem
    )
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
    print("⚠ ReportLab não disponível. Gerando relatório em formato texto.")

import pandas as pd
import numpy as np
import joblib


def get_report_version() -> str:
    """
    Determina a versão do relatório baseado em arquivos existentes.

    Returns:
        String com versão (R5, R6, etc.)
    """
    existing = list(Path(".").glob("Relatorio_SABO_R*.pdf"))
    if not existing:
        return "R5"  # Próximo após R4

    versions = []
    for f in existing:
        try:
            v = int(f.stem.split("_R")[-1])
            versions.append(v)
        except:
            pass

    if versions:
        return f"R{max(versions) + 1}"
    return "R5"


def load_pipeline_results() -> dict:
    """
    Carrega todos os resultados das etapas anteriores do pipeline.

    Returns:
        Dicionário com todos os dados para o relatório
    """
    results = {
        "data": {},
        "model": {},
        "metrics": {},
        "eda": {},
        "plots": [],
    }

    # Carregar modelo
    if Path("best_model.joblib").exists():
        model_data = joblib.load("best_model.joblib")
        if isinstance(model_data, dict):
            results["model"] = model_data
        else:
            results["model"] = {"model": model_data, "name": "unknown"}

    # Carregar dados
    if Path("data_eda.csv").exists():
        df = pd.read_csv("data_eda.csv")
        results["data"]["shape"] = df.shape
        results["data"]["columns"] = list(df.columns)
        results["data"]["dtypes"] = df.dtypes.to_dict()

        # Estatísticas básicas
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        results["data"]["stats"] = df[numeric_cols].describe().to_dict()

    # Carregar relatório de EDA
    if Path("eda_report.txt").exists():
        with open("eda_report.txt", "r", encoding="utf-8") as f:
            results["eda"]["report"] = f.read()

    # Carregar relatório de avaliação
    if Path("evaluation_report.txt").exists():
        with open("evaluation_report.txt", "r", encoding="utf-8") as f:
            results["metrics"]["report"] = f.read()

    # Listar gráficos
    plots_dir = Path("eda_plots")
    if plots_dir.exists():
        results["plots"] = list(plots_dir.glob("*.png"))

    # Carregar dados de treino/teste
    if Path("train_test_split.npz").exists():
        data = np.load("train_test_split.npz", allow_pickle=True)
        results["data"]["train_size"] = len(data["X_train"])
        results["data"]["test_size"] = len(data["X_test"])
        results["data"]["features"] = data["feature_names"].tolist()

    return results


def generate_pdf_report(results: dict, output_path: str) -> str:
    """
    Gera relatório em PDF usando ReportLab.

    Args:
        results: Dados do pipeline
        output_path: Caminho do arquivo PDF

    Returns:
        Caminho do arquivo gerado
    """
    if not HAS_REPORTLAB:
        return generate_text_report(results, output_path.replace(".pdf", ".txt"))

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=2*cm,
        leftMargin=2*cm,
        topMargin=2*cm,
        bottomMargin=2*cm
    )

    # Estilos
    styles = getSampleStyleSheet()

    # Estilos personalizados
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#333333')
    )

    heading1_style = ParagraphStyle(
        'CustomH1',
        parent=styles['Heading1'],
        fontSize=16,
        spaceBefore=20,
        spaceAfter=10,
        textColor=colors.HexColor('#F77F00')  # Laranja do logo Taking
    )

    heading2_style = ParagraphStyle(
        'CustomH2',
        parent=styles['Heading2'],
        fontSize=14,
        spaceBefore=15,
        spaceAfter=8,
        textColor=colors.HexColor('#333333')
    )

    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['Normal'],
        fontSize=11,
        alignment=TA_JUSTIFY,
        spaceAfter=12,
        leading=14
    )

    # Construir documento
    story = []

    # === CAPA ===
    story.append(Spacer(1, 3*cm))
    story.append(Paragraph("Relatório de Entrega", title_style))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph("SABO - Pipeline de Machine Learning", styles['Heading2']))
    story.append(Paragraph("Manutenção Preditiva para Extrusoras de Borracha Y125", styles['Normal']))
    story.append(Spacer(1, 2*cm))
    story.append(Paragraph(f"Data: {datetime.now().strftime('%d/%m/%Y')}", styles['Normal']))
    story.append(PageBreak())

    # === SUMÁRIO ===
    story.append(Paragraph("Sumário", heading1_style))
    sumario_items = [
        "Resumo",
        "1. Introdução",
        "2. Descrição do Método Utilizado",
        "3. Ferramentas Utilizadas",
        "4. Modelos Estocásticos e Estatísticos Utilizados",
        "5. Métricas",
        "6. Variáveis Testadas e a Relevância de Cada Variável",
        "7. Dados Removidos da Análise",
        "8. Desempenho de Modelos",
        "9. Considerações",
        "10. Próximos Passos",
        "11. Recomendações",
        "12. Considerações Finais",
        "Referência Bibliográfica"
    ]
    for item in sumario_items:
        story.append(Paragraph(f"• {item}", styles['Normal']))
    story.append(PageBreak())

    # === RESUMO ===
    story.append(Paragraph("Resumo", heading1_style))

    # Extrair métricas do modelo
    model_name = results.get("model", {}).get("name", "Não identificado")
    metrics = results.get("model", {}).get("metrics", {})
    r2 = metrics.get("r2", 0)
    mse = metrics.get("mse", 0)
    mae = metrics.get("mae", 0)

    data_shape = results.get("data", {}).get("shape", (0, 0))
    train_size = results.get("data", {}).get("train_size", 0)
    test_size = results.get("data", {}).get("test_size", 0)

    resumo_text = f"""
    Este relatório apresenta uma visão abrangente do projeto desenvolvido para prever a
    necessidade de manutenção em extrusoras de borracha modelo Y125, utilizando dados
    coletados em ambiente industrial. A iniciativa surgiu da observação de que, durante as
    manutenções preventivas, diversos componentes demonstraram uma vida útil
    significativamente maior que a inicialmente prevista pelo planejamento tradicional.

    Para viabilizar essa análise, foram coletados e organizados dados provenientes de
    múltiplos arquivos CSV, cada um associado a uma extrusora específica, contendo
    informações como quantidade produzida, refugada e retrabalhada, consumo de
    matéria-prima, datas de produção, códigos dos produtos e outros atributos operacionais.

    O dataset consolidado contém {data_shape[0]} registros e {data_shape[1]} variáveis.
    Foi realizada a divisão dos dados em conjuntos de treinamento ({train_size} amostras, 80%)
    e teste ({test_size} amostras, 20%) para avaliação do poder preditivo dos modelos.

    Os resultados indicaram desempenho promissor. O modelo {model_name.upper()} foi
    selecionado como melhor modelo, apresentando R² de {r2:.4f}, MSE de {mse:.2f} e
    MAE de {mae:.2f}. A análise de importância das variáveis também apontou a relevância
    do "Cód. Ordem", fator que possivelmente encapsula diferenças operacionais.
    """
    story.append(Paragraph(resumo_text, body_style))
    story.append(PageBreak())

    # === 1. INTRODUÇÃO ===
    story.append(Paragraph("1. Introdução", heading1_style))
    intro_text = """
    A indústria moderna enfrenta o desafio constante de equilibrar segurança operacional e
    eficiência econômica. No contexto das extrusoras de borracha modelo Y125, observou-se
    que muitos componentes estavam sendo substituídos seguindo cronogramas fixos de
    manutenção preventiva, mesmo quando ainda apresentavam condições adequadas de
    funcionamento. Este cenário resulta em subutilização de peças, gerando custos
    desnecessários com componentes, mão de obra e tempo de máquina parada para
    intervenções prematuras.

    O objetivo deste projeto foi desenvolver e avaliar um modelo prescritivo capaz não apenas
    de prever com maior precisão o momento ideal para realizar a manutenção, mas também
    de recomendar intervenções específicas baseadas em dados, maximizando a vida útil dos
    componentes sem comprometer a confiabilidade do sistema produtivo.
    """
    story.append(Paragraph(intro_text, body_style))
    story.append(PageBreak())

    # === 2. DESCRIÇÃO DO MÉTODO ===
    story.append(Paragraph("2. Descrição do Método Utilizado", heading1_style))
    metodo_text = """
    A metodologia seguiu um fluxo iterativo de cinco etapas principais, garantindo que o
    pipeline funcione de maneira eficiente e seja avaliado continuamente:
    """
    story.append(Paragraph(metodo_text, body_style))

    # Sub-seções do método
    story.append(Paragraph("Coleta e Integração", heading2_style))
    story.append(Paragraph(
        "Nesta etapa, os arquivos CSV provenientes de diferentes injetoras são reunidos. "
        "Cada arquivo traz dados como quantidade produzida, quantidade refugada, datas de "
        "produção, códigos de produto e informações sobre o consumo de massa. O objetivo é "
        "unificar esses dados em um DataFrame único para viabilizar análises consolidadas.",
        body_style
    ))

    story.append(Paragraph("Pré-processamento e Limpeza", heading2_style))
    story.append(Paragraph(
        "Uma vez que os dados estejam integrados, procede-se à limpeza. Isso envolve a "
        "remoção de possíveis duplicatas, conversão de campos de data para o formato "
        "apropriado (datetime), tratamento de valores ausentes, padronização de colunas e "
        "exclusão de registros inconsistentes. Além disso, nesta etapa ocorrem transformações "
        "como a codificação One-Hot para variáveis categóricas.",
        body_style
    ))

    story.append(Paragraph("Análise Exploratória", heading2_style))
    story.append(Paragraph(
        "Com os dados preparados, explora-se o conjunto para identificar padrões, outliers e "
        "tendências. São calculadas estatísticas descritivas (média, mediana, desvio padrão) "
        "e produzidos gráficos como histogramas, boxplots e gráficos de dispersão.",
        body_style
    ))

    story.append(Paragraph("Modelagem (Treinamento)", heading2_style))
    story.append(Paragraph(
        "Na fase de modelagem, definimos a variável-alvo (tempo até a próxima manutenção) "
        "e selecionamos algoritmos de aprendizado de máquina. Modelos como Regressão Linear, "
        "Decision Tree, Random Forest e XGBoost são treinados nos dados de treinamento "
        "(80% do total).",
        body_style
    ))

    story.append(Paragraph("Validação e Avaliação", heading2_style))
    story.append(Paragraph(
        "Após o treinamento, avalia-se o desempenho dos modelos em dados de teste (20%). "
        "Métricas como R², MSE e MAE são calculadas para verificar o quão próximo cada "
        "modelo chega do valor real.",
        body_style
    ))
    story.append(PageBreak())

    # === 3. FERRAMENTAS UTILIZADAS ===
    story.append(Paragraph("3. Ferramentas Utilizadas", heading1_style))
    ferramentas = [
        ("Python (3.x)", "para toda a etapa de análise e modelagem"),
        ("Pandas e NumPy", "para manipulação de dados e transformações"),
        ("Scikit-learn", "como principal biblioteca de Machine Learning"),
        ("XGBoost", "para algoritmos de gradient boosting"),
        ("Matplotlib/Seaborn", "para visualizações de gráficos"),
        ("Jupyter Notebooks", "para execução interativa de código"),
    ]
    for ferramenta, desc in ferramentas:
        story.append(Paragraph(f"<b>{ferramenta}</b> {desc}.", body_style))
    story.append(PageBreak())

    # === 4. MODELOS UTILIZADOS ===
    story.append(Paragraph("4. Modelos Estocásticos e Estatísticos Utilizados", heading1_style))

    story.append(Paragraph("4.1 Regressão Linear", heading2_style))
    story.append(Paragraph(
        "A Regressão Linear é uma técnica estatística clássica que busca entender a relação "
        "entre uma variável dependente (tempo até a manutenção) e variáveis independentes. "
        "O modelo assume a forma: y ≈ β0 + β1X1 + β2X2 + ... + βnXn.",
        body_style
    ))

    story.append(Paragraph("4.2 Árvores de Decisão (Decision Tree)", heading2_style))
    story.append(Paragraph(
        "As Árvores de Decisão organizam os dados em subdivisões baseadas em critérios "
        "estatísticos, como o erro quadrático médio. São úteis para dados com interações "
        "complexas ou não lineares.",
        body_style
    ))

    story.append(Paragraph("4.3 Random Forest", heading2_style))
    story.append(Paragraph(
        "O Random Forest combina diversas Árvores de Decisão para tornar as previsões mais "
        "precisas e estáveis. Geralmente apresenta menor variância que uma única árvore, "
        "sendo mais robusto a ruídos e outliers.",
        body_style
    ))

    story.append(Paragraph("4.4 XGBoost (Extreme Gradient Boosting)", heading2_style))
    story.append(Paragraph(
        "O XGBoost é um modelo avançado que constrói árvores de decisão sequencialmente, "
        "onde cada nova árvore aprende com os erros da anterior. Utiliza técnicas de "
        "regularização (L1 e L2) para evitar overfitting.",
        body_style
    ))
    story.append(PageBreak())

    # === 5. MÉTRICAS ===
    story.append(Paragraph("5. Métricas", heading1_style))

    story.append(Paragraph("5.1 Erro Quadrático Médio (EQM/MSE)", heading2_style))
    story.append(Paragraph(
        "Mede a diferença entre os valores previstos pelo modelo e os valores reais, "
        "elevando essas diferenças ao quadrado. Quanto menor o EQM, mais precisas são "
        "as previsões do modelo.",
        body_style
    ))

    story.append(Paragraph("5.2 R-quadrado (Coeficiente de Determinação)", heading2_style))
    story.append(Paragraph(
        "O R-quadrado indica a proporção da variabilidade da variável dependente que é "
        "explicada pelo modelo. Seu valor varia entre 0 e 1, onde valores próximos de 1 "
        "indicam alta qualidade preditiva.",
        body_style
    ))

    story.append(Paragraph("5.3 Erro Absoluto Médio (EAM/MAE)", heading2_style))
    story.append(Paragraph(
        "Mede a média das diferenças absolutas entre os valores previstos e os reais. "
        "Ao contrário do EQM, não é tão sensível a erros extremos.",
        body_style
    ))
    story.append(PageBreak())

    # === 6. VARIÁVEIS TESTADAS ===
    story.append(Paragraph("6. Variáveis Testadas e a Relevância de Cada Variável", heading1_style))

    features = results.get("data", {}).get("features", [])
    if features:
        story.append(Paragraph(f"Total de {len(features)} variáveis utilizadas no modelo:", body_style))

        # Listar principais variáveis
        variaveis_principais = [
            ("Quantidade Produzida", "Volume de itens fabricados, diretamente ligada ao desgaste da máquina."),
            ("Quantidade Refugada", "Taxas de refugo podem sinalizar problemas de processo."),
            ("Quantidade Retrabalhada", "Peças que precisaram de retrabalho antes de serem aprovadas."),
            ("Consumo de Massa", "Mudanças bruscas podem sinalizar problemas de ajuste ou desgaste interno."),
            ("Cód. Ordem", "Identificador único de cada lote, encapsula informações específicas do produto."),
        ]

        for var, desc in variaveis_principais:
            story.append(Paragraph(f"<b>{var}:</b> {desc}", body_style))
    story.append(PageBreak())

    # === 7. DADOS REMOVIDOS ===
    story.append(Paragraph("7. Dados Removidos da Análise", heading1_style))
    story.append(Paragraph(
        "Durante a fase de pré-processamento, foram eliminados ou desconsiderados alguns "
        "registros e variáveis por apresentarem inconsistências:",
        body_style
    ))
    dados_removidos = [
        "Registros Duplicados: excluídos para evitar distorções na contagem.",
        "Linhas com Dados Inconsistentes: valores impossíveis ou datas inválidas.",
        "Valores Nulos Irrecuperáveis: quando variáveis essenciais estavam ausentes.",
        "Variáveis Redundantes: que não acrescentavam novas informações.",
    ]
    for item in dados_removidos:
        story.append(Paragraph(f"• {item}", body_style))
    story.append(PageBreak())

    # === 8. DESEMPENHO DE MODELOS ===
    story.append(Paragraph("8. Desempenho de Modelos", heading1_style))

    story.append(Paragraph(
        f"O modelo {model_name.upper()} foi selecionado como o melhor modelo nos experimentos, "
        "destacando a capacidade de capturar padrões complexos de desgaste.",
        body_style
    ))

    # Tabela de resultados
    if metrics:
        story.append(Paragraph("Quadro 1: Desempenho do Modelo Selecionado:", body_style))

        table_data = [
            ["Modelo", "EQM (MSE)", "R-Quadrado", "EAM (MAE)"],
            [model_name.upper(), f"{mse:.2f}", f"{r2:.4f}", f"{mae:.2f}"]
        ]

        table = Table(table_data, colWidths=[4*cm, 3*cm, 3*cm, 3*cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F77F00')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F5F5F5')),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
    story.append(PageBreak())

    # === 8b. GRÁFICOS DE ANÁLISE ===
    story.append(Paragraph("Gráficos de Análise", heading1_style))
    story.append(Paragraph(
        "Os gráficos abaixo apresentam as análises exploratórias realizadas durante o "
        "desenvolvimento do modelo, incluindo matrizes de correlação, análises de distribuição "
        "e visualizações das principais features.",
        body_style
    ))

    # Diretórios onde procurar gráficos (em ordem de prioridade)
    # 1. Diretório local (onde o pipeline é executado)
    # 2. Diretório scripts/eda_plots
    # 3. Diretórios de análises exploratórias existentes (fallback)
    script_dir = Path(__file__).parent.parent  # sabo/sabo/scripts
    plots_directories = [
        Path("eda_plots"),  # Diretório relativo ao CWD (scripts/)
        script_dir / "eda_plots",  # Caminho absoluto para scripts/eda_plots
        Path(__file__).parent.parent / "eda_plots",  # Alternativo
        script_dir.parent / "exploratory_analise",  # Gráficos existentes (fallback)
        script_dir.parent / "exploratory_dado_proposto" / "classification_images",
    ]

    # Log dos diretórios sendo verificados
    print("  Procurando gráficos em:")
    for d in plots_directories:
        exists = "✓" if d.exists() else "✗"
        print(f"    {exists} {d}")

    # Mapeamento de gráficos prioritários com legendas
    priority_plots = [
        ("correlation_matrix.png", "Matriz de Correlação entre Variáveis"),
        ("correlation_matrix_full.png", "Matriz de Correlação Completa"),
        ("heatmap_correlacao.png", "Heatmap de Correlação"),
        ("correlation_matrix_heatmap.png", "Heatmap de Correlação"),
        ("consumo_massa_vs_qtd_produzida.png", "Consumo de Massa vs Quantidade Produzida"),
        ("consumo_vs_producao.png", "Análise de Consumo vs Produção"),
        ("boxplot_consumo_massa_total.png", "Boxplot do Consumo de Massa Total"),
        ("ano_construcao_vs_consumo_massa.png", "Ano de Construção vs Consumo de Massa"),
        ("analise_temporal.png", "Análise Temporal"),
        ("matriz_urgencia.png", "Distribuição de Urgência de Manutenção"),
        ("scatter_plots.png", "Gráficos de Dispersão"),
        ("scatter_plots_features.png", "Scatter Plots das Features"),
        ("dispersao_target.png", "Dispersão do Target"),
        ("histogramas.png", "Histogramas das Variáveis"),
        ("boxplots.png", "Boxplots das Variáveis"),
    ]

    plots_added = 0
    max_plots = 6  # Aumentado para incluir mais gráficos
    added_names = set()  # Evitar duplicatas

    for plot_name, caption in priority_plots:
        if plots_added >= max_plots:
            break

        # Verificar nome base para evitar duplicatas similares
        base_name = plot_name.replace("_full", "").replace("_heatmap", "")
        if base_name in added_names:
            continue

        # Procurar o gráfico em todos os diretórios
        for plots_dir in plots_directories:
            if not plots_dir.exists():
                continue

            plot_path = plots_dir / plot_name
            if plot_path.exists():
                try:
                    # Calcular dimensões proporcionais
                    img_width = 16*cm
                    img_height = 11*cm

                    # Adicionar gráfico ao relatório
                    img = Image(str(plot_path), width=img_width, height=img_height)
                    story.append(img)
                    story.append(Spacer(1, 0.3*cm))

                    # Legenda do gráfico
                    story.append(Paragraph(
                        f"<i>Figura {plots_added + 1}: {caption}</i>",
                        ParagraphStyle('Caption', parent=styles['Normal'], fontSize=10, alignment=TA_CENTER)
                    ))
                    story.append(Spacer(1, 0.8*cm))

                    plots_added += 1
                    added_names.add(base_name)
                    print(f"  ✓ Gráfico adicionado: {plot_name}")
                    break  # Encontrou o gráfico, não procurar mais
                except Exception as e:
                    print(f"  ⚠ Erro ao adicionar gráfico {plot_name}: {e}")

    if plots_added == 0:
        story.append(Paragraph(
            "<i>Gráficos não disponíveis. Execute a etapa 3 (EDA) ou 3b (Análises Avançadas) para gerá-los.</i>",
            styles['Normal']
        ))
    else:
        print(f"  Total de gráficos adicionados ao relatório: {plots_added}")

    story.append(PageBreak())

    # === 9. CONSIDERAÇÕES ===
    story.append(Paragraph("9. Considerações", heading1_style))
    story.append(Paragraph(
        "O desenvolvimento de um projeto de manutenção prescritiva para injetoras de borracha "
        "demonstrou o potencial de algoritmos de Machine Learning não apenas para prever o "
        "momento ideal de manutenção, mas para prescrever intervenções específicas baseadas "
        "nos padrões identificados.",
        body_style
    ))
    story.append(Paragraph(
        "O ensaio de diferentes algoritmos (Regressão Linear, Decision Tree, Random Forest e "
        "XGBoost) evidenciou que modelos de ensemble tendem a produzir resultados superiores "
        "quando há volume de dados adequado.",
        body_style
    ))
    story.append(PageBreak())

    # === 10. PRÓXIMOS PASSOS ===
    story.append(Paragraph("10. Próximos Passos", heading1_style))
    proximos_passos = [
        ("Shadow Teste", "Executar o modelo em operação paralela à linha produtiva, sem intervenções reais."),
        ("Sistema de Recomendações", "Criar regras que traduzam resultados em recomendações de intervenção."),
        ("Implantação em Produção", "Adoção gradativa começando por linha-piloto."),
        ("Validação Contínua", "Incorporar dados recentes de forma recorrente."),
        ("Incorporar Variáveis de Campo", "Incluir dados de temperatura, vibração e pressão."),
    ]
    for titulo, desc in proximos_passos:
        story.append(Paragraph(f"<b>{titulo}:</b> {desc}", body_style))
    story.append(PageBreak())

    # === 11. RECOMENDAÇÕES ===
    story.append(Paragraph("11. Recomendações", heading1_style))
    recomendacoes = [
        "Desenvolvimento de Biblioteca de Recomendações Prescritivas",
        "Plano Estruturado de Coleta e Qualidade de Dados",
        "Cultura de manutenção prescritiva",
        "Integração com Sistemas de Produção",
        "Pipelines de Teste e Retreinamento",
        "Explorar Novos Modelos e Técnicas de Interpretação",
        "Foco nos Benefícios Financeiros e Estratégicos",
    ]
    for rec in recomendacoes:
        story.append(Paragraph(f"• {rec}", body_style))
    story.append(PageBreak())

    # === 12. CONSIDERAÇÕES FINAIS ===
    story.append(Paragraph("12. Considerações Finais", heading1_style))
    story.append(Paragraph(
        f"Os resultados preliminares revelaram boas perspectivas para métodos como "
        f"{model_name.upper()}. A incorporação de novas amostras tende a aumentar a precisão "
        "das previsões, viabilizando uma manutenção realmente prescritiva, que não apenas "
        "prevê quando intervir, mas também determina as ações específicas necessárias.",
        body_style
    ))
    story.append(Paragraph(
        "Como parâmetro de amostragem mínima, recomenda-se reunir ao menos 30 observações "
        "para cada cenário relevante, sendo a faixa ideal entre 50 e 100 observações para "
        "garantir maior robustez estatística.",
        body_style
    ))
    story.append(PageBreak())

    # === REFERÊNCIAS ===
    story.append(Paragraph("Referência Bibliográfica", heading1_style))
    referencias = [
        "Breiman, L. (2001). Random Forests. Machine Learning, 45(1), 5–32.",
        "Breiman, L., Friedman, J. H., Olshen, R. A., & Stone, C. J. (1984). Classification and Regression Trees. Wadsworth.",
        "Chen, T., & Guestrin, C. (2016). XGBoost: A Scalable Tree Boosting System. ACM SIGKDD.",
        "Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow. O'Reilly.",
        "Hastie, T., Tibshirani, R., & Friedman, J. (2009). The Elements of Statistical Learning. Springer.",
    ]
    for ref in referencias:
        story.append(Paragraph(ref, styles['Normal']))
        story.append(Spacer(1, 0.3*cm))

    # Gerar PDF
    doc.build(story)

    return output_path


def generate_text_report(results: dict, output_path: str) -> str:
    """
    Gera relatório em formato texto (fallback quando ReportLab não está disponível).

    Args:
        results: Dados do pipeline
        output_path: Caminho do arquivo

    Returns:
        Caminho do arquivo gerado
    """
    model_name = results.get("model", {}).get("name", "Não identificado")
    metrics = results.get("model", {}).get("metrics", {})
    r2 = metrics.get("r2", 0)
    mse = metrics.get("mse", 0)
    mae = metrics.get("mae", 0)
    data_shape = results.get("data", {}).get("shape", (0, 0))

    report = f"""
{'=' * 70}
RELATÓRIO DE ENTREGA - SABO
Pipeline de Machine Learning para Manutenção Preditiva
Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}
{'=' * 70}

SUMÁRIO
-------
1. Resumo
2. Introdução
3. Descrição do Método Utilizado
4. Ferramentas Utilizadas
5. Modelos Utilizados
6. Métricas
7. Variáveis Testadas
8. Desempenho de Modelos
9. Considerações
10. Próximos Passos
11. Recomendações
12. Considerações Finais

{'=' * 70}
RESUMO
{'=' * 70}

Este relatório apresenta os resultados do projeto de manutenção preditiva
para extrusoras de borracha modelo Y125.

Dataset: {data_shape[0]} registros, {data_shape[1]} variáveis
Modelo Selecionado: {model_name.upper()}

MÉTRICAS DO MODELO:
  - R² (Coeficiente de Determinação): {r2:.4f}
  - MSE (Erro Quadrático Médio): {mse:.2f}
  - MAE (Erro Absoluto Médio): {mae:.2f}

{'=' * 70}
METODOLOGIA
{'=' * 70}

O pipeline seguiu 5 etapas:
1. Coleta e Integração de Dados
2. Pré-processamento e Limpeza
3. Análise Exploratória (EDA)
4. Modelagem e Treinamento
5. Validação e Avaliação

{'=' * 70}
MODELOS TESTADOS
{'=' * 70}

1. Regressão Linear
2. Decision Tree
3. Random Forest
4. XGBoost

{'=' * 70}
DESEMPENHO
{'=' * 70}

Modelo Selecionado: {model_name.upper()}
  R²:  {r2:.4f}
  MSE: {mse:.2f}
  MAE: {mae:.2f}

{'=' * 70}
CONSIDERAÇÕES FINAIS
{'=' * 70}

O projeto demonstrou o potencial de algoritmos de Machine Learning para
manutenção prescritiva. Recomenda-se:

- Executar Shadow Teste em ambiente de produção
- Incorporar mais variáveis de campo (temperatura, vibração)
- Ampliar a base de dados para maior robustez estatística
- Implementar retreinamento periódico do modelo

{'=' * 70}
"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)

    return output_path


def main() -> dict:
    """
    Função principal - Etapa 6: Geração de Relatório.

    Returns:
        Dicionário com resultados da execução
    """
    print("=" * 60)
    print("ETAPA 6: GERAÇÃO DE RELATÓRIO")
    print("=" * 60)

    # Verificar arquivos necessários
    required_files = ["best_model.joblib"]
    missing = [f for f in required_files if not Path(f).exists()]

    if missing:
        print(f"\n⚠ Arquivos não encontrados: {', '.join(missing)}")
        print("Execute as etapas anteriores do pipeline primeiro.")
        return {"status": "error", "message": "Missing required files"}

    # Carregar resultados
    print("\n[1/3] Carregando resultados do pipeline...")
    results = load_pipeline_results()

    # Determinar versão
    version = get_report_version()

    # Gerar relatório
    print(f"\n[2/3] Gerando relatório versão {version}...")

    if HAS_REPORTLAB:
        output_path = f"Relatorio_SABO_{version}.pdf"
        generated_path = generate_pdf_report(results, output_path)
        print(f"  ✓ Relatório PDF gerado: {generated_path}")
    else:
        output_path = f"Relatorio_SABO_{version}.txt"
        generated_path = generate_text_report(results, output_path)
        print(f"  ✓ Relatório TXT gerado: {generated_path}")
        print("  ⚠ Instale 'reportlab' para gerar PDF: pip install reportlab")

    # Resumo
    print("\n[3/3] Finalizando...")

    print("\n" + "=" * 60)
    print("ETAPA 6 CONCLUÍDA")
    print("=" * 60)
    print(f"\nRelatório gerado: {generated_path}")

    # Informações do modelo
    model_name = results.get("model", {}).get("name", "N/A")
    metrics = results.get("model", {}).get("metrics", {})

    print(f"\nModelo documentado: {model_name.upper()}")
    if metrics:
        print(f"  R²:  {metrics.get('r2', 0):.4f}")
        print(f"  MSE: {metrics.get('mse', 0):.2f}")
        print(f"  MAE: {metrics.get('mae', 0):.2f}")

    return {
        "status": "success",
        "output_file": generated_path,
        "version": version,
        "model": model_name,
        "metrics": metrics,
    }


if __name__ == "__main__":
    main()
