"""
SABO - Pipeline Principal de Machine Learning
==============================================
Pipeline estruturado conforme fluxos.drawio

FLUXO DO PIPELINE (6 ETAPAS):
┌─────────────────────────────────────────────────────────────────┐
│  ETAPA 1: COLETA E INTEGRAÇÃO                                   │
│  CSV Files (ij-044, ij-046, ij-117...) → DataFrame Único        │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 2: PRÉ-PROCESSAMENTO E LIMPEZA                           │
│  - Higienização: Remover duplicadas, Tratar nulos, Datas        │
│  - Engenharia: Variáveis acumulativas, One-Hot Encoding         │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 3: ANÁLISE EXPLORATÓRIA (EDA)                            │
│  - Estatísticas: Média, Desvio Padrão, Quartis                  │
│  - Gráficos: Histogramas, Boxplots                              │
│  - Correlação: Heatmaps, Dispersão                              │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 4: MODELAGEM E TREINAMENTO                               │
│  - Dividir: 80% Treino / 20% Teste                              │
│  - Algoritmos: Regressão Linear, Decision Tree,                 │
│                Random Forest, XGBoost                           │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 5: VALIDAÇÃO E AVALIAÇÃO                                 │
│  - Testar nos 20%                                               │
│  - Métricas: R², MSE, MAE                                       │
│  - Comparar e Selecionar Melhor Modelo                          │
├─────────────────────────────────────────────────────────────────┤
│  ETAPA 6: GERAÇÃO DE RELATÓRIO                                  │
│  - Relatório PDF no formato padrão SABO                         │
│  - Documentação completa do projeto                             │
└─────────────────────────────────────────────────────────────────┘

Uso:
    python run_pipeline.py                    # Executa pipeline completo
    python run_pipeline.py --step 1           # Executa apenas etapa 1
    python run_pipeline.py --step 2           # Executa apenas etapa 2
    python run_pipeline.py --list             # Lista todas as etapas
    python run_pipeline.py --history          # Mostra histórico
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path

# Diretório base (Fase02)
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
OUTPUTS_DIR = BASE_DIR / "outputs"

# Adicionar diretórios ao path
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(BASE_DIR / "config"))

from history_manager import HistoryManager, print_history_summary


class _TeeStream:
    """Duplica writes para dois streams (ex.: stdout do terminal + arquivo de log)."""

    def __init__(self, primary, mirror):
        self.primary = primary
        self.mirror = mirror

    def write(self, data):
        self.primary.write(data)
        self.mirror.write(data)

    def flush(self):
        self.primary.flush()
        self.mirror.flush()

    def isatty(self):
        return getattr(self.primary, "isatty", lambda: False)()

    def fileno(self):
        return self.primary.fileno()


def _setup_run_log(run_id: str) -> Path:
    """Redireciona stdout/stderr para history/logs/run_<id>.log sem perder o terminal."""
    log_dir = Path("history/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{run_id}.log"

    fh = open(log_file, "w", encoding="utf-8", buffering=1)
    fh.write(f"# Pipeline SABO — execução {run_id}\n")
    fh.write(f"# Início: {datetime.now().isoformat()}\n")
    fh.write(f"# Comando: {' '.join(sys.argv)}\n\n")

    sys.stdout = _TeeStream(sys.__stdout__, fh)
    sys.stderr = _TeeStream(sys.__stderr__, fh)
    return log_file


# Mapeamento de etapas conforme fluxos.drawio
PIPELINE_STEPS = {
    0: {
        "name": "s00_split_unified",
        "title": "Separação de Arquivo Único",
        "description": "Separa arquivo único por equipamento → data/raw/",
        "color": "cinza",
        "inputs": ["data/arquivo_unico/*.xlsx"],
        "outputs": ["data/raw/<equipamento>.csv", "data/raw/<equipamento>.xlsx"],
        "optional": True,
    },
    1: {
        "name": "s01_data_collection",
        "title": "Coleta e Integração",
        "description": "CSV Files → DataFrame Único",
        "color": "azul",
        "inputs": ["Arquivos CSV/XLSX de equipamentos"],
        "outputs": ["data_raw.csv"],
    },
    2: {
        "name": "s02_preprocessing",
        "title": "Pré-processamento e Limpeza",
        "description": "Higienização + Engenharia de Features",
        "color": "amarelo",
        "inputs": ["data_raw.csv"],
        "outputs": ["data_preprocessed.csv"],
    },
    3: {
        "name": "s03_eda",
        "title": "Análise Exploratória (EDA)",
        "description": "Estatísticas, Gráficos, Correlação",
        "color": "vermelho",
        "inputs": ["data_preprocessed.csv"],
        "outputs": ["data_eda.csv", "eda_report.txt", "eda_plots/"],
    },
    "3b": {
        "name": "s03b_advanced_eda",
        "title": "Análises Avançadas de EDA",
        "description": "Gráficos avançados: correlação, urgência, scatter",
        "color": "vermelho",
        "inputs": ["data_eda.csv"],
        "outputs": ["eda_plots/*.png (avançados)"],
        "optional": True,
    },
    4: {
        "name": "s04_modeling",
        "title": "Modelagem e Treinamento",
        "description": "4 Algoritmos: LR, DT, RF, XGBoost",
        "color": "roxo",
        "inputs": ["data_eda.csv"],
        "outputs": ["models/*.joblib", "train_test_split.npz"],
    },
    5: {
        "name": "s05_evaluation",
        "title": "Validação e Avaliação",
        "description": "Métricas R², MSE, MAE + Seleção",
        "color": "laranja",
        "inputs": ["models/*.joblib", "train_test_split.npz"],
        "outputs": ["best_model.joblib", "evaluation_report.txt"],
    },
    6: {
        "name": "s06_generate_report",
        "title": "Geração de Relatório",
        "description": "Relatório PDF com gráficos e métricas",
        "color": "verde",
        "inputs": ["best_model.joblib", "evaluation_report.txt", "eda_plots/"],
        "outputs": ["Relatorio_SABO_RX.pdf"],
    },
    7: {
        "name": "s07_hist_manutencao",
        "title": "Histórico de Manutenção + Cruzamento com Produção",
        "description": "Consolida Dados Manut*.xlsx (2025, 2026, …) e cruza com data/raw/",
        "color": "cinza",
        "inputs": ["data/manutencao/Dados Manut*.xlsx", "data/raw/IJ-*.csv"],
        "outputs": [
            "equipamentos_historico_completo.csv",
            "equipamentos_historico_recente.csv",
            "equipamentos_janelas_manutencao.csv",
            "equipamentos_ociosidade.csv",
        ],
        "optional": True,
    },
    8: {
        "name": "s08_prescricao_manutencao",
        "title": "Prescrição de Próxima Manutenção por Equipamento",
        "description": "Integra desgaste + consumo de massa + ociosidade",
        "color": "cinza",
        "inputs": [
            "equipamentos_historico_completo.csv",
            "equipamentos_historico_recente.csv",
            "equipamentos_janelas_manutencao.csv",
            "equipamentos_ociosidade.csv",
        ],
        "outputs": ["prescricao_manutencao.csv"],
        "optional": True,
    },
}


def print_pipeline_diagram():
    """Imprime diagrama visual do pipeline."""
    print("""
╔═══════════════════════════════════════════════════════════════════╗
║                    PIPELINE SABO - fluxos.drawio                  ║
╠═══════════════════════════════════════════════════════════════════╣
║                                                                   ║
║   ┌─────────────────┐                                             ║
║   │ 1. COLETA E     │  CSV Files (ij-044, ij-046, ij-117...)      ║
║   │    INTEGRAÇÃO   │  ───────────────────────────────────►       ║
║   │    (azul)       │                    DataFrame Único          ║
║   └────────┬────────┘                                             ║
║            │                                                      ║
║            ▼                                                      ║
║   ┌─────────────────┐  Remover duplicadas                         ║
║   │ 2. PRÉ-PROC.    │  Tratar nulos                               ║
║   │    E LIMPEZA    │  Converter datas                            ║
║   │    (amarelo)    │  Variáveis acumulativas                     ║
║   │                 │  One-Hot Encoding                           ║
║   └────────┬────────┘                                             ║
║            │                                                      ║
║            ▼                                                      ║
║   ┌─────────────────┐  Estatísticas: Média, Desvio, Quartis       ║
║   │ 3. ANÁLISE      │  Gráficos: Histogramas, Boxplots            ║
║   │    EXPLORATÓRIA │  Correlação: Heatmaps, Dispersão            ║
║   │    (vermelho)   │                                             ║
║   └────────┬────────┘                                             ║
║            │                                                      ║
║            ▼                                                      ║
║   ┌─────────────────┐  Divisão: 80% Treino / 20% Teste            ║
║   │ 4. MODELAGEM    │  ┌──────────────────────────────┐           ║
║   │    E TREINO     │  │ Regressão Linear │ Dec. Tree │           ║
║   │    (roxo)       │  │ Random Forest    │ XGBoost   │           ║
║   │                 │  └──────────────────────────────┘           ║
║   └────────┬────────┘                                             ║
║            │                                                      ║
║            ▼                                                      ║
║   ┌─────────────────┐  Testar nos 20%                             ║
║   │ 5. VALIDAÇÃO    │  Métricas: R², MSE, MAE                     ║
║   │    E AVALIAÇÃO  │  Comparar modelos                           ║
║   │    (laranja)    │  Selecionar melhor                          ║
║   └────────┬────────┘                                             ║
║            │                                                      ║
║            ▼                                                      ║
║   ┌─────────────────┐  Relatório PDF completo                     ║
║   │ 6. GERAÇÃO DE   │  Formato padrão SABO                        ║
║   │    RELATÓRIO    │  Documentação do projeto                    ║
║   │    (verde)      │  ───────► Relatorio_SABO_RX.pdf             ║
║   └─────────────────┘                                             ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
""")


def run_step(step, history: HistoryManager = None, pipeline_context: dict = None) -> dict:
    """
    Executa uma etapa específica do pipeline.

    Args:
        step: Número da etapa (1-6 ou "3b")
        history: Gerenciador de histórico (opcional)
        pipeline_context: Contexto do pipeline (inicio, fim, etc.)

    Returns:
        Resultados da etapa
    """
    if pipeline_context is None:
        pipeline_context = {}

    if step not in PIPELINE_STEPS:
        print(f"Erro: Etapa {step} não existe. Use --list para ver opções.")
        return {}

    step_info = PIPELINE_STEPS[step]
    script_name = step_info["name"]

    print("\n" + "=" * 70)
    print(f"ETAPA {step}: {step_info['title'].upper()}")
    print(f"Descrição: {step_info['description']}")
    print(f"Entradas:  {', '.join(step_info['inputs'])}")
    print(f"Saídas:    {', '.join(step_info['outputs'])}")
    print("=" * 70)

    try:
        # Importar e executar o script
        module = __import__(script_name)
        results = module.main(**pipeline_context)

        # Registrar no histórico
        if history and results:
            if isinstance(results, dict):
                to_log = results.get("models_metrics", results)
                history.log_step(script_name, to_log)
            elif isinstance(results, tuple) and len(results) >= 1:
                history.log_step(script_name, results[0] if isinstance(results[0], dict) else {"result": str(results)})

        return results

    except ImportError as e:
        print(f"Erro ao importar {script_name}: {e}")
        return {}
    except Exception as e:
        print(f"Erro ao executar {script_name}: {e}")
        import traceback
        traceback.print_exc()
        if history:
            history.run_data["errors"] = history.run_data.get("errors", [])
            history.run_data["errors"].append({"step": step, "error": str(e)})
        return {}


def list_steps():
    """Lista todas as etapas do pipeline."""
    print("\n" + "=" * 70)
    print("ETAPAS DO PIPELINE (conforme fluxos.drawio)")
    print("=" * 70)

    print(f"\n{'Etapa':<7} {'Nome':<30} {'Descrição'}")
    print("-" * 70)

    for step, info in PIPELINE_STEPS.items():
        print(f"{step:<7} {info['title']:<30} {info['description']}")

    print("\n" + "-" * 70)
    print("\nFluxo sequencial:")
    print("  1 → 2 → 3 → 3b(opcional) → 4 → 5 → 6")
    print("\nExemplos:")
    print("  python run_pipeline.py              # Executa todas as etapas")
    print("  python run_pipeline.py --step 1     # Executa apenas etapa 1")
    print("  python run_pipeline.py --step 3b    # Executa análises avançadas de EDA")
    print("\n" + "=" * 70)


def run_full_pipeline(save_history: bool = True, inicio: str = None, fim: str = None, suffix: str = "", version: str = None, run_id: str = None):
    """
    Executa o pipeline completo (7 etapas conforme fluxos.drawio).

    Args:
        save_history: Se True, salva histórico de execução
        inicio: Data de início do período (YYYY-MM-DD), opcional
        fim: Data de fim do período (YYYY-MM-DD), opcional
        suffix: Sufixo para o nome do relatório (ex: "_v1"), opcional
        version: Versão fixa do relatório (ex: "R12_v1"), opcional
    """
    print("=" * 70)
    print("SABO - PIPELINE DE MACHINE LEARNING")
    print("Manutenção Preditiva para Equipamentos Industriais")
    print("(Conforme fluxos.drawio)")
    print("=" * 70)

    if inicio or fim:
        print(f"\nFiltro de período: {inicio or 'início'} a {fim or 'fim'}")

    print_pipeline_diagram()

    # Inicializar gerenciador de histórico
    history = HistoryManager(run_id=run_id) if save_history else None

    if history:
        print(f"\nID desta execução: {history.run_id}")

    # Contexto do pipeline (propagado para cada etapa)
    pipeline_context = {}
    if inicio:
        pipeline_context["inicio"] = inicio
    if fim:
        pipeline_context["fim"] = fim
    if suffix:
        pipeline_context["suffix"] = suffix
    if version:
        pipeline_context["version"] = version

    results = {}

    try:
        # Executar as etapas em sequência (0, 1, 2, 3, 3b, 4, 5, 6, 7, 8)
        steps_order = [0, 1, 2, 3, "3b", 4, 5, 6, 7, 8]

        for step in steps_order:
            step_info = PIPELINE_STEPS.get(step, {})
            is_optional = step_info.get("optional", False)

            results[step] = run_step(step, history, pipeline_context)

            # Verificar se etapa falhou
            if not results[step] or results[step].get("status") == "error":
                if is_optional or step == 6:
                    print(f"\n⚠ Etapa {step} não foi concluída (opcional ou requer bibliotecas adicionais).")
                else:
                    print(f"\n✗ Etapa {step} falhou. Pipeline interrompido.")
                    break

        # Salvar histórico
        if history:
            history.save_run()

        # Resumo final
        print("\n" + "=" * 70)
        print("PIPELINE CONCLUÍDO!")
        print("=" * 70)

        successful_steps = sum(1 for r in results.values() if r and r.get("status") != "error")
        total_steps = len([s for s in PIPELINE_STEPS.values() if not s.get("optional", False)])
        print(f"\nEtapas executadas com sucesso: {successful_steps}/{total_steps + 1}")

        if successful_steps >= 5:
            print("\n✓ Pipeline executado com sucesso!")

            # Mostrar resultado final
            if 5 in results and results[5].get("best_model"):
                print(f"\nMelhor modelo: {results[5]['best_model'].upper()}")
                metrics = results[5].get("best_metrics") or results[5].get("metrics", {})
                if metrics:
                    print(f"  R²:  {metrics.get('r2', 0):.4f}")
                    print(f"  MSE: {metrics.get('mse', 0):.2f}")
                    print(f"  MAE: {metrics.get('mae', 0):.2f}")

            # Mostrar relatório gerado
            if 6 in results and results[6].get("output_file"):
                print(f"\nRelatório gerado: {results[6]['output_file']}")

        if history:
            print(f"\nArquivos de histórico:")
            print(f"  - history/runs/run_{history.run_id}.json")
            print(f"  - history/reports/report_{history.run_id}.txt")

    except Exception as e:
        print(f"\nErro durante execução: {e}")
        if history:
            history.run_data["error"] = str(e)
            history.save_run()
        raise

    return results


def main():
    parser = argparse.ArgumentParser(
        description="SABO - Pipeline de ML (conforme fluxos.drawio)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python run_pipeline.py                    # Executa pipeline completo (6 etapas)
  python run_pipeline.py --step 1           # Executa apenas Etapa 1 (Coleta)
  python run_pipeline.py --step 2           # Executa apenas Etapa 2 (Pré-proc.)
  python run_pipeline.py --step 3           # Executa apenas Etapa 3 (EDA)
  python run_pipeline.py --step 4           # Executa apenas Etapa 4 (Modelagem)
  python run_pipeline.py --step 5           # Executa apenas Etapa 5 (Avaliação)
  python run_pipeline.py --step 6           # Executa apenas Etapa 6 (Relatório)
  python run_pipeline.py --list             # Lista todas as etapas
  python run_pipeline.py --diagram          # Mostra diagrama do pipeline
  python run_pipeline.py --history          # Mostra histórico de execuções

Etapas do Pipeline (fluxos.drawio):
  1. Coleta e Integração      - CSV Files → DataFrame Único
  2. Pré-processamento        - Higienização + Engenharia de Features
  3. Análise Exploratória     - Estatísticas, Gráficos, Correlação
  4. Modelagem e Treinamento  - LR, Decision Tree, Random Forest, XGBoost
  5. Validação e Avaliação    - Métricas R², MSE, MAE + Seleção do Melhor
  6. Geração de Relatório     - Relatório PDF no formato padrão SABO
        """
    )
    parser.add_argument(
        "--step",
        type=str,
        choices=["0", "1", "2", "3", "3b", "4", "5", "6", "7", "8"],
        help="Executa apenas a etapa especificada (0-6 ou 3b)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Lista todas as etapas do pipeline"
    )
    parser.add_argument(
        "--diagram",
        action="store_true",
        help="Mostra diagrama visual do pipeline"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Visualiza histórico de execuções anteriores"
    )
    parser.add_argument(
        "--compare",
        type=int,
        default=0,
        help="Compara as últimas N execuções"
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Não salva histórico desta execução"
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Não grava o stdout/stderr em history/logs/run_<id>.log"
    )
    parser.add_argument(
        "--inicio",
        type=str,
        default=None,
        help="Data de início do período (formato YYYY-MM-DD)"
    )
    parser.add_argument(
        "--fim",
        type=str,
        default=None,
        help="Data de fim do período (formato YYYY-MM-DD)"
    )
    parser.add_argument(
        "--suffix",
        type=str,
        default="",
        help="Sufixo para o nome do relatório (ex: _v1)"
    )
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="Versão fixa do relatório (ex: R12_v1), sobrescreve auto-incremento"
    )

    args = parser.parse_args()

    # Mudar para o diretório outputs (onde os arquivos são gerados)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    os.chdir(str(OUTPUTS_DIR))

    # Listar etapas
    if args.list:
        list_steps()
        return

    # Mostrar diagrama
    if args.diagram:
        print_pipeline_diagram()
        return

    # Visualizar histórico
    if args.history:
        print_history_summary()
        return

    # Comparar execuções
    if args.compare > 0:
        hm = HistoryManager()
        df = hm.compare_runs(n_runs=args.compare)
        if not df.empty:
            print("\n" + "=" * 70)
            print(f"COMPARATIVO DAS ÚLTIMAS {args.compare} EXECUÇÕES")
            print("=" * 70)
            print(df.to_markdown(index=False))

            best = hm.get_best_run()
            if best:
                print(f"\nMelhor execução histórica: {best['run_id']}")
        return

    # Executar pipeline — gerar run_id único para alinhar histórico e arquivo de log
    save_history = not args.no_history
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not args.no_log:
        log_file = _setup_run_log(run_id)
        print(f"Log desta execução: {log_file}")

    history = HistoryManager(run_id=run_id) if save_history else None

    # Contexto do pipeline (inicio/fim/suffix)
    pipeline_context = {}
    if args.inicio:
        pipeline_context["inicio"] = args.inicio
    if args.fim:
        pipeline_context["fim"] = args.fim
    if args.suffix:
        pipeline_context["suffix"] = args.suffix
    if args.version:
        pipeline_context["version"] = args.version

    if args.step:
        # Executar etapa específica
        step = int(args.step) if args.step.isdigit() else args.step
        run_step(step, history, pipeline_context)
        if history:
            history.save_run()
    else:
        # Pipeline completo
        run_full_pipeline(save_history=save_history, inicio=args.inicio, fim=args.fim, suffix=args.suffix, version=args.version, run_id=run_id)


if __name__ == "__main__":
    main()
