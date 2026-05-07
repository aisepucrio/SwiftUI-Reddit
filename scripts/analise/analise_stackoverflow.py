import re
from html.parser import HTMLParser
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

matplotlib.use("Agg")


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_RAW_SO_DIR = BASE_DIR / "data" / "raw" / "stackoverflow"
DATA_PROCESSED_SO_DIR = BASE_DIR / "data" / "processed" / "stackoverflow"
OUTPUTS_SO_DIR = BASE_DIR / "outputs" / "stackoverflow"

ARQ_QUESTIONS = DATA_RAW_SO_DIR / "stackoverflow_swiftui_questions.csv"
ARQ_ANSWERS = DATA_RAW_SO_DIR / "stackoverflow_swiftui_answers.csv"

plt.rcParams["axes.unicode_minus"] = False

ARCHITECTURE_ALIASES = {
    "MVVM": [r"\bMVVM\b", r"\bmodel[\s-]?view[\s-]?viewmodel\b"],
    "MVVM-C": [r"\bMVVM[-\s]?C\b", r"\bMVVM Coordinator\b"],
    "MVC": [r"\bMVC\b", r"\bmodel[\s-]?view[\s-]?controller\b"],
    "MVP": [r"\bMVP\b", r"\bmodel[\s-]?view[\s-]?presenter\b"],
    "VIPER": [r"\bVIPER\b"],
    "TCA": [r"\bTCA\b", r"\bthe composable architecture\b", r"\bcomposable architecture\b"],
    "MVI": [r"\bMVI\b", r"\bmodel[\s-]?view[\s-]?intent\b"],
    "Redux": [r"\bredux\b", r"\bredux-like\b"],
    "RIBs": [r"\bRIBs\b", r"\brouter[\s-]?interactor[\s-]?builder\b"],
}

ARCHITECTURE_PATTERNS = {
    arch: [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    for arch, patterns in ARCHITECTURE_ALIASES.items()
}


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        """Inicializa o acumulador de trechos textuais extraídos do HTML."""
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        """Recebe texto puro encontrado pelo parser e armazena para recomposição."""
        self._parts.append(data)

    def get_text(self) -> str:
        """Retorna o texto extraído do HTML em uma única string."""
        return " ".join(self._parts)


def strip_html(html: str) -> str:
    """Remove tags HTML de um corpo do Stack Overflow preservando o texto."""
    if not html or not isinstance(html, str):
        return ""
    stripper = _HTMLStripper()
    stripper.feed(html)
    return stripper.get_text()


def carregar_dados() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carrega perguntas e respostas brutas e prepara campos temporais e textuais."""
    questions = pd.read_csv(ARQ_QUESTIONS)
    answers = pd.read_csv(ARQ_ANSWERS)

    questions["created_iso"] = pd.to_datetime(questions["created_iso"], format="ISO8601", utc=True)
    answers["created_iso"] = pd.to_datetime(answers["created_iso"], format="ISO8601", utc=True)

    questions["analysis_text"] = (
        questions["title"].fillna("") + "\n" + questions["body"].fillna("").apply(strip_html)
    )
    answers["analysis_text"] = answers["body"].fillna("").apply(strip_html)

    return questions, answers


def extract_architectures(text: str) -> list[str]:
    """Identifica arquiteturas mencionadas em um texto do Stack Overflow."""
    if not text or not isinstance(text, str):
        return []
    return [
        arch
        for arch, patterns in ARCHITECTURE_PATTERNS.items()
        if any(p.search(text) for p in patterns)
    ]


def preparar_arquiteturas(df: pd.DataFrame, text_col: str) -> pd.DataFrame:
    """Adiciona uma coluna com arquiteturas detectadas em uma coluna textual."""
    df = df.copy()
    df["detected_architectures"] = df[text_col].apply(extract_architectures)
    df["detected_architectures"] = df["detected_architectures"].apply(",".join)
    return df


def classify_sentiment(compound: float) -> str:
    """Classifica o score compound do VADER em positivo, negativo ou neutro."""
    if compound >= 0.05:
        return "positivo"
    if compound <= -0.05:
        return "negativo"
    return "neutro"


def preparar_sentimento(df: pd.DataFrame, text_col: str, sia: SentimentIntensityAnalyzer) -> pd.DataFrame:
    """Calcula scores de sentimento VADER e adiciona rótulos ao DataFrame."""
    df = df.copy()
    scores = df[text_col].fillna("").apply(sia.polarity_scores)
    df["sentiment_neg"] = scores.apply(lambda s: s["neg"])
    df["sentiment_neu"] = scores.apply(lambda s: s["neu"])
    df["sentiment_pos"] = scores.apply(lambda s: s["pos"])
    df["sentiment_compound"] = scores.apply(lambda s: s["compound"])
    df["sentiment_label"] = df["sentiment_compound"].apply(classify_sentiment)
    return df


def explodir_keywords(df: pd.DataFrame, col_keywords: str, nova_col: str) -> pd.DataFrame:
    """Expande uma coluna de arquiteturas separadas por vírgula em linhas."""
    df = df.copy()
    df[col_keywords] = df[col_keywords].fillna("")
    df[nova_col] = df[col_keywords].str.split(",")
    df = df.explode(nova_col)
    df[nova_col] = df[nova_col].str.strip()
    df = df[df[nova_col] != ""]
    return df


def salvar_arquiteturas_monitoradas() -> None:
    """Salva as arquiteturas e expressões usadas na classificação."""
    rows = [
        {"architecture": arch, "patterns": " | ".join(aliases)}
        for arch, aliases in ARCHITECTURE_ALIASES.items()
    ]
    out_csv = DATA_PROCESSED_SO_DIR / "arquiteturas_monitoradas.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")


def salvar_bases_reclassificadas(questions: pd.DataFrame, answers: pd.DataFrame) -> None:
    """Salva perguntas e respostas enriquecidas com arquitetura e sentimento."""
    questions_out = DATA_PROCESSED_SO_DIR / "stackoverflow_questions_reclassificadas.csv"
    answers_out = DATA_PROCESSED_SO_DIR / "stackoverflow_answers_reclassificadas.csv"
    questions.to_csv(questions_out, index=False)
    answers.to_csv(answers_out, index=False)
    print(f"Salvo: {questions_out}")
    print(f"Salvo: {answers_out}")


def frequencia_arquiteturas(questions: pd.DataFrame, answers: pd.DataFrame) -> None:
    """Calcula e plota frequência de arquiteturas em perguntas e respostas."""
    q_arch = explodir_keywords(questions, "detected_architectures", "arch")
    freq_q = (
        q_arch["arch"].value_counts().rename_axis("architecture").reset_index(name="count_questions")
    )

    a_arch = explodir_keywords(answers, "detected_architectures", "arch")
    freq_a = (
        a_arch["arch"].value_counts().rename_axis("architecture").reset_index(name="count_answers")
    )

    freq_total = pd.merge(freq_q, freq_a, on="architecture", how="outer").fillna(0)
    freq_total["count_questions"] = freq_total["count_questions"].astype(int)
    freq_total["count_answers"] = freq_total["count_answers"].astype(int)
    freq_total["count_total"] = freq_total["count_questions"] + freq_total["count_answers"]
    freq_total = freq_total.sort_values("count_total", ascending=False)

    print("\n=== Frequência de arquiteturas (questions + answers) ===")
    print(freq_total)

    out_csv = DATA_PROCESSED_SO_DIR / "freq_arquiteturas_total.csv"
    freq_total.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    plt.figure(figsize=(10, 5))
    plt.bar(freq_total["architecture"], freq_total["count_total"])
    plt.title("Menções a arquiteturas no Stack Overflow (tag SwiftUI)")
    plt.xlabel("Arquitetura")
    plt.ylabel("Número de menções")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_SO_DIR / "grafico_freq_arquiteturas_total.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def evolucao_temporal(questions: pd.DataFrame) -> None:
    """Calcula e plota a evolução mensal das arquiteturas nas perguntas."""
    q_arch = explodir_keywords(questions, "detected_architectures", "arch")
    q_arch["year_month"] = q_arch["created_iso"].dt.to_period("M").dt.to_timestamp()

    counts = (
        q_arch.groupby(["year_month", "arch"])
        .size()
        .reset_index(name="num_questions")
        .sort_values(["year_month", "num_questions"], ascending=[True, False])
    )

    print("\n=== Evolução temporal por arquitetura (questions por mês) ===")
    print(counts)

    out_csv = DATA_PROCESSED_SO_DIR / "evolucao_arquiteturas_mes.csv"
    counts.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    COLOR_MAP = {
        "MVVM": "#1f77b4",
        "MVVM-C": "#17becf",
        "MVC": "#2ca02c",
        "MVP": "#9467bd",
        "VIPER": "#8c564b",
        "TCA": "#e377c2",
        "MVI": "#bcbd22",
        "Redux": "#ff7f0e",
        "RIBs": "#7f7f7f",
    }

    plt.figure(figsize=(11, 6))
    for architecture, df_arch in counts.groupby("arch"):
        color = COLOR_MAP.get(architecture)
        plt.plot(df_arch["year_month"], df_arch["num_questions"], marker="o", label=architecture, color=color)

    plt.title("Evolução mensal das arquiteturas no Stack Overflow (tag SwiftUI)")
    plt.xlabel("Ano-Mês")
    plt.ylabel("Número de perguntas")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    out_png = OUTPUTS_SO_DIR / "grafico_evolucao_arquiteturas_mes.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def sentimento_por_arquitetura(questions: pd.DataFrame, answers: pd.DataFrame) -> None:
    """Agrega e plota a distribuição de sentimento por arquitetura."""
    q_arch = explodir_keywords(questions, "detected_architectures", "arch")
    a_arch = explodir_keywords(answers, "detected_architectures", "arch")

    q_arch["source"] = "question"
    a_arch["source"] = "answer"
    combined = pd.concat([q_arch, a_arch], ignore_index=True)

    sentiment_arch = (
        combined.groupby(["arch", "sentiment_label"])
        .size()
        .reset_index(name="count")
    )

    out_csv = DATA_PROCESSED_SO_DIR / "sentimento_por_arquitetura.csv"
    sentiment_arch.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    pivot = (
        sentiment_arch.pivot(index="arch", columns="sentiment_label", values="count")
        .fillna(0)
        .astype(int)
    )

    for column in ["positivo", "neutro", "negativo"]:
        if column not in pivot.columns:
            pivot[column] = 0

    pivot = pivot[["positivo", "neutro", "negativo"]]
    pivot["total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("total", ascending=False)

    out_pivot = DATA_PROCESSED_SO_DIR / "sentimento_por_arquitetura_resumo.csv"
    pivot.reset_index().to_csv(out_pivot, index=False)
    print(f"Salvo: {out_pivot}")

    ax = pivot[["positivo", "neutro", "negativo"]].plot(kind="bar", figsize=(11, 6))
    ax.set_title("Sentimento por arquitetura (Stack Overflow — questions + answers)")
    ax.set_xlabel("Arquitetura")
    ax.set_ylabel("Número de menções")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_SO_DIR / "grafico_sentimento_por_arquitetura.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def score_medio_por_arquitetura(questions: pd.DataFrame, answers: pd.DataFrame) -> None:
    """Calcula e plota score médio por arquitetura com base nos votos da comunidade."""
    q_arch = explodir_keywords(questions, "detected_architectures", "arch")
    a_arch = explodir_keywords(answers, "detected_architectures", "arch")

    score_q = q_arch.groupby("arch")["score"].mean().rename("score_medio_questions")
    score_a = a_arch.groupby("arch")["score"].mean().rename("score_medio_answers")

    score_df = pd.concat([score_q, score_a], axis=1).fillna(0).round(2)
    score_df["score_medio_total"] = (
        (score_df["score_medio_questions"] + score_df["score_medio_answers"]) / 2
    ).round(2)
    score_df = score_df.sort_values("score_medio_total", ascending=False)

    print("\n=== Score médio por arquitetura ===")
    print(score_df)

    out_csv = DATA_PROCESSED_SO_DIR / "score_medio_por_arquitetura.csv"
    score_df.reset_index().to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    plt.figure(figsize=(10, 5))
    plt.bar(score_df.index, score_df["score_medio_total"])
    plt.title("Score médio por arquitetura no Stack Overflow (votos da comunidade)")
    plt.xlabel("Arquitetura")
    plt.ylabel("Score médio")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_SO_DIR / "grafico_score_medio_por_arquitetura.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def main() -> None:
    """Executa o fluxo completo de análise dos dados do Stack Overflow."""
    DATA_PROCESSED_SO_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_SO_DIR.mkdir(parents=True, exist_ok=True)

    if not ARQ_QUESTIONS.exists() or not ARQ_ANSWERS.exists():
        print("ERRO: Certifique-se de que os arquivos CSV gerados pelo scraper existem:")
        print(f"- {ARQ_QUESTIONS}")
        print(f"- {ARQ_ANSWERS}")
        return

    questions, answers = carregar_dados()
    sia = SentimentIntensityAnalyzer()

    questions = preparar_arquiteturas(questions, "analysis_text")
    answers = preparar_arquiteturas(answers, "analysis_text")
    questions = preparar_sentimento(questions, "analysis_text", sia)
    answers = preparar_sentimento(answers, "analysis_text", sia)

    print(f"Perguntas carregadas: {len(questions)}")
    print(f"Respostas carregadas: {len(answers)}")
    print(f"Arquiteturas monitoradas: {', '.join(ARCHITECTURE_ALIASES.keys())}")

    salvar_arquiteturas_monitoradas()
    salvar_bases_reclassificadas(questions, answers)
    frequencia_arquiteturas(questions, answers)
    evolucao_temporal(questions)
    sentimento_por_arquitetura(questions, answers)
    score_medio_por_arquitetura(questions, answers)


if __name__ == "__main__":
    main()
