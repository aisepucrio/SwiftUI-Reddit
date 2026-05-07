import re
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

matplotlib.use("Agg")


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_RAW_REDDIT_DIR = BASE_DIR / "data" / "raw" / "reddit"
DATA_PROCESSED_REDDIT_DIR = BASE_DIR / "data" / "processed" / "reddit"
OUTPUTS_REDDIT_DIR = BASE_DIR / "outputs" / "reddit"

ARQ_POSTS = DATA_RAW_REDDIT_DIR / "reddit_swiftui_arch_posts_big.csv"
ARQ_COMMENTS = DATA_RAW_REDDIT_DIR / "reddit_swiftui_arch_comments_big.csv"

plt.rcParams["axes.unicode_minus"] = False

ARCHITECTURE_ALIASES = {
    "MVVM": [r"\bMVVM\b", r"\bmodel[\s-]?view[\s-]?viewmodel\b"],
    "MVVM-C": [r"\bMVVM[-\s]?C\b", r"\bMVVM Coordinator\b"],
    "MVC": [r"\bMVC\b", r"\bmodel[\s-]?view[\s-]?controller\b"],
    "MVP": [r"\bMVP\b", r"\bmodel[\s-]?view[\s-]?presenter\b"],
    "VIPER": [r"\bVIPER\b"],
    "TCA": [r"\bTCA\b", r"\bthe composable architecture\b", r"\bcomposable architecture\b"],
    "Clean Architecture": [r"\bclean architecture\b"],
    "Coordinator": [r"\bcoordinator pattern\b", r"\bcoordinator architecture\b"],
    "MVI": [r"\bMVI\b", r"\bmodel[\s-]?view[\s-]?intent\b"],
    "Redux": [r"\bredux\b", r"\bredux-like\b"],
    "RIBs": [r"\bRIBs\b", r"\brouter[\s-]?interactor[\s-]?builder\b"],
}

ARCHITECTURE_PATTERNS = {
    arch: [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    for arch, patterns in ARCHITECTURE_ALIASES.items()
}


def carregar_dados() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carrega posts e comentários brutos do Reddit e prepara texto de análise."""
    posts = pd.read_csv(ARQ_POSTS)
    comments = pd.read_csv(ARQ_COMMENTS)

    posts["created_iso"] = pd.to_datetime(posts["created_iso"])
    comments["created_iso"] = pd.to_datetime(comments["created_iso"])

    posts["analysis_text"] = (
        posts["title"].fillna("") + "\n" + posts["selftext"].fillna("")
    )
    comments["analysis_text"] = comments["body"].fillna("")

    return posts, comments


def extract_architectures(text: str) -> list[str]:
    """Identifica arquiteturas mencionadas em um texto usando padrões monitorados."""
    if not text or not isinstance(text, str):
        return []

    found = []
    for architecture, patterns in ARCHITECTURE_PATTERNS.items():
        if any(pattern.search(text) for pattern in patterns):
            found.append(architecture)
    return found


def preparar_arquiteturas(df: pd.DataFrame, text_col: str) -> pd.DataFrame:
    """Adiciona a coluna de arquiteturas detectadas a partir de uma coluna textual."""
    df = df.copy()
    df["detected_architectures"] = df[text_col].apply(extract_architectures)
    df["detected_architectures"] = df["detected_architectures"].apply(
        lambda values: ",".join(values)
    )
    return df


def classify_sentiment(compound: float) -> str:
    """Classifica o score compound do VADER em positivo, negativo ou neutro."""
    if compound >= 0.05:
        return "positivo"
    if compound <= -0.05:
        return "negativo"
    return "neutro"


def preparar_sentimento(df: pd.DataFrame, text_col: str, sia: SentimentIntensityAnalyzer) -> pd.DataFrame:
    """Calcula scores de sentimento VADER e adiciona rótulo ao DataFrame."""
    df = df.copy()
    sentiment_scores = df[text_col].fillna("").apply(sia.polarity_scores)
    df["sentiment_neg"] = sentiment_scores.apply(lambda score: score["neg"])
    df["sentiment_neu"] = sentiment_scores.apply(lambda score: score["neu"])
    df["sentiment_pos"] = sentiment_scores.apply(lambda score: score["pos"])
    df["sentiment_compound"] = sentiment_scores.apply(lambda score: score["compound"])
    df["sentiment_label"] = df["sentiment_compound"].apply(classify_sentiment)
    return df


def explodir_keywords(df: pd.DataFrame, col_keywords: str, nova_col: str) -> pd.DataFrame:
    """Transforma uma coluna CSV de palavras-chave em múltiplas linhas normalizadas."""
    df = df.copy()
    df[col_keywords] = df[col_keywords].fillna("")
    df[nova_col] = df[col_keywords].str.split(",")
    df = df.explode(nova_col)
    df[nova_col] = df[nova_col].str.strip()
    df = df[df[nova_col] != ""]
    return df


def salvar_arquiteturas_monitoradas() -> None:
    """Salva a lista de arquiteturas e expressões regulares monitoradas."""
    rows = []
    for architecture, aliases in ARCHITECTURE_ALIASES.items():
        rows.append(
            {
                "architecture": architecture,
                "patterns": " | ".join(aliases),
            }
        )

    out_csv = DATA_PROCESSED_REDDIT_DIR / "arquiteturas_monitoradas.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")


def salvar_bases_reclassificadas(posts: pd.DataFrame, comments: pd.DataFrame) -> None:
    """Salva posts e comentários enriquecidos com arquitetura e sentimento."""
    posts_out = DATA_PROCESSED_REDDIT_DIR / "reddit_posts_reclassificados.csv"
    comments_out = DATA_PROCESSED_REDDIT_DIR / "reddit_comments_reclassificados.csv"

    posts.to_csv(posts_out, index=False)
    comments.to_csv(comments_out, index=False)

    print(f"Salvo: {posts_out}")
    print(f"Salvo: {comments_out}")


def sentimento_por_arquitetura(posts: pd.DataFrame, comments: pd.DataFrame) -> None:
    """Agrega e plota a distribuição de sentimento por arquitetura no Reddit."""
    posts_arch = explodir_keywords(posts, "detected_architectures", "arch")
    comments_arch = explodir_keywords(comments, "detected_architectures", "arch")

    posts_arch["source"] = "post"
    comments_arch["source"] = "comment"
    combined = pd.concat([posts_arch, comments_arch], ignore_index=True)

    sentiment_arch = (
        combined.groupby(["arch", "sentiment_label"])
        .size()
        .reset_index(name="count")
    )

    out_csv = DATA_PROCESSED_REDDIT_DIR / "sentimento_por_arquitetura.csv"
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

    out_pivot = DATA_PROCESSED_REDDIT_DIR / "sentimento_por_arquitetura_resumo.csv"
    pivot.reset_index().to_csv(out_pivot, index=False)
    print(f"Salvo: {out_pivot}")

    ax = pivot[["positivo", "neutro", "negativo"]].plot(
        kind="bar",
        figsize=(11, 6),
    )
    ax.set_title("Sentimento por arquitetura (posts + comentários)")
    ax.set_xlabel("Arquitetura")
    ax.set_ylabel("Número de menções")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_REDDIT_DIR / "grafico_sentimento_por_arquitetura.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def frequencia_arquiteturas(posts: pd.DataFrame, comments: pd.DataFrame) -> None:
    """Calcula e plota frequência total de arquiteturas em posts e comentários."""
    posts_arch = explodir_keywords(posts, "detected_architectures", "arch")
    freq_posts = (
        posts_arch["arch"]
        .value_counts()
        .rename_axis("architecture")
        .reset_index(name="count_posts")
    )

    comments_arch = explodir_keywords(comments, "detected_architectures", "arch")
    freq_comments = (
        comments_arch["arch"]
        .value_counts()
        .rename_axis("architecture")
        .reset_index(name="count_comments")
    )

    freq_total = pd.merge(freq_posts, freq_comments, on="architecture", how="outer").fillna(0)
    freq_total["count_posts"] = freq_total["count_posts"].astype(int)
    freq_total["count_comments"] = freq_total["count_comments"].astype(int)
    freq_total["count_total"] = freq_total["count_posts"] + freq_total["count_comments"]
    freq_total = freq_total.sort_values("count_total", ascending=False)

    print("\n=== Frequência de arquiteturas (posts + comentários) ===")
    print(freq_total)

    out_csv = DATA_PROCESSED_REDDIT_DIR / "freq_arquiteturas_total.csv"
    freq_total.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    plt.figure(figsize=(10, 5))
    plt.bar(freq_total["architecture"], freq_total["count_total"])
    plt.title("Menções a arquiteturas em discussões SwiftUI (posts + comentários)")
    plt.xlabel("Arquitetura")
    plt.ylabel("Número de menções")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_REDDIT_DIR / "grafico_freq_arquiteturas_total.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def distribuicao_por_subreddit(posts: pd.DataFrame, comments: pd.DataFrame) -> None:
    """Calcula e plota frequência de arquiteturas por subreddit."""
    posts_arch = explodir_keywords(posts, "detected_architectures", "arch")
    comments_arch = explodir_keywords(comments, "detected_architectures", "arch")

    grp_posts = posts_arch.groupby(["subreddit", "arch"]).size().reset_index(name="count_posts")
    grp_comments = comments_arch.groupby(["subreddit", "arch"]).size().reset_index(name="count_comments")

    freq_sub = pd.merge(
        grp_posts,
        grp_comments,
        on=["subreddit", "arch"],
        how="outer",
    ).fillna(0)

    freq_sub["count_posts"] = freq_sub["count_posts"].astype(int)
    freq_sub["count_comments"] = freq_sub["count_comments"].astype(int)
    freq_sub["count_total"] = freq_sub["count_posts"] + freq_sub["count_comments"]
    freq_sub = freq_sub.sort_values(["subreddit", "count_total"], ascending=[True, False])

    print("\n=== Frequência por subreddit e arquitetura ===")
    print(freq_sub)

    out_csv = DATA_PROCESSED_REDDIT_DIR / "freq_arquiteturas_por_subreddit.csv"
    freq_sub.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    for subreddit in freq_sub["subreddit"].unique():
        df_sub = freq_sub[freq_sub["subreddit"] == subreddit].sort_values(
            "count_total", ascending=False
        )

        plt.figure(figsize=(10, 5))
        plt.bar(df_sub["arch"], df_sub["count_total"])
        plt.title(f"Menções a arquiteturas em {subreddit}")
        plt.xlabel("Arquitetura")
        plt.ylabel("Número de menções")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        out_png = OUTPUTS_REDDIT_DIR / f"grafico_freq_arquiteturas_{subreddit}.png"
        plt.savefig(out_png, dpi=300)
        plt.close()
        print(f"Gráfico salvo: {out_png}")


def evolucao_temporal(posts: pd.DataFrame) -> None:
    """Calcula e plota a evolução mensal das arquiteturas nos posts."""
    posts_arch = explodir_keywords(posts, "detected_architectures", "arch")
    posts_arch["year_month"] = posts_arch["created_iso"].dt.to_period("M").dt.to_timestamp()

    counts = (
        posts_arch.groupby(["year_month", "arch"])
        .size()
        .reset_index(name="num_posts")
        .sort_values(["year_month", "num_posts"], ascending=[True, False])
    )

    print("\n=== Evolução temporal por arquitetura (posts por mês) ===")
    print(counts)

    out_csv = DATA_PROCESSED_REDDIT_DIR / "evolucao_arquiteturas_mes.csv"
    counts.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    plt.figure(figsize=(11, 6))
    for architecture, df_arch in counts.groupby("arch"):
        plt.plot(
            df_arch["year_month"],
            df_arch["num_posts"],
            marker="o",
            label=architecture,
        )

    plt.title("Evolução mensal das arquiteturas em posts sobre SwiftUI")
    plt.xlabel("Ano-Mês")
    plt.ylabel("Número de posts")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    out_png = OUTPUTS_REDDIT_DIR / "grafico_evolucao_arquiteturas_mes.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def main() -> None:
    """Executa o fluxo completo de análise dos dados coletados do Reddit."""
    DATA_PROCESSED_REDDIT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REDDIT_DIR.mkdir(parents=True, exist_ok=True)

    if not ARQ_POSTS.exists() or not ARQ_COMMENTS.exists():
        print("ERRO: Certifique-se de que os arquivos CSV gerados pelo scraper existem:")
        print(f"- {ARQ_POSTS}")
        print(f"- {ARQ_COMMENTS}")
        return

    posts, comments = carregar_dados()
    sia = SentimentIntensityAnalyzer()

    posts = preparar_arquiteturas(posts, "analysis_text")
    comments = preparar_arquiteturas(comments, "analysis_text")
    posts = preparar_sentimento(posts, "analysis_text", sia)
    comments = preparar_sentimento(comments, "analysis_text", sia)

    print(f"Posts carregados: {len(posts)}")
    print(f"Comentários carregados: {len(comments)}")
    print(f"Arquiteturas monitoradas: {', '.join(ARCHITECTURE_ALIASES.keys())}")

    salvar_arquiteturas_monitoradas()
    salvar_bases_reclassificadas(posts, comments)
    frequencia_arquiteturas(posts, comments)
    distribuicao_por_subreddit(posts, comments)
    evolucao_temporal(posts)
    sentimento_por_arquitetura(posts, comments)


if __name__ == "__main__":
    main()
