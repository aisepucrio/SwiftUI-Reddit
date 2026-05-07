from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUTS_DIR = BASE_DIR / "outputs"

SOURCES = {
    "reddit": {
        "input": DATA_PROCESSED_DIR / "reddit" / "freq_arquiteturas_total.csv",
        "word_col": "architecture",
        "count_col": "count_total",
        "output": OUTPUTS_DIR / "reddit" / "nuvem_arquiteturas_reddit.png",
        "title": "Nuvem de palavras das arquiteturas no Reddit",
    },
    "stackoverflow": {
        "input": DATA_PROCESSED_DIR / "stackoverflow" / "freq_arquiteturas_total.csv",
        "word_col": "architecture",
        "count_col": "count_total",
        "output": OUTPUTS_DIR / "stackoverflow" / "nuvem_arquiteturas_stackoverflow.png",
        "title": "Nuvem de palavras das arquiteturas no Stack Overflow",
    },
    "github": {
        "input": DATA_PROCESSED_DIR / "github" / "freq_arquiteturas_total.csv",
        "word_col": "architecture",
        "count_col": "count_repos",
        "output": OUTPUTS_DIR / "github" / "nuvem_arquiteturas_github.png",
        "title": "Nuvem de palavras das arquiteturas no GitHub",
    },
    "forms": {
        "input": DATA_PROCESSED_DIR / "forms" / "arquiteturas_utilizadas_forms.csv",
        "word_col": "architecture",
        "count_col": "quantidade",
        "output": OUTPUTS_DIR / "forms" / "nuvem_arquiteturas_forms.png",
        "title": "Nuvem de palavras das arquiteturas no Forms",
    },
}

POSITIONS = [
    (0.50, 0.53),
    (0.30, 0.67),
    (0.70, 0.67),
    (0.29, 0.39),
    (0.71, 0.39),
    (0.50, 0.80),
    (0.50, 0.25),
    (0.15, 0.53),
    (0.85, 0.53),
    (0.36, 0.16),
    (0.64, 0.16),
    (0.36, 0.88),
    (0.64, 0.88),
]
COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#9467bd",
    "#8c564b",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]


def normalize_freq(df: pd.DataFrame, word_col: str, count_col: str) -> pd.DataFrame:
    """Normaliza uma tabela de frequência para colunas word e count agregadas."""
    freq = df[[word_col, count_col]].copy()
    freq = freq.rename(columns={word_col: "word", count_col: "count"})
    freq["word"] = freq["word"].fillna("").astype(str).str.strip()
    freq = freq[freq["word"] != ""]
    freq["count"] = pd.to_numeric(freq["count"], errors="coerce").fillna(0)
    freq = (
        freq.groupby("word", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
    )
    return freq


def calc_font_size(count: float, min_count: float, max_count: float) -> float:
    """Calcula o tamanho da fonte de uma palavra proporcionalmente à frequência."""
    if max_count == min_count:
        return 52
    ratio = (count - min_count) / (max_count - min_count)
    return 24 + (ratio**1.35) * 76


def plot_cloud(freq: pd.DataFrame, title: str, output_path: Path) -> None:
    """Desenha uma nuvem de palavras determinística e salva como imagem."""
    if freq.empty:
        print(f"Sem dados para: {output_path}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    top = freq.head(len(POSITIONS)).reset_index(drop=True)
    min_count = top["count"].min()
    max_count = top["count"].max()

    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    for idx, row in top.iterrows():
        x, y = POSITIONS[idx]
        ax.text(
            x,
            y,
            row["word"],
            fontsize=calc_font_size(row["count"], min_count, max_count),
            color=COLORS[idx % len(COLORS)],
            ha="center",
            va="center",
            rotation=0 if idx % 4 else 8,
            fontweight="bold" if idx < 3 else "normal",
            transform=ax.transAxes,
        )

    ax.set_title(title, fontsize=18, pad=16)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Gráfico salvo: {output_path}")


def main() -> None:
    """Gera nuvens de palavras por fonte e uma nuvem consolidada."""
    combined_rows = []

    for source, config in SOURCES.items():
        input_path = config["input"]
        if not input_path.exists():
            print(f"Arquivo não encontrado, pulando {source}: {input_path}")
            continue

        df = pd.read_csv(input_path)
        freq = normalize_freq(df, config["word_col"], config["count_col"])
        plot_cloud(freq, config["title"], config["output"])

        source_out = DATA_PROCESSED_DIR / source / f"nuvem_arquiteturas_{source}_frequencias.csv"
        freq.to_csv(source_out, index=False)
        print(f"Salvo: {source_out}")

        source_freq = freq.copy()
        source_freq["source"] = source
        combined_rows.append(source_freq)

    if not combined_rows:
        print("Nenhuma nuvem gerada.")
        return

    combined = pd.concat(combined_rows, ignore_index=True)
    combined_total = (
        combined.groupby("word", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
    )

    combined_csv = DATA_PROCESSED_DIR / "comparacao" / "freq_arquiteturas_todas_fontes.csv"
    combined_csv.parent.mkdir(parents=True, exist_ok=True)
    combined_total.to_csv(combined_csv, index=False)
    print(f"Salvo: {combined_csv}")

    combined_png = OUTPUTS_DIR / "comparacao" / "nuvem_arquiteturas_todas_fontes.png"
    plot_cloud(
        combined_total,
        "Nuvem de palavras das arquiteturas em todas as fontes",
        combined_png,
    )


if __name__ == "__main__":
    main()
