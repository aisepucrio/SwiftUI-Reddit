import re
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from cycler import cycler

matplotlib.use("Agg")

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_RAW_GITHUB_DIR = BASE_DIR / "data" / "raw" / "github"
DATA_PROCESSED_GITHUB_DIR = BASE_DIR / "data" / "processed" / "github"
OUTPUTS_GITHUB_DIR = BASE_DIR / "outputs" / "github"

ARQ_REPOS = DATA_RAW_GITHUB_DIR / "github_swiftui_repos.csv"

plt.rcParams["axes.unicode_minus"] = False

DEFAULT_BLUE = "#1f77b4"
DEFAULT_ORANGE = "#ff7f0e"
LINE_COLORS_NO_RED = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#9467bd",
    "#8c564b",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]

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


def carregar_dados() -> pd.DataFrame:
    """Carrega repositórios brutos do GitHub e prepara campos temporais e textuais."""
    repos = pd.read_csv(ARQ_REPOS)
    repos["created_iso"] = pd.to_datetime(repos["created_iso"], format="ISO8601", utc=True)
    repos["updated_iso"] = pd.to_datetime(repos["updated_iso"], format="ISO8601", utc=True)

    # Texto para detecção: descrição + tópicos + nome do repo
    repos["analysis_text"] = (
        repos["description"].fillna("") + " "
        + repos["topics"].fillna("").str.replace(",", " ")
        + " "
        + repos["name"].fillna("")
    )
    return repos


def extract_architectures(text: str) -> list[str]:
    """Detecta arquiteturas mencionadas em nome, descrição ou tópicos do repositório."""
    if not text or not isinstance(text, str):
        return []
    return [
        arch
        for arch, patterns in ARCHITECTURE_PATTERNS.items()
        if any(p.search(text) for p in patterns)
    ]


def preparar_arquiteturas(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona arquiteturas detectadas, usando a query original como fallback."""
    df = df.copy()
    detected = df["analysis_text"].apply(extract_architectures)
    # Fallback: usa arquitetura da query de busca se nada for detectado no texto
    df["detected_architectures"] = [
        ",".join(arches) if arches else query_arch
        for arches, query_arch in zip(detected, df["architecture"])
    ]
    return df


def explodir_keywords(df: pd.DataFrame, col_keywords: str, nova_col: str) -> pd.DataFrame:
    """Expande uma coluna de arquiteturas separadas por vírgula em linhas individuais."""
    df = df.copy()
    df[col_keywords] = df[col_keywords].fillna("")
    df[nova_col] = df[col_keywords].str.split(",")
    df = df.explode(nova_col)
    df[nova_col] = df[nova_col].str.strip()
    df = df[df[nova_col] != ""]
    return df


def salvar_arquiteturas_monitoradas() -> None:
    """Salva as arquiteturas e padrões usados na classificação dos repositórios."""
    rows = [
        {"architecture": arch, "patterns": " | ".join(aliases)}
        for arch, aliases in ARCHITECTURE_ALIASES.items()
    ]
    out_csv = DATA_PROCESSED_GITHUB_DIR / "arquiteturas_monitoradas.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")


def salvar_base_reclassificada(repos: pd.DataFrame) -> None:
    """Salva a base do GitHub enriquecida com arquiteturas detectadas."""
    out_csv = DATA_PROCESSED_GITHUB_DIR / "github_repos_reclassificados.csv"
    repos.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")


def frequencia_arquiteturas(repos: pd.DataFrame) -> None:
    """Calcula e plota a frequência de arquiteturas nos repositórios GitHub."""
    repos_arch = explodir_keywords(repos, "detected_architectures", "arch")
    freq = (
        repos_arch["arch"]
        .value_counts()
        .rename_axis("architecture")
        .reset_index(name="count_repos")
        .sort_values("count_repos", ascending=False)
    )

    print("\n=== Frequência de arquiteturas (repositórios) ===")
    print(freq)

    out_csv = DATA_PROCESSED_GITHUB_DIR / "freq_arquiteturas_total.csv"
    freq.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    plt.figure(figsize=(10, 5))
    plt.bar(freq["architecture"], freq["count_repos"], color=DEFAULT_BLUE)
    plt.title("Número de repositórios por arquitetura no GitHub (SwiftUI)")
    plt.xlabel("Arquitetura")
    plt.ylabel("Número de repositórios")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_GITHUB_DIR / "grafico_freq_arquiteturas_total.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def distribuicao_por_source(repos: pd.DataFrame) -> None:
    """Calcula e plota a distribuição de arquiteturas por origem da busca no GitHub."""
    repos_arch = explodir_keywords(repos, "detected_architectures", "arch")

    dist = (
        repos_arch.groupby(["arch", "source"])
        .size()
        .reset_index(name="count")
    )

    out_csv = DATA_PROCESSED_GITHUB_DIR / "freq_arquiteturas_por_source.csv"
    dist.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    pivot = (
        dist.pivot(index="arch", columns="source", values="count")
        .fillna(0)
        .astype(int)
    )
    pivot["total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("total", ascending=False).drop(columns="total")

    print("\n=== Distribuição por source e arquitetura ===")
    print(pivot)

    source_colors = {
        "readme_search": DEFAULT_ORANGE,
        "repo_search": DEFAULT_BLUE,
    }
    ordered_colors = [source_colors.get(column, DEFAULT_BLUE) for column in pivot.columns]
    ax = pivot.plot(kind="bar", figsize=(11, 6), color=ordered_colors)
    ax.set_title("Repositórios por arquitetura e origem da busca (GitHub)")
    ax.set_xlabel("Arquitetura")
    ax.set_ylabel("Número de repositórios")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_png = OUTPUTS_GITHUB_DIR / "grafico_freq_arquiteturas_por_source.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def evolucao_temporal(repos: pd.DataFrame) -> None:
    """Calcula e plota a evolução mensal de repositórios por arquitetura."""
    repos_arch = explodir_keywords(repos, "detected_architectures", "arch")
    repos_arch["year_month"] = repos_arch["created_iso"].dt.to_period("M").dt.to_timestamp()

    counts = (
        repos_arch.groupby(["year_month", "arch"])
        .size()
        .reset_index(name="num_repos")
        .sort_values(["year_month", "num_repos"], ascending=[True, False])
    )

    print("\n=== Evolução temporal por arquitetura (repos criados por mês) ===")
    print(counts)

    out_csv = DATA_PROCESSED_GITHUB_DIR / "evolucao_arquiteturas_mes.csv"
    counts.to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    plt.figure(figsize=(11, 6))
    plt.gca().set_prop_cycle(cycler(color=LINE_COLORS_NO_RED))
    for architecture, df_arch in counts.groupby("arch"):
        plt.plot(
            df_arch["year_month"],
            df_arch["num_repos"],
            marker="o",
            label=architecture,
        )

    plt.title("Evolução mensal de repositórios por arquitetura (GitHub — SwiftUI)")
    plt.xlabel("Ano-Mês")
    plt.ylabel("Número de repositórios criados")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    out_png = OUTPUTS_GITHUB_DIR / "grafico_evolucao_arquiteturas_mes.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def popularidade_por_arquitetura(repos: pd.DataFrame) -> None:
    """Calcula e plota stars e forks por arquitetura como métrica de popularidade."""
    repos_arch = explodir_keywords(repos, "detected_architectures", "arch")

    pop = (
        repos_arch.groupby("arch")
        .agg(
            stars_total=("stars", "sum"),
            stars_medio=("stars", "mean"),
            forks_total=("forks", "sum"),
            forks_medio=("forks", "mean"),
            num_repos=("repo_id", "count"),
        )
        .round(2)
        .sort_values("stars_total", ascending=False)
    )

    print("\n=== Popularidade por arquitetura (stars e forks) ===")
    print(pop)

    out_csv = DATA_PROCESSED_GITHUB_DIR / "popularidade_por_arquitetura.csv"
    pop.reset_index().to_csv(out_csv, index=False)
    print(f"Salvo: {out_csv}")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].bar(pop.index, pop["stars_total"], color=DEFAULT_BLUE)
    axes[0].set_title("Total de stars por arquitetura")
    axes[0].set_xlabel("Arquitetura")
    axes[0].set_ylabel("Stars")
    axes[0].tick_params(axis="x", rotation=30)

    axes[1].bar(pop.index, pop["stars_medio"], color=DEFAULT_BLUE)
    axes[1].set_title("Média de stars por repositório")
    axes[1].set_xlabel("Arquitetura")
    axes[1].set_ylabel("Stars médias")
    axes[1].tick_params(axis="x", rotation=30)

    plt.tight_layout()
    out_png = OUTPUTS_GITHUB_DIR / "grafico_popularidade_por_arquitetura.png"
    plt.savefig(out_png, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


def main() -> None:
    """Executa o fluxo completo de análise dos dados coletados do GitHub."""
    DATA_PROCESSED_GITHUB_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_GITHUB_DIR.mkdir(parents=True, exist_ok=True)

    if not ARQ_REPOS.exists():
        print(f"ERRO: Arquivo não encontrado: {ARQ_REPOS}")
        return

    repos = carregar_dados()
    repos = preparar_arquiteturas(repos)

    print(f"Repositórios carregados: {len(repos)}")
    print(f"Arquiteturas monitoradas: {', '.join(ARCHITECTURE_ALIASES.keys())}")

    salvar_arquiteturas_monitoradas()
    salvar_base_reclassificada(repos)
    frequencia_arquiteturas(repos)
    distribuicao_por_source(repos)
    evolucao_temporal(repos)
    popularidade_por_arquitetura(repos)


if __name__ == "__main__":
    main()
