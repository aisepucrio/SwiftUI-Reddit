from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")


BASE_DIR = Path(__file__).resolve().parent.parent.parent

DATA_PROCESSED_REDDIT_DIR = BASE_DIR / "data" / "processed" / "reddit"
DATA_PROCESSED_FORMS_DIR = BASE_DIR / "data" / "processed" / "forms"
DATA_PROCESSED_COMPARISON_DIR = BASE_DIR / "data" / "processed" / "comparacao"
OUTPUTS_COMPARISON_DIR = BASE_DIR / "outputs" / "comparacao"

ARQ_REDDIT_FREQ = DATA_PROCESSED_REDDIT_DIR / "freq_arquiteturas_total.csv"
ARQ_REDDIT_SENT = DATA_PROCESSED_REDDIT_DIR / "sentimento_por_arquitetura_resumo.csv"
ARQ_FORMS_USED = DATA_PROCESSED_FORMS_DIR / "arquiteturas_utilizadas_forms.csv"
ARQ_FORMS_MAIN = DATA_PROCESSED_FORMS_DIR / "arquitetura_principal_forms.csv"
ARQ_FORMS_BEST = DATA_PROCESSED_FORMS_DIR / "arquitetura_mais_adequada_forms.csv"


def salvar_csv(df: pd.DataFrame, filename: str) -> None:
    """Salva um DataFrame no diretório de comparações processadas."""
    out = DATA_PROCESSED_COMPARISON_DIR / filename
    df.to_csv(out, index=False)
    print(f"Salvo: {out}")


def salvar_plot(filename: str) -> None:
    """Ajusta, salva e fecha o gráfico Matplotlib atual."""
    out = OUTPUTS_COMPARISON_DIR / filename
    plt.tight_layout()
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out}")


def carregar_reddit() -> pd.DataFrame:
    """Carrega métricas consolidadas do Reddit e calcula participação e ranking."""
    reddit = pd.read_csv(ARQ_REDDIT_FREQ).rename(
        columns={
            "architecture": "arquitetura",
            "count_posts": "reddit_posts",
            "count_comments": "reddit_comments",
            "count_total": "reddit_total",
        }
    )
    reddit["reddit_share"] = reddit["reddit_total"] / reddit["reddit_total"].sum()
    reddit["reddit_rank"] = reddit["reddit_total"].rank(method="dense", ascending=False).astype(int)
    return reddit


def carregar_forms() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Carrega respostas do Forms e calcula participação e ranking por pergunta."""
    used = pd.read_csv(ARQ_FORMS_USED).rename(
        columns={"architecture": "arquitetura", "quantidade": "forms_ja_utilizou"}
    )
    main = pd.read_csv(ARQ_FORMS_MAIN).rename(
        columns={"architecture": "arquitetura", "quantidade": "forms_principal"}
    )
    best = pd.read_csv(ARQ_FORMS_BEST).rename(
        columns={"architecture": "arquitetura", "quantidade": "forms_mais_adequada"}
    )

    for df, col in [
        (used, "forms_ja_utilizou"),
        (main, "forms_principal"),
        (best, "forms_mais_adequada"),
    ]:
        total = df[col].sum()
        df[f"{col}_share"] = df[col] / total if total else 0
        df[f"{col}_rank"] = df[col].rank(method="dense", ascending=False).astype(int)

    return used, main, best


def carregar_sentimento() -> pd.DataFrame:
    """Carrega o resumo de sentimento do Reddit e calcula proporções por classe."""
    sent = pd.read_csv(ARQ_REDDIT_SENT).rename(columns={"arch": "arquitetura"})
    total = sent[["positivo", "neutro", "negativo"]].sum(axis=1)
    sent["sent_total"] = total
    sent["sent_pos_share"] = sent["positivo"] / total
    sent["sent_neu_share"] = sent["neutro"] / total
    sent["sent_neg_share"] = sent["negativo"] / total
    sent["sentimento_predominante"] = sent[["positivo", "neutro", "negativo"]].idxmax(axis=1)
    return sent


def consolidar() -> pd.DataFrame:
    """Integra Reddit, Forms e sentimento em uma base comparativa única."""
    reddit = carregar_reddit()
    forms_used, forms_main, forms_best = carregar_forms()
    sent = carregar_sentimento()

    base = reddit.merge(forms_used, on="arquitetura", how="outer")
    base = base.merge(forms_main, on="arquitetura", how="outer")
    base = base.merge(forms_best, on="arquitetura", how="outer")
    base = base.merge(sent, on="arquitetura", how="left")

    numeric_cols = [
        "reddit_posts",
        "reddit_comments",
        "reddit_total",
        "reddit_share",
        "reddit_rank",
        "forms_ja_utilizou",
        "forms_ja_utilizou_share",
        "forms_ja_utilizou_rank",
        "forms_principal",
        "forms_principal_share",
        "forms_principal_rank",
        "forms_mais_adequada",
        "forms_mais_adequada_share",
        "forms_mais_adequada_rank",
        "positivo",
        "neutro",
        "negativo",
        "sent_total",
        "sent_pos_share",
        "sent_neu_share",
        "sent_neg_share",
    ]
    for col in numeric_cols:
        if col in base.columns:
            base[col] = base[col].fillna(0)

    base["presente_no_reddit"] = base["reddit_total"] > 0
    base["presente_no_forms_principal"] = base["forms_principal"] > 0
    base["presente_no_forms_adequada"] = base["forms_mais_adequada"] > 0
    base["presente_no_forms_utilizada"] = base["forms_ja_utilizou"] > 0

    base["delta_share_reddit_vs_forms_principal"] = (
        base["reddit_share"] - base["forms_principal_share"]
    )
    base["delta_share_reddit_vs_forms_adequada"] = (
        base["reddit_share"] - base["forms_mais_adequada_share"]
    )
    base["delta_share_forms_principal_vs_adequada"] = (
        base["forms_principal_share"] - base["forms_mais_adequada_share"]
    )

    base["gap_absoluto_reddit_vs_forms_principal"] = (
        base["delta_share_reddit_vs_forms_principal"].abs()
    )
    base["gap_absoluto_reddit_vs_forms_adequada"] = (
        base["delta_share_reddit_vs_forms_adequada"].abs()
    )

    base["delta_rank_reddit_vs_forms_principal"] = (
        base["reddit_rank"].replace(0, pd.NA) - base["forms_principal_rank"].replace(0, pd.NA)
    )
    base["delta_rank_reddit_vs_forms_adequada"] = (
        base["reddit_rank"].replace(0, pd.NA) - base["forms_mais_adequada_rank"].replace(0, pd.NA)
    )

    base["indice_alinhamento"] = 1 - (
        (base["gap_absoluto_reddit_vs_forms_principal"] + base["gap_absoluto_reddit_vs_forms_adequada"]) / 2
    )

    base["tipo_cruzamento"] = "parcial"
    base.loc[
        base["presente_no_reddit"]
        & base["presente_no_forms_principal"]
        & base["presente_no_forms_adequada"],
        "tipo_cruzamento",
    ] = "alinhado_nas_duas_fontes"
    base.loc[
        base["presente_no_reddit"]
        & ~base["presente_no_forms_principal"]
        & ~base["presente_no_forms_adequada"],
        "tipo_cruzamento",
    ] = "so_reddit"
    base.loc[
        ~base["presente_no_reddit"]
        & (base["presente_no_forms_principal"] | base["presente_no_forms_adequada"]),
        "tipo_cruzamento",
    ] = "so_forms"

    base = base.sort_values(
        ["indice_alinhamento", "reddit_total", "forms_principal", "forms_mais_adequada"],
        ascending=[False, False, False, False],
    )
    return base


def cruzamento_ranking(base: pd.DataFrame) -> None:
    """Gera CSV com rankings, participações e diferenças entre fontes."""
    ranking = base[
        [
            "arquitetura",
            "reddit_total",
            "reddit_share",
            "reddit_rank",
            "forms_ja_utilizou",
            "forms_ja_utilizou_share",
            "forms_ja_utilizou_rank",
            "forms_principal",
            "forms_principal_share",
            "forms_principal_rank",
            "forms_mais_adequada",
            "forms_mais_adequada_share",
            "forms_mais_adequada_rank",
            "delta_share_reddit_vs_forms_principal",
            "delta_share_reddit_vs_forms_adequada",
            "delta_share_forms_principal_vs_adequada",
            "delta_rank_reddit_vs_forms_principal",
            "delta_rank_reddit_vs_forms_adequada",
            "gap_absoluto_reddit_vs_forms_principal",
            "gap_absoluto_reddit_vs_forms_adequada",
            "indice_alinhamento",
            "tipo_cruzamento",
        ]
    ].copy()
    salvar_csv(ranking, "cruzamento_rankings_fontes.csv")


def cruzamento_convergencia(base: pd.DataFrame) -> None:
    """Separa arquiteturas alinhadas, discrepantes e exclusivas por fonte."""
    alinhadas = base[base["tipo_cruzamento"] == "alinhado_nas_duas_fontes"].copy()
    alinhadas = alinhadas.sort_values("indice_alinhamento", ascending=False)
    salvar_csv(alinhadas, "cruzamento_arquiteturas_alinhadas.csv")

    discrepantes = base.sort_values("gap_absoluto_reddit_vs_forms_principal", ascending=False)
    salvar_csv(discrepantes, "cruzamento_arquiteturas_discrepantes.csv")

    so_reddit = base[base["tipo_cruzamento"] == "so_reddit"].copy()
    salvar_csv(so_reddit, "cruzamento_arquiteturas_so_reddit.csv")

    so_forms = base[base["tipo_cruzamento"] == "so_forms"].copy()
    salvar_csv(so_forms, "cruzamento_arquiteturas_so_forms.csv")


def cruzamento_sentimento(base: pd.DataFrame) -> None:
    """Cruza métricas de sentimento do Reddit com adesão declarada no Forms."""
    sentiment = base[
        [
            "arquitetura",
            "reddit_total",
            "forms_principal",
            "forms_mais_adequada",
            "positivo",
            "neutro",
            "negativo",
            "sent_pos_share",
            "sent_neu_share",
            "sent_neg_share",
            "sentimento_predominante",
            "indice_alinhamento",
        ]
    ].copy()
    sentiment = sentiment.sort_values(["reddit_total", "forms_principal"], ascending=False)
    salvar_csv(sentiment, "cruzamento_sentimento_fontes.csv")

    resumo = (
        base.groupby("sentimento_predominante")
        .agg(
            arquiteturas=("arquitetura", "count"),
            reddit_total=("reddit_total", "sum"),
            forms_principal=("forms_principal", "sum"),
            forms_mais_adequada=("forms_mais_adequada", "sum"),
        )
        .reset_index()
    )
    salvar_csv(resumo, "cruzamento_resumo_sentimento_fontes.csv")


def cruzamento_resumo(base: pd.DataFrame) -> None:
    """Cria um resumo executivo com os principais indicadores comparativos."""
    rows = []

    if not base.empty:
        mais_alinhada = base.sort_values("indice_alinhamento", ascending=False).iloc[0]
        rows.append(
            {
                "indicador": "arquitetura_mais_alinhada",
                "arquitetura": mais_alinhada["arquitetura"],
                "valor": round(float(mais_alinhada["indice_alinhamento"]), 4),
            }
        )

        maior_gap = base.sort_values("gap_absoluto_reddit_vs_forms_principal", ascending=False).iloc[0]
        rows.append(
            {
                "indicador": "maior_gap_reddit_vs_forms_principal",
                "arquitetura": maior_gap["arquitetura"],
                "valor": round(float(maior_gap["gap_absoluto_reddit_vs_forms_principal"]), 4),
            }
        )

        mais_discutida = base.sort_values("reddit_total", ascending=False).iloc[0]
        rows.append(
            {
                "indicador": "mais_discutida_no_reddit",
                "arquitetura": mais_discutida["arquitetura"],
                "valor": int(mais_discutida["reddit_total"]),
            }
        )

        mais_usada = base.sort_values("forms_principal", ascending=False).iloc[0]
        rows.append(
            {
                "indicador": "mais_usada_no_forms",
                "arquitetura": mais_usada["arquitetura"],
                "valor": int(mais_usada["forms_principal"]),
            }
        )

        mais_adequada = base.sort_values("forms_mais_adequada", ascending=False).iloc[0]
        rows.append(
            {
                "indicador": "mais_adequada_no_forms",
                "arquitetura": mais_adequada["arquitetura"],
                "valor": int(mais_adequada["forms_mais_adequada"]),
            }
        )

    salvar_csv(pd.DataFrame(rows), "cruzamento_resumo_executivo.csv")


def grafico_participacao_relativa(base: pd.DataFrame) -> None:
    """Gera gráfico comparando participações relativas entre Reddit e Forms."""
    top = base.sort_values("indice_alinhamento", ascending=False).head(8)
    plot_df = top[
        ["arquitetura", "reddit_share", "forms_principal_share", "forms_mais_adequada_share"]
    ].set_index("arquitetura")

    plot_df.plot(kind="bar", figsize=(12, 6))
    plt.title("Cruzamento de participação relativa entre Reddit e Forms")
    plt.xlabel("Arquitetura")
    plt.ylabel("Participação relativa")
    plt.xticks(rotation=30, ha="right")
    salvar_plot("grafico_cruzamento_participacao_relativa.png")


def grafico_gap_reddit_forms(base: pd.DataFrame) -> None:
    """Gera gráfico do gap de participação entre Reddit e arquitetura principal no Forms."""
    gap = base.sort_values("delta_share_reddit_vs_forms_principal", ascending=False)
    colors = ["#2b6cb0" if value >= 0 else "#c53030" for value in gap["delta_share_reddit_vs_forms_principal"]]

    plt.figure(figsize=(12, 6))
    plt.bar(gap["arquitetura"], gap["delta_share_reddit_vs_forms_principal"], color=colors)
    plt.axhline(0, color="black", linewidth=0.8)
    plt.title("Gap entre Reddit e Forms (arquitetura principal)")
    plt.xlabel("Arquitetura")
    plt.ylabel("Reddit share - Forms principal share")
    plt.xticks(rotation=30, ha="right")
    salvar_plot("grafico_cruzamento_gap_reddit_forms.png")


def grafico_sentimento_vs_adesao(base: pd.DataFrame) -> None:
    """Gera gráfico combinando sentimento no Reddit e participação no Forms."""
    top = base.sort_values("reddit_total", ascending=False).head(8)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.bar(top["arquitetura"], top["sent_pos_share"], label="Positivo", alpha=0.75)
    ax1.bar(
        top["arquitetura"],
        top["sent_neg_share"],
        bottom=top["sent_pos_share"],
        label="Negativo",
        alpha=0.75,
    )
    ax1.set_xlabel("Arquitetura")
    ax1.set_ylabel("Participação do sentimento no Reddit")
    ax1.set_title("Cruzamento entre sentimento no Reddit e adoção no Forms")
    ax1.tick_params(axis="x", rotation=30)

    ax2 = ax1.twinx()
    ax2.plot(
        top["arquitetura"],
        top["forms_principal_share"],
        color="black",
        marker="o",
        label="Forms principal share",
    )
    ax2.plot(
        top["arquitetura"],
        top["forms_mais_adequada_share"],
        color="green",
        marker="s",
        label="Forms adequada share",
    )
    ax2.set_ylabel("Participação no Forms")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    salvar_plot("grafico_cruzamento_sentimento_adesao.png")


def main() -> None:
    """Executa a consolidação, cruzamentos e gráficos comparativos entre fontes."""
    DATA_PROCESSED_COMPARISON_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_COMPARISON_DIR.mkdir(parents=True, exist_ok=True)

    required = [
        ARQ_REDDIT_FREQ,
        ARQ_REDDIT_SENT,
        ARQ_FORMS_USED,
        ARQ_FORMS_MAIN,
        ARQ_FORMS_BEST,
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        print("ERRO: arquivos necessários não encontrados:")
        for path in missing:
            print(f"- {path}")
        return

    base = consolidar()
    salvar_csv(base, "cruzamento_fontes_consolidado.csv")

    cruzamento_ranking(base)
    cruzamento_convergencia(base)
    cruzamento_sentimento(base)
    cruzamento_resumo(base)

    grafico_participacao_relativa(base)
    grafico_gap_reddit_forms(base)
    grafico_sentimento_vs_adesao(base)


if __name__ == "__main__":
    main()
