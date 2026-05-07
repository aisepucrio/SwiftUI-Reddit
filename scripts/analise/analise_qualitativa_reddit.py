import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional, Set

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    matplotlib = None
    plt = None


BASE_DIR = Path(__file__).resolve().parent.parent.parent
REDDIT_DIR = BASE_DIR / "data" / "processed" / "reddit"
DATA_OUT_DIR = BASE_DIR / "data" / "processed" / "qualitativa"
OUTPUTS_DIR = BASE_DIR / "outputs" / "qualitativa"

POSTS_FILE = REDDIT_DIR / "reddit_posts_reclassificados.csv"
COMMENTS_FILE = REDDIT_DIR / "reddit_comments_reclassificados.csv"

TARGET_POSTS = 30
TARGET_COMMENTS = 30

CATEGORY_LABELS = {
    "adequacao_tecnica": "Adequacao tecnica",
    "beneficio_percebido": "Beneficio percebido",
    "custo_percebido": "Custo percebido",
    "adocao_e_aprendizado": "Adocao e aprendizado",
    "estrutura_arquitetural": "Estrutura arquitetural",
    "percepcao_geral": "Percepcao geral",
}

CODE_LABELS = {
    "aderencia_swiftui": "Aderencia ao SwiftUI",
    "simplicidade": "Simplicidade",
    "complexidade_excessiva": "Complexidade excessiva",
    "boilerplate": "Boilerplate",
    "gestao_estado": "Gestao de estado",
    "escalabilidade": "Escalabilidade",
    "manutenibilidade": "Manutenibilidade",
    "testabilidade": "Testabilidade",
    "aprendizado_curva": "Curva de aprendizado",
    "organizacao_camadas": "Organizacao de camadas",
    "avaliacao_geral_arquitetura": "Avaliacao geral da arquitetura",
}

THEME_RULES = [
    {
        "code": "aderencia_swiftui",
        "category": "adequacao_tecnica",
        "patterns": [
            r"fit[s]? naturally",
            r"works? well with swiftui",
            r"good fit",
            r"best for swiftui",
            r"suits? swiftui",
            r"idiomatic",
            r"swiftui way",
            r"natural fit",
        ],
    },
    {
        "code": "simplicidade",
        "category": "beneficio_percebido",
        "patterns": [
            r"\bsimple\b",
            r"\bsimplicity\b",
            r"\beasy\b",
            r"\bcleaner\b",
            r"\bstraightforward\b",
            r"\bless code\b",
            r"\bminimal\b",
        ],
    },
    {
        "code": "complexidade_excessiva",
        "category": "custo_percebido",
        "patterns": [
            r"\bcomplex\b",
            r"\btoo much\b",
            r"\boverkill\b",
            r"\bheavy\b",
            r"\bover-engineer",
            r"\bcomplicated\b",
            r"\btoo many layers\b",
        ],
    },
    {
        "code": "boilerplate",
        "category": "custo_percebido",
        "patterns": [
            r"\bboilerplate\b",
            r"\bverbose\b",
            r"\btoo much code\b",
            r"\bceremony\b",
        ],
    },
    {
        "code": "gestao_estado",
        "category": "adequacao_tecnica",
        "patterns": [
            r"state management",
            r"single source of truth",
            r"observable",
            r"@state",
            r"@stateobject",
            r"@observedobject",
            r"@environmentobject",
            r"\bstore\b",
        ],
    },
    {
        "code": "escalabilidade",
        "category": "beneficio_percebido",
        "patterns": [
            r"\bscalab",
            r"\blarge app",
            r"\bbig project",
            r"\bgrow\b",
            r"\bmaintain at scale\b",
        ],
    },
    {
        "code": "manutenibilidade",
        "category": "beneficio_percebido",
        "patterns": [
            r"\bmaintain",
            r"\bmaintenance\b",
            r"\brefactor\b",
            r"\borganization\b",
            r"\borganize\b",
        ],
    },
    {
        "code": "testabilidade",
        "category": "beneficio_percebido",
        "patterns": [
            r"\btestable\b",
            r"\btestability\b",
            r"\bunit test",
            r"\bmock\b",
        ],
    },
    {
        "code": "aprendizado_curva",
        "category": "adocao_e_aprendizado",
        "patterns": [
            r"\blearn\b",
            r"\blearning curve\b",
            r"\bhard to understand\b",
            r"\bdifficult\b",
            r"\bconfusing\b",
        ],
    },
    {
        "code": "organizacao_camadas",
        "category": "estrutura_arquitetural",
        "patterns": [
            r"\bview model\b",
            r"\bviewmodel\b",
            r"\bcoordinator\b",
            r"\busecase\b",
            r"\brepository\b",
            r"\blayer\b",
            r"\bseparation\b",
        ],
    },
]

INTERPRETIVE_THEMES = [
    {
        "title": "Pragmatismo e simplicidade favorecem MVVM",
        "codes": {"simplicidade", "organizacao_camadas", "avaliacao_geral_arquitetura"},
    },
    {
        "title": "Gestao de estado e navegacao concentram a tensao tecnica",
        "codes": {"gestao_estado", "organizacao_camadas", "aderencia_swiftui"},
    },
    {
        "title": "Teste e escala aparecem como justificativa para mais estrutura",
        "codes": {"testabilidade", "escalabilidade", "manutenibilidade"},
    },
    {
        "title": "Arquiteturas mais sofisticadas cobram custo de adocao",
        "codes": {"complexidade_excessiva", "boilerplate", "aprendizado_curva"},
    },
]


def load_rows(path: Path, row_type: str) -> list[dict]:
    """Carrega posts ou comentários reclassificados e normaliza unidades de análise."""
    rows = []
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            arches = [a.strip() for a in row.get("detected_architectures", "").split(",") if a.strip()]
            if not arches:
                continue

            if row_type == "post":
                text = f"{row.get('title', '').strip()} {row.get('selftext', '').strip()}".strip()
                weight = float(row.get("score") or 0) + float(row.get("num_comments") or 0)
                unit_id = row.get("id", "")
                excerpt_source = row.get("selftext", "") or row.get("title", "")
            else:
                text = row.get("body", "").strip()
                weight = float(row.get("score") or 0)
                unit_id = row.get("comment_id", "")
                excerpt_source = row.get("body", "")

            rows.append(
                {
                    "unit_id": unit_id,
                    "type": row_type,
                    "subreddit": row.get("subreddit", ""),
                    "architectures": arches,
                    "primary_architecture": arches[0],
                    "weight": weight,
                    "score": float(row.get("score") or 0),
                    "num_comments": float(row.get("num_comments") or 0) if row_type == "post" else "",
                    "title": row.get("title", ""),
                    "text": text,
                    "excerpt": clean_excerpt(excerpt_source, 350),
                }
            )
    return rows


def diverse_top_sample(rows: list[dict], target: int) -> list[dict]:
    """Seleciona uma amostra diversa por arquitetura priorizando maior engajamento."""
    rows_sorted = sorted(rows, key=lambda r: (r["weight"], r["score"]), reverse=True)
    by_arch = defaultdict(list)
    for row in rows_sorted:
        for arch in row["architectures"]:
            by_arch[arch].append(row)

    selected = []
    seen = set()

    for arch, arch_rows in by_arch.items():
        added = 0
        for row in arch_rows:
            if row["unit_id"] in seen:
                continue
            selected.append(row)
            seen.add(row["unit_id"])
            added += 1
            if added == 3:
                break

    for row in rows_sorted:
        if len(selected) >= target:
            break
        if row["unit_id"] in seen:
            continue
        selected.append(row)
        seen.add(row["unit_id"])

    return selected[:target]


def apply_codes(text: str) -> tuple[list[str], list[str]]:
    """Aplica regras temáticas ao texto e retorna códigos e categorias detectadas."""
    text_lower = text.lower()
    codes = []
    categories = []
    for rule in THEME_RULES:
        if any(re.search(pattern, text_lower) for pattern in rule["patterns"]):
            codes.append(rule["code"])
            categories.append(rule["category"])

    if not codes:
        codes = ["avaliacao_geral_arquitetura"]
        categories = ["percepcao_geral"]

    return sorted(set(codes)), sorted(set(categories))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Escreve linhas de dicionários em CSV com ordem explícita de colunas."""
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def pct(count: int, total: int) -> str:
    """Formata uma razão como percentual com vírgula decimal em português."""
    if total == 0:
        return "0,0%"
    return f"{(count / total) * 100:.1f}%".replace(".", ",")


def clean_excerpt(text: str, limit: int = 180) -> str:
    """Compacta espaços e limita um trecho textual para uso em relatórios."""
    text = re.sub(r"\s+", " ", (text or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def pick_examples(rows: list[dict], desired_codes: set[str], limit: int = 2, excluded: Optional[Set[str]] = None) -> list[dict]:
    """Seleciona exemplos representativos que contenham códigos desejados."""
    selected = []
    seen = set(excluded or set())
    for row in rows:
        row_codes = {item.strip() for item in row["codes"].split(",") if item.strip()}
        if not row_codes.intersection(desired_codes):
            continue
        if row["excerpt"] in seen:
            continue
        selected.append(row)
        seen.add(row["excerpt"])
        if len(selected) >= limit:
            break
    return selected


def format_quote(row: dict) -> str:
    """Formata um trecho selecionado como citação curta para Markdown."""
    return f'- "{clean_excerpt(row["excerpt"])}" ({row["type"]} / {row["primary_architecture"]})'


def save_bar_chart(rows: list[dict], label_key: str, value_key: str, title: str, filename: str, figsize: tuple[float, float]) -> bool:
    """Gera um gráfico de barras para resumos qualitativos, se Matplotlib existir."""
    if plt is None:
        return False

    labels = [row[label_key] for row in rows]
    values = [int(row[value_key]) for row in rows]
    plt.figure(figsize=figsize)
    bars = plt.bar(labels, values, color="#2F6B6F")
    plt.title(title)
    plt.ylabel("Ocorrencias")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    for bar, value in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, value + 0.15, str(value), ha="center", va="bottom", fontsize=9)
    plt.savefig(OUTPUTS_DIR / filename, dpi=300)
    plt.close()
    return True


def save_heatmap(arch_category_rows: list[dict]) -> bool:
    """Gera um heatmap de arquitetura por categoria qualitativa."""
    if plt is None:
        return False

    architectures = sorted({row["primary_architecture"] for row in arch_category_rows})
    categories = [key for key in CATEGORY_LABELS if any(row["category"] == key for row in arch_category_rows)]
    matrix = []
    for arch in architectures:
        row_counts = []
        for category in categories:
            value = 0
            for row in arch_category_rows:
                if row["primary_architecture"] == arch and row["category"] == category:
                    value = int(row["count"])
                    break
            row_counts.append(value)
        matrix.append(row_counts)

    plt.figure(figsize=(9, 4.8))
    plt.imshow(matrix, cmap="YlGnBu", aspect="auto")
    plt.title("Arquitetura x categoria na analise qualitativa")
    plt.xticks(range(len(categories)), [CATEGORY_LABELS[item] for item in categories], rotation=25, ha="right")
    plt.yticks(range(len(architectures)), architectures)
    for i, row_counts in enumerate(matrix):
        for j, value in enumerate(row_counts):
            plt.text(j, i, value, ha="center", va="center", color="black", fontsize=9)
    plt.colorbar(label="Ocorrencias")
    plt.tight_layout()
    plt.savefig(OUTPUTS_DIR / "grafico_qualitativa_heatmap_arquitetura_categoria.png", dpi=300)
    plt.close()
    return True


def build_report(coded_rows: list[dict], category_rows: list[dict], code_rows: list[dict]) -> str:
    """Monta o relatório Markdown consolidando amostra, categorias e achados."""
    total_units = len(coded_rows)
    type_counter = Counter(row["type"] for row in coded_rows)
    arch_counter = Counter(row["primary_architecture"] for row in coded_rows)
    sorted_rows = sorted(
        coded_rows,
        key=lambda row: (
            -(float(row["score"] or 0)),
            -(float(row["num_comments"] or 0) if row["num_comments"] else 0),
        ),
    )

    theme_sections = []
    used_quotes = set()
    for theme in INTERPRETIVE_THEMES:
        examples = pick_examples(sorted_rows, theme["codes"], limit=2, excluded=used_quotes)
        used_quotes.update(row["excerpt"] for row in examples)
        lines = [f"### {theme['title']}", "", "Evidencias:"]
        lines.extend(format_quote(example) for example in examples)
        theme_sections.append("\n".join(lines))

    arch_profiles = []
    for arch, count in arch_counter.most_common():
        arch_rows = [row for row in coded_rows if row["primary_architecture"] == arch]
        local_categories = Counter()
        local_codes = Counter()
        for row in arch_rows:
            local_categories.update(item.strip() for item in row["categories"].split(",") if item.strip())
            local_codes.update(item.strip() for item in row["codes"].split(",") if item.strip())
        top_category = local_categories.most_common(1)[0][0]
        top_code = local_codes.most_common(1)[0][0]
        example = pick_examples(arch_rows, {top_code}, limit=1)
        arch_profiles.append(
            "\n".join(
                [
                    f"### {arch}",
                    f"- Presenca na amostra: {count} de {total_units} unidades ({pct(count, total_units)})",
                    f"- Categoria predominante: {CATEGORY_LABELS.get(top_category, top_category)}",
                    f"- Codigo predominante: {CODE_LABELS.get(top_code, top_code)}",
                    f"- Exemplo: {format_quote(example[0])[2:] if example else 'Sem exemplo selecionado'}",
                ]
            )
        )

    return "\n".join(
        [
            "# Analise qualitativa consolidada do Reddit",
            "",
            "## Escopo",
            f"- Corpus qualitativo: {total_units} unidades, sendo {type_counter['post']} posts e {type_counter['comment']} comentarios.",
            "- Selecionadas por relevancia/engajamento e diversificacao entre arquiteturas detectadas.",
            "- Analise baseada em codificacao tematica inicial por regras, seguida de consolidacao interpretativa.",
            "",
            "## Procedimento",
            "1. Selecionar amostra intencional de posts e comentarios.",
            "2. Aplicar codigos tematicos iniciais.",
            "3. Agrupar os codigos em categorias analiticas.",
            "4. Interpretar frequencias e trechos representativos.",
            "",
            "## Distribuicao da amostra",
            *[f"- {arch}: {count} unidades ({pct(count, total_units)})" for arch, count in arch_counter.most_common()],
            "",
            "## Categorias mais recorrentes",
            *[
                f"- {CATEGORY_LABELS.get(row['category'], row['category'])}: {row['count']} ocorrencias ({pct(int(row['count']), total_units)})"
                for row in category_rows
            ],
            "",
            "## Codigos mais recorrentes",
            *[
                f"- {CODE_LABELS.get(row['code'], row['code'])}: {row['count']} ocorrencias ({pct(int(row['count']), total_units)})"
                for row in code_rows[:8]
            ],
            "",
            "## Achados centrais",
            "",
            *theme_sections,
            "",
            "## Perfis por arquitetura",
            "",
            *arch_profiles,
            "",
            "## Sintese interpretativa",
            "- MVVM domina o corpus e aparece associado a simplicidade, organizacao e uso pragmatico.",
            "- TCA tem visibilidade relevante, mas aparece cercado por disputa, defesa tecnica e critica a complexidade.",
            "- Estado, navegacao e separacao de responsabilidades sao o centro da tensao arquitetural em SwiftUI.",
            "- Em projetos maiores, a conversa se desloca para teste, manutenibilidade e escalabilidade.",
            "",
            "## O que esta analise permite afirmar",
            "- Nao ha uma arquitetura unica tratada como consenso absoluto.",
            "- A escolha arquitetural e apresentada como equilibrio entre simplicidade, controle de estado e capacidade de escala.",
            "- O ecossistema SwiftUI valoriza solucoes pragmaticas, mas nao elimina a busca por mais estrutura em cenarios complexos.",
            "",
            "## Limitacoes",
            "- Codificacao inicial automatizada por regras.",
            "- Amostra qualitativa intencional, nao probabilistica.",
            "- O material e adequado para discussao e apresentacao, mas ainda comporta refinamento manual posterior.",
        ]
    )


def build_speaking_notes() -> str:
    """Monta um roteiro curto de fala para apresentar a análise qualitativa."""
    return "\n".join(
        [
            "# Roteiro de fala",
            "",
            "## Como a analise foi feita",
            "Eu selecionei uma amostra qualitativa de posts e comentarios do Reddit com arquiteturas detectadas, priorizando relevancia e diversidade. Em seguida, apliquei uma codificacao tematica inicial para identificar os temas mais recorrentes nas discussoes sobre SwiftUI.",
            "",
            "## O que apareceu com mais forca",
            "Os temas mais fortes foram simplicidade, organizacao de camadas, gestao de estado, testabilidade e complexidade. Isso mostra que a discussao arquitetural nao gira so em torno de nomes de padroes, mas principalmente de trade-offs praticos.",
            "",
            "## O que os dados sugerem",
            "MVVM aparece como a solucao mais recorrente e mais pragmatica. Ja TCA aparece como alternativa forte para cenarios mais estruturados, mas acompanhada de debate sobre boilerplate, curva de aprendizado e excesso de complexidade.",
            "",
            "## Conclusao defendivel",
            "A conclusao mais segura e que o ecossistema SwiftUI nao converge para uma unica arquitetura ideal. O criterio principal e o equilibrio entre simplicidade, controle de estado e capacidade de crescimento do projeto.",
            "",
            "## Limitacao",
            "Esta e uma analise qualitativa exploratoria e sistematizada, baseada em codificacao inicial por regras. Ela sustenta bem a discussao do trabalho, mas ainda pode ser refinada manualmente em uma rodada posterior.",
        ]
    )


def main() -> None:
    """Executa a análise qualitativa do Reddit e gera CSVs, relatório e gráficos."""
    DATA_OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    posts = load_rows(POSTS_FILE, "post")
    comments = load_rows(COMMENTS_FILE, "comment")

    sample_posts = diverse_top_sample(posts, TARGET_POSTS)
    sample_comments = diverse_top_sample(comments, TARGET_COMMENTS)
    sample = sample_posts + sample_comments

    coded_rows = []
    category_counter = Counter()
    code_counter = Counter()
    arch_category_counter = Counter()

    for row in sample:
        codes, categories = apply_codes(row["text"])
        category_counter.update(categories)
        code_counter.update(codes)
        for category in categories:
            arch_category_counter[(row["primary_architecture"], category)] += 1

        coded_rows.append(
            {
                "unit_id": row["unit_id"],
                "type": row["type"],
                "subreddit": row["subreddit"],
                "primary_architecture": row["primary_architecture"],
                "all_architectures": ", ".join(row["architectures"]),
                "score": row["score"],
                "num_comments": row["num_comments"],
                "codes": ", ".join(codes),
                "categories": ", ".join(categories),
                "title": row["title"],
                "excerpt": row["excerpt"],
            }
        )

    sample_rows = [
        {
            "unit_id": row["unit_id"],
            "type": row["type"],
            "subreddit": row["subreddit"],
            "primary_architecture": row["primary_architecture"],
            "all_architectures": ", ".join(row["architectures"]),
            "score": row["score"],
            "num_comments": row["num_comments"],
            "title": row["title"],
            "excerpt": row["excerpt"],
        }
        for row in sample
    ]

    write_csv(
        DATA_OUT_DIR / "reddit_amostra_qualitativa.csv",
        sample_rows,
        [
            "unit_id",
            "type",
            "subreddit",
            "primary_architecture",
            "all_architectures",
            "score",
            "num_comments",
            "title",
            "excerpt",
        ],
    )

    write_csv(
        DATA_OUT_DIR / "reddit_codificacao_inicial.csv",
        coded_rows,
        [
            "unit_id",
            "type",
            "subreddit",
            "primary_architecture",
            "all_architectures",
            "score",
            "num_comments",
            "codes",
            "categories",
            "title",
            "excerpt",
        ],
    )

    categories_summary = [{"category": category, "count": count} for category, count in category_counter.most_common()]
    codes_summary = [{"code": code, "count": count} for code, count in code_counter.most_common()]
    arch_category_summary = [
        {"primary_architecture": arch, "category": category, "count": count}
        for (arch, category), count in sorted(arch_category_counter.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
    ]

    write_csv(DATA_OUT_DIR / "reddit_resumo_categorias.csv", categories_summary, ["category", "count"])
    write_csv(DATA_OUT_DIR / "reddit_resumo_codigos.csv", codes_summary, ["code", "count"])
    write_csv(
        DATA_OUT_DIR / "reddit_cruzamento_arquitetura_categoria.csv",
        arch_category_summary,
        ["primary_architecture", "category", "count"],
    )

    protocol = """# Analise qualitativa do Reddit

## Objetivo
Identificar temas recorrentes nas discussoes sobre arquiteturas em SwiftUI, complementando as analises quantitativas.

## Estrategia adotada
- Amostra inicial: 30 posts e 30 comentarios.
- Criterio de selecao: itens com arquitetura detectada e maior relevancia/engajamento.
- Diversificacao: tentativa de cobrir mais de uma arquitetura na amostra.
- Codificacao: primeira rodada com codigos tematicos inspirados em Grounded Theory.

## Categorias iniciais
- adequacao_tecnica
- beneficio_percebido
- custo_percebido
- adocao_e_aprendizado
- estrutura_arquitetural
- percepcao_geral

## Codigos iniciais
- aderencia_swiftui
- simplicidade
- complexidade_excessiva
- boilerplate
- gestao_estado
- escalabilidade
- manutenibilidade
- testabilidade
- aprendizado_curva
- organizacao_camadas
- avaliacao_geral_arquitetura

## Observacao metodologica
Esta e uma primeira rodada de codificacao, adequada para discussao com a orientacao e refinamento posterior.
"""
    (OUTPUTS_DIR / "protocolo_analise_qualitativa_reddit.md").write_text(protocol, encoding="utf-8")

    report = build_report(coded_rows, categories_summary, codes_summary)
    notes = build_speaking_notes()
    (OUTPUTS_DIR / "relatorio_analise_qualitativa_reddit.md").write_text(report, encoding="utf-8")
    (OUTPUTS_DIR / "roteiro_fala_analise_qualitativa_reddit.md").write_text(notes, encoding="utf-8")

    generated_plots = []
    category_plot_rows = [
        {"label": CATEGORY_LABELS.get(row["category"], row["category"]), "count": int(row["count"])}
        for row in categories_summary
    ]
    code_plot_rows = [
        {"label": CODE_LABELS.get(row["code"], row["code"]), "count": int(row["count"])}
        for row in codes_summary[:8]
    ]
    if save_bar_chart(
        category_plot_rows,
        "label",
        "count",
        "Categorias da analise qualitativa",
        "grafico_qualitativa_categorias.png",
        (8.5, 4.8),
    ):
        generated_plots.append("grafico_qualitativa_categorias.png")
    if save_bar_chart(
        code_plot_rows,
        "label",
        "count",
        "Codigos mais recorrentes",
        "grafico_qualitativa_codigos.png",
        (10, 5.2),
    ):
        generated_plots.append("grafico_qualitativa_codigos.png")
    if save_heatmap(arch_category_summary):
        generated_plots.append("grafico_qualitativa_heatmap_arquitetura_categoria.png")

    print("Dados processados em:", DATA_OUT_DIR)
    print("Saidas em:", OUTPUTS_DIR)
    if generated_plots:
        print("Graficos:", ", ".join(generated_plots))
    else:
        print("Graficos nao gerados: matplotlib indisponivel no ambiente.")


if __name__ == "__main__":
    main()
