from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_RAW_FORMS_DIR = BASE_DIR / "data" / "raw" / "forms"
DATA_PROCESSED_FORMS_DIR = BASE_DIR / "data" / "processed" / "forms"
OUTPUTS_FORMS_DIR = BASE_DIR / "outputs" / "forms"

ARQ_FORMS = DATA_RAW_FORMS_DIR / "PesquisaArqiOSForms.csv"

COL_TIMESTAMP = "Carimbo de data/hora"
COL_CONSENT = (
    "O Termo de Consentimento Livre e Esclarecido (TCLE) visa assegurar seus direitos "
    "como participante e está disponível nesse link. Por favor, leia com atenção e calma, "
    "buscando entender completamente a proposta da pesquisa. Não haverá qualquer tipo de "
    "penalização ou prejuízo se você não quiser participar ou retirar sua autorização em qualquer momento.\n\n\n"
    "Após ter recebido esclarecimentos sobre a natureza da pesquisa, seus objetivos e métodos, "
    "eu declaro que sou maior de 18 anos e aceito participar por meio do formulário de participação, "
    "conforme as disposições da Lei Geral de Proteção de Dados (Lei nº 13.709/2018).\n\n"
    "Em caso de resposta negativa, o formulário será encerrado, garantindo assim o respeito à autonomia "
    "e privacidade dos participantes, conforme estabelecido pela legislação vigente."
)
COL_IOS_EXPERIENCE = "Nível de experiência em desenvolvimento iOS:"
COL_SWIFTUI_EXPERIENCE = "Tempo de experiência com SwiftUI:"
COL_ARCH_USED = "Quais arquiteturas você já utilizou em projetos SwiftUI?"
COL_ARCH_MAIN = "Qual delas você utiliza com mais frequência atualmente?"
COL_ARCH_REASON = "Principal motivo da escolha dessa arquitetura:"
COL_LAYER_ORG = "Como você organiza suas camadas (View, ViewModel, Model, etc.)?"
COL_LAYER_ORG_JUST = "Justifique a escolha acima"
COL_EXTRA_LAYERS = "Você adiciona camadas extras como UseCase, Repository ou Coordinator?"
COL_EXTRA_LAYERS_JUST = "Justifique a escolha acima"
COL_MVVM_PURE = "O uso de @State, @StateObject, @ObservedObject e @EnvironmentObject compromete o MVVM “puro”?"
COL_MVVM_PURE_JUST = "Justifique a escolha acima"
COL_VIPER_CHALLENGES = "Principais desafios ao aplicar arquiteturas complexas (VIPER) em SwiftUI:"
COL_VIPER_CHALLENGES_JUST = "Justifique a escolha acima"
COL_BEST_ARCH = "Qual arquitetura você acredita ser mais adequada ao ecossistema SwiftUI?"
COL_BEST_ARCH_JUST = "Justifique a escolha acima"
COL_FINAL_COMMENT = (
    "Obrigado pela atenção e dedicação ao responder esse formulário! Suas respostas serão muito "
    "construtivas para o desenvolvimento do trabalho.\n\nQualquer dúvida ou sugestão fique a vontade para mencionar abaixo."
)

ARCHITECTURE_NORMALIZATION = {
    "mvvm": "MVVM",
    "mvvm-c": "MVVM-C",
    "mvvm c": "MVVM-C",
    "mvc": "MVC",
    "mvp": "MVP",
    "mv": "MV",
    "viper": "VIPER",
    "tca": "TCA",
    "clean architecture": "Clean Architecture",
    "coordinator": "Coordinator",
    "mvi": "MVI",
    "redux": "Redux",
    "ribs": "RIBs",
}


def carregar_dados() -> pd.DataFrame:
    """Carrega respostas do Forms e mantém apenas participantes com TCLE aceito."""
    df = pd.read_csv(ARQ_FORMS)
    df[COL_TIMESTAMP] = pd.to_datetime(df[COL_TIMESTAMP], errors="coerce")
    df = df[df[COL_CONSENT].fillna("").str.strip().str.lower() == "sim"].copy()
    return df


def normalizar_arquitetura(value: str) -> str:
    """Padroniza nomes de arquiteturas para rótulos consistentes."""
    if not isinstance(value, str):
        return ""
    key = value.strip().lower()
    return ARCHITECTURE_NORMALIZATION.get(key, value.strip())


def split_multiple_architectures(value: str) -> list[str]:
    """Divide respostas com múltiplas arquiteturas e normaliza cada item."""
    if not isinstance(value, str) or not value.strip():
        return []
    parts = [normalizar_arquitetura(item) for item in value.split(";")]
    return [item for item in parts if item]


def salvar_csv(df: pd.DataFrame, filename: str) -> None:
    """Salva um DataFrame no diretório de dados processados do Forms."""
    out = DATA_PROCESSED_FORMS_DIR / filename
    df.to_csv(out, index=False)
    print(f"Salvo: {out}")


def salvar_grafico_barra(df: pd.DataFrame, x_col: str, y_col: str, title: str, filename: str) -> None:
    """Gera e salva um gráfico de barras simples para uma frequência tabular."""
    plt.figure(figsize=(10, 5))
    plt.bar(df[x_col], df[y_col])
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out = OUTPUTS_FORMS_DIR / filename
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"Gráfico salvo: {out}")


def frequencia_coluna(df: pd.DataFrame, column: str, output_name: str, chart_title: str, chart_name: str) -> None:
    """Calcula frequência de uma coluna categórica e salva CSV e gráfico."""
    freq = (
        df[column]
        .fillna("Sem resposta")
        .astype(str)
        .str.strip()
        .replace("", "Sem resposta")
        .value_counts()
        .rename_axis("categoria")
        .reset_index(name="quantidade")
    )
    salvar_csv(freq, output_name)
    salvar_grafico_barra(freq, "categoria", "quantidade", chart_title, chart_name)


def perfil_respondentes(df: pd.DataFrame) -> None:
    """Gera frequências e gráficos do perfil de experiência dos respondentes."""
    frequencia_coluna(
        df,
        COL_IOS_EXPERIENCE,
        "perfil_experiencia_ios.csv",
        "Nível de experiência em desenvolvimento iOS",
        "grafico_perfil_experiencia_ios.png",
    )
    frequencia_coluna(
        df,
        COL_SWIFTUI_EXPERIENCE,
        "perfil_experiencia_swiftui.csv",
        "Tempo de experiência com SwiftUI",
        "grafico_perfil_experiencia_swiftui.png",
    )


def arquiteturas_utilizadas(df: pd.DataFrame) -> None:
    """Processa arquiteturas usadas, principal e considerada mais adequada."""
    used = df[[COL_ARCH_USED]].copy()
    used["architecture"] = used[COL_ARCH_USED].apply(split_multiple_architectures)
    used = used.explode("architecture")
    used = used[used["architecture"].notna() & (used["architecture"] != "")]

    freq_used = (
        used["architecture"]
        .value_counts()
        .rename_axis("architecture")
        .reset_index(name="quantidade")
    )
    salvar_csv(freq_used, "arquiteturas_utilizadas_forms.csv")
    salvar_grafico_barra(
        freq_used,
        "architecture",
        "quantidade",
        "Arquiteturas já utilizadas em projetos SwiftUI",
        "grafico_arquiteturas_utilizadas_forms.png",
    )

    main_arch = df[[COL_ARCH_MAIN]].copy()
    main_arch["architecture"] = main_arch[COL_ARCH_MAIN].apply(normalizar_arquitetura)
    freq_main = (
        main_arch["architecture"]
        .replace("", "Sem resposta")
        .value_counts()
        .rename_axis("architecture")
        .reset_index(name="quantidade")
    )
    salvar_csv(freq_main, "arquitetura_principal_forms.csv")
    salvar_grafico_barra(
        freq_main,
        "architecture",
        "quantidade",
        "Arquitetura mais utilizada atualmente",
        "grafico_arquitetura_principal_forms.png",
    )

    best_arch = df[[COL_BEST_ARCH]].copy()
    best_arch["architecture"] = best_arch[COL_BEST_ARCH].apply(normalizar_arquitetura)
    freq_best = (
        best_arch["architecture"]
        .replace("", "Sem resposta")
        .value_counts()
        .rename_axis("architecture")
        .reset_index(name="quantidade")
    )
    salvar_csv(freq_best, "arquitetura_mais_adequada_forms.csv")
    salvar_grafico_barra(
        freq_best,
        "architecture",
        "quantidade",
        "Arquitetura considerada mais adequada ao ecossistema SwiftUI",
        "grafico_arquitetura_mais_adequada_forms.png",
    )


def perguntas_fechadas(df: pd.DataFrame) -> None:
    """Gera frequências e gráficos para perguntas fechadas do formulário."""
    frequencia_coluna(
        df,
        COL_LAYER_ORG,
        "organizacao_camadas_forms.csv",
        "Como os respondentes organizam as camadas",
        "grafico_organizacao_camadas_forms.png",
    )
    frequencia_coluna(
        df,
        COL_EXTRA_LAYERS,
        "camadas_extras_forms.csv",
        "Uso de camadas extras como UseCase, Repository ou Coordinator",
        "grafico_camadas_extras_forms.png",
    )
    frequencia_coluna(
        df,
        COL_MVVM_PURE,
        "mvvm_puro_forms.csv",
        "Percepção sobre comprometimento do MVVM puro",
        "grafico_mvvm_puro_forms.png",
    )
    frequencia_coluna(
        df,
        COL_VIPER_CHALLENGES,
        "desafios_viper_forms.csv",
        "Principais desafios ao aplicar arquiteturas complexas em SwiftUI",
        "grafico_desafios_viper_forms.png",
    )
    frequencia_coluna(
        df,
        COL_ARCH_REASON,
        "motivo_escolha_arquitetura_forms.csv",
        "Principal motivo da escolha da arquitetura",
        "grafico_motivo_escolha_arquitetura_forms.png",
    )


def cruzamentos(df: pd.DataFrame) -> None:
    """Gera tabelas cruzadas entre experiência, camadas e arquitetura principal."""
    cross_ios_main = pd.crosstab(
        df[COL_IOS_EXPERIENCE].fillna("Sem resposta"),
        df[COL_ARCH_MAIN].apply(normalizar_arquitetura).replace("", "Sem resposta"),
    )
    salvar_csv(
        cross_ios_main.reset_index(),
        "cruzamento_experiencia_ios_x_arquitetura_principal.csv",
    )

    cross_swiftui_main = pd.crosstab(
        df[COL_SWIFTUI_EXPERIENCE].fillna("Sem resposta"),
        df[COL_ARCH_MAIN].apply(normalizar_arquitetura).replace("", "Sem resposta"),
    )
    salvar_csv(
        cross_swiftui_main.reset_index(),
        "cruzamento_experiencia_swiftui_x_arquitetura_principal.csv",
    )

    cross_extra_main = pd.crosstab(
        df[COL_EXTRA_LAYERS].fillna("Sem resposta"),
        df[COL_ARCH_MAIN].apply(normalizar_arquitetura).replace("", "Sem resposta"),
    )
    salvar_csv(
        cross_extra_main.reset_index(),
        "cruzamento_camadas_extras_x_arquitetura_principal.csv",
    )

    cross_mvvm_main = pd.crosstab(
        df[COL_MVVM_PURE].fillna("Sem resposta"),
        df[COL_ARCH_MAIN].apply(normalizar_arquitetura).replace("", "Sem resposta"),
    )
    salvar_csv(
        cross_mvvm_main.reset_index(),
        "cruzamento_mvvm_puro_x_arquitetura_principal.csv",
    )


def respostas_abertas(df: pd.DataFrame) -> None:
    """Exporta respostas abertas relevantes para análise qualitativa posterior."""
    open_answers = df[
        [
            COL_ARCH_REASON,
            COL_LAYER_ORG_JUST,
            COL_EXTRA_LAYERS_JUST,
            COL_MVVM_PURE_JUST,
            COL_VIPER_CHALLENGES_JUST,
            COL_BEST_ARCH_JUST,
            COL_FINAL_COMMENT,
        ]
    ].copy()
    salvar_csv(open_answers, "respostas_abertas_forms.csv")


def main() -> None:
    """Executa o fluxo completo de análise das respostas do Forms."""
    DATA_PROCESSED_FORMS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_FORMS_DIR.mkdir(parents=True, exist_ok=True)

    if not ARQ_FORMS.exists():
        print(f"ERRO: arquivo não encontrado: {ARQ_FORMS}")
        return

    df = carregar_dados()
    print(f"Respostas válidas (TCLE=Sim): {len(df)}")

    perfil_respondentes(df)
    arquiteturas_utilizadas(df)
    perguntas_fechadas(df)
    cruzamentos(df)
    respostas_abertas(df)


if __name__ == "__main__":
    main()
