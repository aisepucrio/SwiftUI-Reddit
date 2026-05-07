import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
ANALISE_DIR = BASE_DIR / "scripts" / "analise"

PARALLEL_STEPS = [
    ("Analise Reddit",             ANALISE_DIR / "analise_reddit.py"),

    ("Analise Stack Overflow",     ANALISE_DIR / "analise_stackoverflow.py"),
    ("Analise GitHub",             ANALISE_DIR / "analise_github.py"),
    ("Analise Forms",              ANALISE_DIR / "analise_forms.py"),
    ("Analise qualitativa Reddit", ANALISE_DIR / "analise_qualitativa_reddit.py"),
]

SEQUENTIAL_STEPS = [
    ("Comparacao entre fontes", ANALISE_DIR / "comparacao_fontes.py"),
    ("Nuvens de palavras", ANALISE_DIR / "gerar_nuvens_palavras.py"),
]


def run_step(label: str, script_path: Path) -> str:
    """Executa um script da pipeline e retorna sua saída padrão."""
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Falha em '{label}' (codigo {result.returncode}):\n{result.stderr}"
        )
    return result.stdout


def main() -> None:
    """Coordena a execução paralela e sequencial das análises do projeto."""
    all_steps = PARALLEL_STEPS + SEQUENTIAL_STEPS
    missing = [str(p) for _, p in all_steps if not p.exists()]
    if missing:
        print("ERRO: scripts nao encontrados:")
        for path in missing:
            print(f"  - {path}")
        raise SystemExit(1)

    print(f"Python em uso: {sys.executable}")
    print(f"\nRodando {len(PARALLEL_STEPS)} analises em paralelo...\n")

    failed = []

    with ThreadPoolExecutor(max_workers=len(PARALLEL_STEPS)) as executor:
        futures = {
            executor.submit(run_step, label, path): label
            for label, path in PARALLEL_STEPS
        }
        for future in as_completed(futures):
            label = futures[future]
            try:
                output = future.result()
                print(f"[OK] {label}")
                if output.strip():
                    for line in output.strip().splitlines():
                        print(f"     {line}")
            except RuntimeError as e:
                print(f"[ERRO] {label}")
                print(f"     {e}")
                failed.append(label)

    if failed:
        raise SystemExit(f"\nPipeline abortada. Falhas: {', '.join(failed)}")

    print(f"\nRodando etapas sequenciais...\n")
    for label, path in SEQUENTIAL_STEPS:
        try:
            output = run_step(label, path)
            print(f"[OK] {label}")
            if output.strip():
                for line in output.strip().splitlines():
                    print(f"     {line}")
        except RuntimeError as e:
            raise SystemExit(str(e))

    print("\nPipeline concluida com sucesso.")


if __name__ == "__main__":
    main()
