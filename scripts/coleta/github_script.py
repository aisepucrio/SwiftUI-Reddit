import csv
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_RAW_GITHUB_DIR = BASE_DIR / "data" / "raw" / "github"

BASE_URL = "https://api.github.com"
USER_AGENT = "tcc-swiftui-architecture-research/1.0"
MAX_PAGES_PER_QUERY = 10 

ARCHITECTURE_QUERIES = {
    "MVVM":   "MVVM",
    "MVVM-C": "MVVM-C",
    "MVC":    "MVC",
    "MVP":    "MVP",
    "VIPER":  "VIPER",
    "TCA":    "composable architecture",
    "MVI":    "MVI",
    "Redux":  "Redux",
    "RIBs":   "RIBs",
}


class RateLimiter:
    """Token bucket compartilhado entre threads."""

    def __init__(self, calls_per_minute: int) -> None:
        """Configura o intervalo mínimo entre chamadas à API."""
        self._interval = 60.0 / calls_per_minute
        self._lock = threading.Lock()
        self._last_call = 0.0

    def acquire(self) -> None:
        """Bloqueia a thread até que uma nova chamada possa ser feita."""
        with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()


def load_token() -> Optional[str]:
    """Carrega o token do GitHub a partir do arquivo .env, quando disponível."""
    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("AVISO: GITHUB_TOKEN não encontrado no .env. Limite: 10 req/min.")
    return token


def fetch_json(url: str, token: Optional[str]) -> Dict[str, Any]:
    """Executa uma requisição GET à API do GitHub e retorna o JSON decodificado."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _repo_to_row(architecture: str, repo: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Converte um payload de repositório do GitHub para uma linha tabular."""
    return {
        "architecture": architecture,
        "source": source,
        "repo_id": repo.get("id", ""),
        "full_name": repo.get("full_name", ""),
        "name": repo.get("name", ""),
        "description": repo.get("description", ""),
        "topics": ",".join(repo.get("topics", [])),
        "stars": repo.get("stargazers_count", 0),
        "forks": repo.get("forks_count", 0),
        "watchers": repo.get("watchers_count", 0),
        "open_issues": repo.get("open_issues_count", 0),
        "language": repo.get("language", ""),
        "created_iso": repo.get("created_at", ""),
        "updated_iso": repo.get("updated_at", ""),
        "url": repo.get("html_url", ""),
    }


def search_repos(
    architecture: str,
    keyword: str,
    token: Optional[str],
    limiter: RateLimiter,
) -> Dict[int, Dict[str, Any]]:
    """Busca repositórios SwiftUI por nome, descrição e tópicos."""
    results: Dict[int, Dict[str, Any]] = {}
    query = f"swiftui {keyword} language:swift"

    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        params = urllib.parse.urlencode({
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": 100,
            "page": page,
        })
        limiter.acquire()
        payload = fetch_json(f"{BASE_URL}/search/repositories?{params}", token)
        items = payload.get("items", [])

        if not items:
            break

        for repo in items:
            rid = repo.get("id")
            if rid and rid not in results:
                results[rid] = _repo_to_row(architecture, repo, "repo_search")

        if len(items) < 100:
            break

    print(f"  [{architecture}] repo_search: {len(results)} repos")
    return results


def search_readme(
    architecture: str,
    keyword: str,
    token: Optional[str],
    limiter: RateLimiter,
) -> Dict[int, Dict[str, Any]]:
    """Busca a palavra-chave em arquivos README de repositórios Swift."""
    results: Dict[int, Dict[str, Any]] = {}
    query = f"{keyword} language:swift filename:README"

    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        params = urllib.parse.urlencode({
            "q": query,
            "per_page": 100,
            "page": page,
        })
        limiter.acquire()
        payload = fetch_json(f"{BASE_URL}/search/code?{params}", token)
        items = payload.get("items", [])

        if not items:
            break

        for item in items:
            repo = item.get("repository", {})
            rid = repo.get("id")
            if rid and rid not in results:
                results[rid] = _repo_to_row(architecture, repo, "readme_search")

        if len(items) < 100:
            break

    print(f"  [{architecture}] readme_search: {len(results)} repos")
    return results


def collect_architecture(
    architecture: str,
    keyword: str,
    token: Optional[str],
    limiter: RateLimiter,
) -> List[Dict[str, Any]]:
    """Coleta repositórios de uma arquitetura e consolida resultados duplicados."""
    repo_results = search_repos(architecture, keyword, token, limiter)
    readme_results = search_readme(architecture, keyword, token, limiter)

    # Marca repos encontrados em ambas as fontes
    merged: Dict[int, Dict[str, Any]] = {}
    for rid, row in repo_results.items():
        merged[rid] = row
    for rid, row in readme_results.items():
        if rid in merged:
            merged[rid]["source"] = "both"
        else:
            merged[rid] = row

    return list(merged.values())


def save_dicts_to_csv(rows: List[Dict[str, Any]], filename: Path) -> None:
    """Salva uma lista de dicionários em CSV usando as chaves da primeira linha."""
    if not rows:
        print(f"Nenhuma linha para salvar em {filename}.")
        return
    fieldnames = list(rows[0].keys())
    with filename.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Arquivo salvo: {filename} (linhas: {len(rows)})")


def main() -> None:
    """Executa a coleta paralela de repositórios SwiftUI no GitHub."""
    DATA_RAW_GITHUB_DIR.mkdir(parents=True, exist_ok=True)

    token = load_token()
    calls_per_minute = 28 if token else 9
    limiter = RateLimiter(calls_per_minute)

    all_repos: List[Dict[str, Any]] = []
    print(f"Iniciando coleta paralela ({len(ARCHITECTURE_QUERIES)} arquiteturas)...\n")

    with ThreadPoolExecutor(max_workers=len(ARCHITECTURE_QUERIES)) as executor:
        futures = {
            executor.submit(collect_architecture, arch, keyword, token, limiter): arch
            for arch, keyword in ARCHITECTURE_QUERIES.items()
        }
        for future in as_completed(futures):
            arch = futures[future]
            repos = future.result()
            print(f"[{arch}] concluído — {len(repos)} repos únicos")
            all_repos.extend(repos)

    save_dicts_to_csv(
        all_repos,
        DATA_RAW_GITHUB_DIR / "github_swiftui_repos.csv",
    )
    print(f"\nTOTAL de repos coletados: {len(all_repos)}")


if __name__ == "__main__":
    main()
