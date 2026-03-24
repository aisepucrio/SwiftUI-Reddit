import os
import csv
import re
from datetime import datetime
from typing import List, Dict, Any, Set

import praw
from dotenv import load_dotenv

CORE_ARCH_KEYWORDS = ["MVVM", "MVP", "VIPER", "TCA", "MV", "MVVM-C"]

SWIFTUI_KEYWORDS = [
    "SwiftUI",
    "@State",
    "@Binding",
    "@Environment",
    "@ObservedObject",
    "@EnvironmentObject",
    "StateObject",
    "ObservableObject",
    "NavigationStack",
    "Combine",
    "async await",
    "async/await",
]

SEARCH_QUERIES = [
    "SwiftUI MVVM",
    "SwiftUI MVP",
    "SwiftUI VIPER",
    "SwiftUI TCA",
    "MVVM architecture SwiftUI",
    "MVP architecture SwiftUI",
    "VIPER architecture SwiftUI",
    "TCA architecture SwiftUI",
    "Clean architecture SwiftUI",
    "SwiftUI architecture",
]

SWIFTUI_ONLY_SUBREDDITS = ["SwiftUI"]
MIXED_SUBREDDITS = ["iOSProgramming", "swift"]


def build_patterns(keywords: List[str]) -> Dict[str, re.Pattern]:
    return {
        kw: re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
        for kw in keywords
    }


ARCH_PATTERNS = build_patterns(CORE_ARCH_KEYWORDS)
SWIFTUI_PATTERNS = build_patterns(SWIFTUI_KEYWORDS)


def create_reddit_client() -> praw.Reddit:
    load_dotenv()

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")

    if not all([client_id, client_secret, user_agent]):
        raise RuntimeError(
            "Faltam variáveis de ambiente. "
            "Verifique REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET "
            "e REDDIT_USER_AGENT no seu .env."
        )

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )
    reddit.read_only = True
    return reddit

def find_keywords(text: str, patterns: Dict[str, re.Pattern]) -> List[str]:
    if not text:
        return []
    found = []
    for kw, pattern in patterns.items():
        if pattern.search(text):
            found.append(kw)
    return found

def collect_posts_for_subreddit(
    reddit: praw.Reddit,
    subreddit_name: str,
    time_filter: str = "all",
    limit_per_query: int = 300,
) -> Dict[str, Dict[str, Any]]:
    """
    Faz várias buscas (SEARCH_QUERIES) em um subreddit
    e retorna um dict {id: dados_do_post}, já filtrando por SwiftUI+arquitetura.
    """
    subreddit = reddit.subreddit(subreddit_name)
    collected: Dict[str, Dict[str, Any]] = {}

    for query in SEARCH_QUERIES:
        print(f"[{subreddit_name}] Query: {query!r}")
        for submission in subreddit.search(
            query=query,
            sort="new",
            time_filter=time_filter,
            limit=limit_per_query,
        ):
            if submission.id in collected:
                continue  # evita duplicatas

            full_text = f"{submission.title}\n{submission.selftext}"

            arch_hits = find_keywords(full_text, ARCH_PATTERNS)
            if not arch_hits:
                continue

            if subreddit_name in SWIFTUI_ONLY_SUBREDDITS:
                swiftui_hits = ["SwiftUI (contexto do subreddit)"]
            else:
                swiftui_hits = find_keywords(full_text, SWIFTUI_PATTERNS)
                if not swiftui_hits:
                    continue

            data = {
                "subreddit": subreddit_name,
                "id": submission.id,
                "title": submission.title,
                "matched_arch_keywords": ",".join(sorted(set(arch_hits))),
                "matched_swiftui_keywords": ",".join(sorted(set(swiftui_hits))),
                "score": submission.score,
                "num_comments": submission.num_comments,
                "created_utc": submission.created_utc,
                "created_iso": datetime.utcfromtimestamp(
                    submission.created_utc
                ).isoformat(),
                "author": str(submission.author) if submission.author else None,
                "url": submission.url,
                "permalink": f"https://www.reddit.com{submission.permalink}",
                "selftext": submission.selftext,
            }
            collected[submission.id] = data

    print(f"[{subreddit_name}] Total de posts coletados (únicos): {len(collected)}")
    return collected


def collect_all_posts(reddit: praw.Reddit) -> List[Dict[str, Any]]:
    posts_by_id: Dict[str, Dict[str, Any]] = {}

    for sub in SWIFTUI_ONLY_SUBREDDITS:
        posts = collect_posts_for_subreddit(reddit, sub)
        posts_by_id.update(posts)

    for sub in MIXED_SUBREDDITS:
        posts = collect_posts_for_subreddit(reddit, sub)
        posts_by_id.update(posts)

    print(f"TOTAL posts únicos (todos subreddits): {len(posts_by_id)}")
    return list(posts_by_id.values())

def collect_comments_for_posts(
    reddit: praw.Reddit,
    posts: List[Dict[str, Any]],
    max_comments_per_post: int = 400,
) -> List[Dict[str, Any]]:
    all_comments: List[Dict[str, Any]] = []

    for post in posts:
        sub_id = post["id"]
        subreddit_name = post["subreddit"]
        print(f"Coletando comentários de {subreddit_name} / {sub_id}...")

        submission = reddit.submission(id=sub_id)
        submission.comments.replace_more(limit=None)

        count = 0

        for comment in submission.comments.list():
            if count >= max_comments_per_post:
                break

            text = comment.body
            arch_hits = find_keywords(text, ARCH_PATTERNS)
            if not arch_hits:
                continue

            swiftui_hits = find_keywords(text, SWIFTUI_PATTERNS)
            if subreddit_name in SWIFTUI_ONLY_SUBREDDITS and not swiftui_hits:
                swiftui_hits = ["SwiftUI (contexto do subreddit)"]
            elif subreddit_name not in SWIFTUI_ONLY_SUBREDDITS and not swiftui_hits:
                continue

            all_comments.append(
                {
                    "subreddit": subreddit_name,
                    "submission_id": sub_id,
                    "comment_id": comment.id,
                    "parent_id": comment.parent_id,
                    "matched_arch_keywords": ",".join(sorted(set(arch_hits))),
                    "matched_swiftui_keywords": ",".join(sorted(set(swiftui_hits))),
                    "author": str(comment.author) if comment.author else None,
                    "body": comment.body,
                    "score": comment.score,
                    "created_utc": comment.created_utc,
                    "created_iso": datetime.utcfromtimestamp(
                        comment.created_utc
                    ).isoformat(),
                    "comment_permalink": (
                        f"https://www.reddit.com{submission.permalink}{comment.id}"
                    ),
                }
            )
            count += 1

        print(f"Comentários coletados (após filtro): {count}")

    print(f"TOTAL de comentários coletados: {len(all_comments)}")
    return all_comments

def save_dicts_to_csv(rows: List[Dict[str, Any]], filename: str) -> None:
    if not rows:
        print(f"Nenhuma linha para salvar em {filename}.")
        return
    fieldnames = list(rows[0].keys())
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Arquivo salvo: {filename} (linhas: {len(rows)})")

def main():
    reddit = create_reddit_client()

    posts = collect_all_posts(reddit)
    save_dicts_to_csv(posts, "reddit_swiftui_arch_posts_big.csv")

    comments = collect_comments_for_posts(
        reddit,
        posts,
        max_comments_per_post=500,
    )
    save_dicts_to_csv(comments, "reddit_swiftui_arch_comments_big.csv")


if __name__ == "__main__":
    main()
