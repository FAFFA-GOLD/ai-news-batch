# ai_news_fetcher.py
import os
from datetime import datetime, timezone
from typing import Optional

import feedparser
from supabase import create_client, Client


# =========================
# Supabase 接続設定
# =========================
# Render 側の Environment で
#   SUPABASE_URL
#   SUPABASE_SERVICE_ROLE_KEY
# が設定されている前提
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================
# 収集したいフィード一覧
# ここにどんどん追加していけばOK
# =========================
FEEDS = [
    # 公式・企業系
    {
        "source": "OpenAI News",
        "url": "https://openai.com/news/rss.xml",
    },
    {
        "source": "Google Blog (全体)",
        "url": "https://blog.google/feed/",
    },
    {
        "source": "DeepMind Blog",
        "url": "https://deepmind.google/discover/blog/feed",
    },
    {
        "source": "Google Research Blog",
        "url": "https://research.google/blog/feed/",
    },
    # 日本語コミュニティ例
    {
        "source": "Zenn LLM",
        "url": "https://zenn.dev/topics/llm/feed",
    },
    # ここに個人ブログ・技術ブログなどを追加していく想定:
    # {"source": "Example AI Blog", "url": "https://example.com/feed"},
    #
    # ※ X(Twitter) や一部のSNSは公式RSSがないので、
    #   後で別の方法（API / スクレイピング）で拡張していくイメージです。
]


# =========================
# ユーティリティ
# =========================
def parse_published(entry) -> Optional[datetime]:
    """
    RSS の published / updated から datetime を作る。
    取れなければ None を返す。
    """
    struct = getattr(entry, "published_parsed", None) or getattr(
        entry, "updated_parsed", None
    )
    if struct:
        # struct: time.struct_time -> year, month, day, hour, minute, second
        return datetime(*struct[:6], tzinfo=timezone.utc)
    return None


def save_entry(feed_source: str, entry) -> None:
    """
    1件のエントリを articles テーブルに保存。
    同じ URL が既にある場合はスキップする。
    """
    url = getattr(entry, "link", None)
    title = getattr(entry, "title", None)

    if not url or not title:
        # URL またはタイトルが無いものはスキップ
        print(f"Skip entry without url/title from {feed_source}")
        return

    summary = getattr(entry, "summary", None)
    content_raw = summary  # ひとまず summary を生テキストとして入れておく

    published_at = parse_published(entry)

    # 既存 URL チェック（重複防止）
    existing = (
        supabase.table("articles")
        .select("id")
        .eq("url", url)
        .limit(1)
        .execute()
    )
    if existing.data:
        # 既に登録済み
        return

    row = {
        "source": feed_source,
        "url": url,
        "title": title,
        "summary": summary,
        "content_raw": content_raw,
        "published_at": published_at,  # None でもOK
        # category / tags / importance などは後で LLM で埋める想定
    }

    # INSERT 実行
    res = supabase.table("articles").insert(row).execute()
    # 簡易ログ
    print(f"Inserted: {feed_source} | {title[:60]} ... (id={res.data[0]['id']})")


def fetch_all() -> None:
    """
    登録された全フィードを巡回して Supabase に保存するメイン処理。
    """
    for feed in FEEDS:
        source = feed["source"]
        url = feed["url"]

        print(f"=== Fetching: {source} ({url}) ===")
        d = feedparser.parse(url)

        # エラーなどで entries が空の場合もある
        entries = getattr(d, "entries", [])
        print(f" -> {len(entries)} entries")

        for entry in entries:
            try:
                save_entry(source, entry)
            except Exception as e:
                # 1件失敗しても全体は止めない
                print(f"[ERROR] saving entry from {source}: {e!r}")


if __name__ == "__main__":
    fetch_all()
