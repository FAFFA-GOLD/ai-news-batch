# ai_news_fetcher.py
import os
from datetime import datetime, timezone
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client


# =========================
# Supabase 接続設定
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================
# 収集したいフィード一覧
# =========================
FEEDS = [
    {
        "source": "OpenAI News",
        "url": "https://openai.com/news/rss.xml",
    },
    {
        "source": "Zenn LLM",
        "url": "https://zenn.dev/topics/llm/feed",
    },
    # 必要に応じて追加
]


# =========================
# ユーティリティ
# =========================
def parse_published(entry) -> Optional[datetime]:
    """RSS の published / updated から datetime を作る。"""
    struct = getattr(entry, "published_parsed", None) or getattr(
        entry, "updated_parsed", None
    )
    if struct:
        return datetime(*struct[:6], tzinfo=timezone.utc)
    return None


def fetch_article_content(url: str) -> Optional[str]:
    """
    記事ページの HTML を取得して、本文テキストだけを抜き出す。
    失敗した場合は None を返す。
    """
    try:
        resp = requests.get(
            url,
            timeout=10,
            headers={
                # シンプルな User-Agent（ブロックされにくくするため）
                "User-Agent": "Mozilla/5.0 (compatible; AI-News-Fetcher/1.0)",
            },
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] fetch_article_content failed: {url} ({e!r})")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # script / style は削除
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # よくある本文候補を優先
    candidates = []
    if soup.find("article"):
        candidates.append(soup.find("article"))
    if soup.find("main"):
        candidates.append(soup.find("main"))
    # class 名に article, content を含むものも候補に
    candidates += soup.select("[class*='article'], [class*='content']")

    for c in candidates:
        text = c.get_text(separator="\n", strip=True)
        if text and len(text) > 200:  # ある程度長さがあるものを本文とみなす
            return text

    # 候補がダメなら <body> 全体からテキストだけ抜く
    body = soup.body or soup
    text = body.get_text(separator="\n", strip=True)
    return text or None


def save_entry(feed_source: str, entry) -> None:
    """
    1件のエントリを articles テーブルに保存。
    同じ URL が既にある場合はスキップする。
    """
    url = getattr(entry, "link", None)
    title = getattr(entry, "title", None)

    if not url or not title:
        print(f"Skip entry without url/title from {feed_source}")
        return

    summary = getattr(entry, "summary", None)
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
        return

    # まずは本文を取りに行く（失敗したら summary を fallback に）
    content_raw = fetch_article_content(url) or summary

    row = {
        "source": feed_source,
        "url": url,
        "title": title,
        "summary": summary,
        "content_raw": content_raw,
        "published_at": published_at,
    }

    res = supabase.table("articles").insert(row).execute()
    print(f"Inserted: {feed_source} | {title[:60]} ... (id={res.data[0]['id']})")


def fetch_all() -> None:
    """登録された全フィードを巡回して Supabase に保存するメイン処理。"""
    for feed in FEEDS:
        source = feed["source"]
        url = feed["url"]

        print(f"=== Fetching: {source} ({url}) ===")
        d = feedparser.parse(url)
        entries = getattr(d, "entries", [])
        print(f" -> {len(entries)} entries")

        for entry in entries:
            try:
                save_entry(source, entry)
            except Exception as e:
                print(f"[ERROR] saving entry from {source}: {e!r}")


if __name__ == "__main__":
    fetch_all()
