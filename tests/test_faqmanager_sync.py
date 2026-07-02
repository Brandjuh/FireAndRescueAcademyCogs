import asyncio
import tempfile
from datetime import datetime
from pathlib import Path

import aiosqlite

from faqmanager.database import FAQDatabase
from faqmanager.helpshift_scraper import (
    HelpshiftScraper,
    auto_crawl_due,
    is_missionchief_usa_article,
    missionchief_usa_filter_reason,
)
from faqmanager.models import HelpshiftArticle


def make_article(article_id: int, title: str, body: str, section_name: str = "General"):
    return HelpshiftArticle(
        id=article_id,
        slug=f"article-{article_id}",
        url=f"https://xyrality.helpshift.com/hc/en/23-mission-chief/faq/{article_id}-article/",
        title=title,
        section_id=1,
        section_name=section_name,
        last_updated_text="today",
        last_seen_utc="2026-07-02T12:00:00Z",
        body_md=body,
        hash_body=HelpshiftArticle.compute_hash(body),
        lang="en",
    )


def test_missionchief_usa_filter_allows_generic_usa_article():
    assert is_missionchief_usa_article(
        "How do I build a fire station?",
        "This MissionChief article explains fire station construction.",
        "Buildings",
    )


def test_missionchief_usa_filter_rejects_non_usa_versions():
    assert missionchief_usa_filter_reason("HART Base (UK version)", "Build a HART base.")
    assert missionchief_usa_filter_reason("SES building", "Australian SES station requirements.")
    assert not is_missionchief_usa_article(
        "Police station (AU version)",
        "Australian police station details.",
        "Buildings",
    )


def test_auto_crawl_due_uses_last_completed_timestamp():
    now = datetime(2026, 7, 2, 12, 0, 0)

    assert auto_crawl_due(None, 24, now_utc=now)
    assert auto_crawl_due("not-a-date", 24, now_utc=now)
    assert auto_crawl_due("2026-07-01T11:59:59Z", 24, now_utc=now)
    assert not auto_crawl_due("2026-07-01T12:30:00Z", 24, now_utc=now)


def test_helpshift_search_filters_existing_non_usa_database_rows():
    async def run_test():
        with tempfile.TemporaryDirectory() as temp_dir:
            database = FAQDatabase(Path(temp_dir) / "faq.db")
            await database.initialize()

            usa = make_article(
                100,
                "Fire station requirements",
                "MissionChief fire station requirements for the USA game.",
            )
            uk = make_article(
                200,
                "HART Base (UK version)",
                "HART base requirements for the UK version.",
            )

            await database.upsert_article(usa)
            await database.upsert_article(uk)

            scraper = HelpshiftScraper()
            scraper.set_database(database)

            results = await scraper.search_all_articles("requirements", max_articles=10)
            assert [article.title for article in results] == ["Fire station requirements"]

    asyncio.run(run_test())


def test_article_version_snapshots_keep_previous_body():
    async def run_test():
        with tempfile.TemporaryDirectory() as temp_dir:
            database = FAQDatabase(Path(temp_dir) / "faq.db")
            await database.initialize()

            article = make_article(300, "Mission list", "Original body text.")
            status, version_saved = await database.upsert_article(article)
            assert status == "new"
            assert not version_saved

            article.body_md = "Updated body text."
            article.hash_body = HelpshiftArticle.compute_hash(article.body_md)
            status, version_saved = await database.upsert_article(article)
            assert status == "updated"
            assert version_saved

            async with aiosqlite.connect(database.db_path) as db:
                cursor = await db.execute(
                    "SELECT body_md FROM article_versions WHERE article_id = ?",
                    (300,),
                )
                row = await cursor.fetchone()

            assert row[0] == "Original body text."

    asyncio.run(run_test())
