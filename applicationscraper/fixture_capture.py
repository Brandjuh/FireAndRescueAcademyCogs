import re

from bs4 import BeautifulSoup, Comment

ALLOWED_TEXT_PATTERNS = (
    re.compile(r"^Fixture (?:Applicant|application message|date|text)$"),
    re.compile(r"\b(?:accept|accepted|approved|reject|rejected|declined|pending)\b", re.I),
    re.compile(r"\b\d[\d,]*\s*(?:credits|coins|buildings|stations)\b", re.I),
    re.compile(r"\b(?:sign out|logout)\b", re.I),
)


def sanitize_applications_fixture(html: str) -> str:
    """Create a review fixture while removing likely personal and secret data."""
    soup = BeautifulSoup(html, "html.parser")

    for node in soup.find_all(["script", "style", "noscript"]):
        node.decompose()

    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()

    for tag in soup.find_all(True):
        allowed_attributes = {}

        if tag.get("class"):
            allowed_attributes["class"] = tag.get("class")

        href = tag.get("href")
        if href:
            if re.search(r"/(?:profile|users)/\d+", href):
                route = "profile" if "/profile/" in href else "users"
                allowed_attributes["href"] = f"/{route}/100001"
            elif href == "/users/sign_out":
                allowed_attributes["href"] = href
            else:
                allowed_attributes["href"] = "#"

        if tag.name == "time" and tag.get("datetime"):
            allowed_attributes["datetime"] = "2026-01-01T00:00:00"

        tag.attrs = allowed_attributes

    for link in soup.find_all("a", href=re.compile(r"^/(?:profile|users)/100001$")):
        link.string = "Fixture Applicant"

    for paragraph in soup.find_all("p"):
        paragraph.string = "Fixture application message"

    for time_tag in soup.find_all("time"):
        time_tag.string = "Fixture date"

    for text_node in list(soup.find_all(string=True)):
        if not text_node.strip():
            continue
        if any(pattern.search(text_node) for pattern in ALLOWED_TEXT_PATTERNS):
            continue
        text_node.replace_with("Fixture text")

    return str(soup)


def inspect_applications_page(html: str) -> dict[str, int]:
    """Count the structures currently used by ApplicationsScraper."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="table")
    table_rows = 0
    if table:
        tbody = table.find("tbody")
        table_rows = len(tbody.find_all("tr") if tbody else table.find_all("tr"))

    cards = soup.find_all(
        "div",
        class_=lambda value: value and ("card" in value.lower() or "panel" in value.lower()),
    )
    list_items = soup.find_all(
        "li",
        class_=lambda value: value and "application" in value.lower(),
    )
    profile_links = soup.find_all("a", href=lambda value: value and "/profile/" in value)

    return {
        "table_rows": table_rows,
        "cards_or_panels": len(cards),
        "application_list_items": len(list_items),
        "profile_links": len(profile_links),
    }
