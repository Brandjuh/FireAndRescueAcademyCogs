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
