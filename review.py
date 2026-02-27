#!/usr/bin/env python3
"""
Fetch product reviews from a product URL and print normalized JSON.

Usage:
    python pricespy_lookup.py "https://example.com/product-page" --max-reviews 0
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, FeatureNotFound


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REVIEW_ATTR_RE = re.compile(
    r"review|rating|feedback|testimonial|customer-review", re.IGNORECASE
)
REVIEW_LINK_RE = re.compile(
    r"review|rating|feedback|customer-reviews?|opinions?|/page/|[?&](page|p|pg|start|offset)=",
    re.IGNORECASE,
)
NEXT_TEXT_RE = re.compile(r"\b(next|more|older|load more)\b", re.IGNORECASE)
ALL_REVIEWS_TEXT_RE = re.compile(
    r"\b(all reviews|see all reviews|read all reviews|customer reviews)\b",
    re.IGNORECASE,
)
AMAZON_HOST_RE = re.compile(r"(^|\.)amazon\.[a-z.]+$", re.IGNORECASE)
AMAZON_ASIN_RE = re.compile(r"/(?:dp|gp/product|gp/aw/d)/([A-Z0-9]{10})(?:[/?]|$)", re.IGNORECASE)
HTML_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_THREAD_LOCAL = threading.local()


@dataclass
class Review:
    title: Optional[str]
    rating: Optional[float]
    author: Optional[str]
    date: Optional[str]
    content: str
    source_url: str


def normalize_netloc(netloc: str) -> str:
    return netloc.lower().replace("www.", "")


def is_amazon_url(url: str) -> bool:
    host = normalize_netloc(urlparse(url).netloc)
    return bool(AMAZON_HOST_RE.search(host))


def extract_amazon_asin(url: str) -> Optional[str]:
    path = urlparse(url).path or ""
    match = AMAZON_ASIN_RE.search(path)
    if not match:
        return None
    return match.group(1).upper()


def extract_flipkart_pid(url: str) -> Optional[str]:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    pid = query.get("pid") or query.get("PID")
    if pid:
        return pid.strip()
    review_match = re.search(r"/product-reviews/([A-Za-z0-9]+)", parsed.path, flags=re.IGNORECASE)
    if review_match:
        return review_match.group(1)
    return None


def build_seed_review_urls(seed_url: str) -> List[str]:
    parsed = urlparse(seed_url)
    host = parsed.netloc
    candidates: List[str] = []

    if is_amazon_url(seed_url):
        asin = extract_amazon_asin(seed_url)
        if asin and host:
            candidates.append(
                canonicalize_url(
                    f"https://{host}/product-reviews/{asin}/?reviewerType=all_reviews&pageNumber=1"
                )
            )

    if "flipkart.com" in normalize_netloc(host):
        pid = extract_flipkart_pid(seed_url)
        if pid:
            candidates.append(
                canonicalize_url(
                    f"https://www.flipkart.com/product-reviews/{pid}?pid={pid}&sortOrder=MOST_HELPFUL"
                )
            )

    candidates.append(seed_url)
    # Deduplicate while preserving order.
    out: List[str] = []
    seen: Set[str] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def ensure_url_scheme(url: str) -> str:
    parsed = urlparse(url.strip())
    if parsed.scheme:
        return url.strip()
    return f"https://{url.strip()}"


def canonicalize_url(raw_url: str) -> str:
    parsed = urlparse(raw_url.strip())
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = sorted(parse_qsl(parsed.query, keep_blank_values=True))
    query = urlencode(query_pairs, doseq=True)
    return urlunparse((scheme, netloc, path, "", query, ""))


def same_site(url_a: str, url_b: str) -> bool:
    host_a = normalize_netloc(urlparse(url_a).netloc)
    host_b = normalize_netloc(urlparse(url_b).netloc)
    if not host_a or not host_b:
        return False
    # Prevent drifting to Amazon ad/cdn subdomains while crawling.
    if AMAZON_HOST_RE.search(host_a) and AMAZON_HOST_RE.search(host_b):
        return host_a == host_b
    return host_a == host_b or host_a.endswith(f".{host_b}") or host_b.endswith(f".{host_a}")


def detect_platform(url: str) -> str:
    netloc = normalize_netloc(urlparse(url).netloc)
    parts = netloc.split(".")
    if len(parts) >= 2:
        return parts[-2]
    return netloc or "unknown"


def infer_product_name_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    path = unquote(parsed.path or "")
    host = normalize_netloc(parsed.netloc)
    if not path:
        return None

    # Amazon often has /<slug>/dp/<ASIN> and title fallback is useful when CAPTCHA page is returned.
    amazon_match = re.search(r"/([^/]+)/dp/[A-Z0-9]{10}(?:[/?]|$)", path, flags=re.IGNORECASE)
    if amazon_match:
        slug = amazon_match.group(1)
        name = clean_text(re.sub(r"[-_]+", " ", slug))
        if name:
            return name

    # Flipkart product URLs usually look like /<slug>/p/<id>.
    flipkart_match = re.search(r"^/([^/]+)/p/[^/]+", path, flags=re.IGNORECASE)
    if flipkart_match:
        slug = flipkart_match.group(1)
        name = clean_text(re.sub(r"[-_]+", " ", slug))
        if name:
            return name

    # Generic slug fallback.
    segments = [seg for seg in path.split("/") if seg and seg not in {"dp", "p", "gp", "product"}]
    candidates: List[str] = []
    for seg in segments:
        if re.fullmatch(r"[A-Za-z0-9]{8,32}", seg):
            continue
        normalized = clean_text(re.sub(r"[-_]+", " ", seg))
        if normalized and len(normalized) >= 6:
            candidates.append(normalized)

    if candidates:
        return max(candidates, key=len)

    # As a last fallback return host-derived label.
    host_label = host.split(".")[0] if host else None
    return clean_text(host_label)


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", value).strip()
    return text or None


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"\d+(\.\d+)?", value)
        if match:
            try:
                return float(match.group(0))
            except ValueError:
                return None
    return None


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%B %d, %Y", "%d %B %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    return value


def extract_html_title(html: str) -> Optional[str]:
    match = HTML_TITLE_RE.search(html or "")
    if not match:
        return None
    return clean_text(match.group(1))


def detect_antibot_reason(html: str, url: str, status_code: Optional[int] = None) -> Optional[str]:
    text = (html or "").lower()
    title = (extract_html_title(html) or "").lower()
    host = normalize_netloc(urlparse(url).netloc)

    if "flipkart.com" in host:
        if "flipkart recaptcha" in title or "are you a human?" in text or "recaptcha" in text:
            return "Flipkart reCAPTCHA challenge page"
        if status_code == 403:
            return "Flipkart blocked the request (HTTP 403)"

    if AMAZON_HOST_RE.search(host):
        amazon_markers = (
            "sorry, we just need to make sure you're not a robot",
            "enter the characters you see below",
            "validatecaptcha",
            "automated access",
        )
        if any(marker in text for marker in amazon_markers):
            return "Amazon CAPTCHA/anti-bot challenge page"
        if title in {"amazon.in", "amazon.com", "amazon.co.uk"} and "captcha" in text:
            return "Amazon challenge page"
        if "amazon sign-in" in title or "sign in to continue" in text or "ap/signin" in text:
            return "Amazon sign-in/login wall"

    generic_markers = (
        "cf-challenge",
        "checking your browser before accessing",
        "access denied",
        "robot check",
    )
    if any(marker in text for marker in generic_markers):
        return "Generic anti-bot challenge page"

    return None


def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        response = session.get(url, headers=headers, timeout=timeout)
        block_reason = detect_antibot_reason(response.text, response.url or url, response.status_code)
        if block_reason:
            raise requests.HTTPError(
                f"Access blocked at {response.url or url}: {block_reason}",
                response=response,
            )
        response.raise_for_status()
        return response.text
    except requests.exceptions.SSLError:
        # Fallback for environments with missing CA roots.
        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
        response = session.get(url, headers=headers, timeout=timeout, verify=False)
        block_reason = detect_antibot_reason(response.text, response.url or url, response.status_code)
        if block_reason:
            raise requests.HTTPError(
                f"Access blocked at {response.url or url}: {block_reason}",
                response=response,
            )
        response.raise_for_status()
        return response.text


def get_thread_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        setattr(_THREAD_LOCAL, "session", session)
    return session


def build_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def type_matches(value: Any, target: str) -> bool:
    target_l = target.lower()
    if isinstance(value, list):
        return any(str(item).lower() == target_l for item in value)
    if isinstance(value, str):
        return value.lower() == target_l
    return False


def parse_json_ld_objects(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    objects: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not tag.string and not tag.text:
            continue
        raw = (tag.string or tag.text or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            objects.extend(item for item in parsed if isinstance(item, dict))
        elif isinstance(parsed, dict):
            if "@graph" in parsed and isinstance(parsed["@graph"], list):
                objects.extend(item for item in parsed["@graph"] if isinstance(item, dict))
            objects.append(parsed)
    return objects


def first_of_type(objects: Iterable[Dict[str, Any]], type_name: str) -> Optional[Dict[str, Any]]:
    for obj in objects:
        if type_matches(obj.get("@type"), type_name):
            return obj
    return None


def extract_rating_from_text(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    patterns = [
        r"(\d+(?:\.\d+)?)\s*(?:out of|/)\s*5",
        r"(\d+(?:\.\d+)?)\s*stars?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return to_float(match.group(1))
    return None


def parse_schema_review_item(item: Dict[str, Any], source_url: str) -> Optional[Review]:
    if not isinstance(item, dict):
        return None

    author = item.get("author")
    if isinstance(author, dict):
        author = author.get("name")
    elif isinstance(author, list):
        author = ", ".join(
            str(a.get("name")) if isinstance(a, dict) else str(a) for a in author
        )

    review_rating = item.get("reviewRating")
    rating_value = None
    if isinstance(review_rating, dict):
        rating_value = to_float(review_rating.get("ratingValue"))
    else:
        rating_value = to_float(review_rating)

    body = clean_text(item.get("reviewBody") or item.get("description") or "")
    if not body:
        return None

    return Review(
        title=clean_text(item.get("name") or item.get("headline")),
        rating=rating_value,
        author=clean_text(author if isinstance(author, str) else None),
        date=normalize_date(clean_text(item.get("datePublished"))),
        content=body,
        source_url=source_url,
    )


def review_key(review: Review) -> Tuple[str, str, str, str]:
    return (
        (review.author or "").lower(),
        (review.date or "").lower(),
        str(review.rating or ""),
        review.content[:220].lower(),
    )


def parse_schema_product(soup: BeautifulSoup, source_url: str) -> Dict[str, Any]:
    objects = parse_json_ld_objects(soup)
    product = first_of_type(objects, "Product")
    aggregate_rating = None

    product_name: Optional[str] = None
    if product:
        product_name = clean_text(product.get("name"))
        aggregate_rating = product.get("aggregateRating")

    if not aggregate_rating:
        aggregate_obj = first_of_type(objects, "AggregateRating")
        aggregate_rating = aggregate_obj if aggregate_obj else None

    overall_rating: Optional[float] = None
    total_reviews: Optional[int] = None
    if isinstance(aggregate_rating, dict):
        overall_rating = to_float(aggregate_rating.get("ratingValue"))
        review_count = aggregate_rating.get("reviewCount") or aggregate_rating.get("ratingCount")
        if review_count is not None:
            try:
                total_reviews = int(float(str(review_count)))
            except ValueError:
                total_reviews = None

    reviews: List[Review] = []
    seen: Set[Tuple[str, str, str, str]] = set()

    if product and product.get("review") is not None:
        raw_reviews = product.get("review")
        if isinstance(raw_reviews, dict):
            raw_reviews = [raw_reviews]
        if isinstance(raw_reviews, list):
            for item in raw_reviews:
                if not isinstance(item, dict):
                    continue
                parsed = parse_schema_review_item(item, source_url)
                if not parsed:
                    continue
                key = review_key(parsed)
                if key in seen:
                    continue
                seen.add(key)
                reviews.append(parsed)

    for obj in objects:
        if not type_matches(obj.get("@type"), "Review"):
            continue
        parsed = parse_schema_review_item(obj, source_url)
        if not parsed:
            continue
        key = review_key(parsed)
        if key in seen:
            continue
        seen.add(key)
        reviews.append(parsed)

    return {
        "product_name": product_name,
        "overall_rating": overall_rating,
        "total_reviews": total_reviews,
        "reviews": reviews,
    }


def tag_attr_blob(tag: Any) -> str:
    class_text = ""
    if tag.get("class"):
        class_raw = tag.get("class")
        if isinstance(class_raw, list):
            class_text = " ".join(str(item) for item in class_raw)
        else:
            class_text = str(class_raw)
    return " ".join(
        [
            str(tag.get("id", "")),
            class_text,
            str(tag.get("data-testid", "")),
            str(tag.get("itemprop", "")),
            str(tag.get("aria-label", "")),
        ]
    )


def parse_generic_reviews(soup: BeautifulSoup, source_url: str) -> List[Review]:
    def is_candidate(tag: Any) -> bool:
        if tag.name not in {"div", "article", "li", "section"}:
            return False
        if str(tag.get("itemprop", "")).lower() == "review":
            return True
        return bool(REVIEW_ATTR_RE.search(tag_attr_blob(tag)))

    candidates = soup.find_all(is_candidate)
    reviews: List[Review] = []
    seen: Set[Tuple[str, str, str, str]] = set()

    for node in candidates:
        text = clean_text(node.get_text(" ", strip=True))
        if not text or len(text) < 40 or len(text) > 4500:
            continue

        title_node = node.find(attrs={"itemprop": re.compile(r"name|headline", re.I)}) or node.find(
            ["h2", "h3", "h4", "strong"]
        )
        author_node = node.find(attrs={"itemprop": re.compile(r"author", re.I)}) or node.find(
            attrs={"class": re.compile(r"author|user|name|profile", re.I)}
        )
        date_node = node.find("time") or node.find(
            attrs={"class": re.compile(r"date|time", re.I)}
        )
        rating_node = node.find(attrs={"itemprop": re.compile(r"ratingvalue", re.I)}) or node.find(
            attrs={"class": re.compile(r"rating|stars?|score", re.I)}
        )
        body_node = node.find(attrs={"itemprop": re.compile(r"reviewbody|description", re.I)})

        rating = None
        if rating_node:
            rating = to_float(
                rating_node.get("content")
                or rating_node.get("aria-label")
                or rating_node.get_text(" ", strip=True)
            )
        if rating is None:
            rating = extract_rating_from_text(text)

        body = clean_text(body_node.get_text(" ", strip=True) if body_node else text)
        if not body:
            continue
        lower_body = body.lower()
        if any(
            marker in lower_body
            for marker in (
                "customer reviews ",
                "how are ratings calculated",
                "would you like to tell us about a lower price",
                "where did you see a lower price",
                "add to cart",
                "buy now",
                "shop now",
                "captcha",
                "are you a human",
            )
        ):
            continue
        if len(body) > 1600:
            continue

        review = Review(
            title=clean_text(title_node.get_text(" ", strip=True) if title_node else None),
            rating=rating,
            author=clean_text(author_node.get_text(" ", strip=True) if author_node else None),
            date=normalize_date(
                clean_text(
                    date_node.get("datetime")
                    if date_node and date_node.has_attr("datetime")
                    else (date_node.get_text(" ", strip=True) if date_node else None)
                )
            ),
            content=body,
            source_url=source_url,
        )
        review_signals = 0
        if review.rating is not None:
            review_signals += 1
        if review.author:
            review_signals += 1
        if review.date:
            review_signals += 1
        if "reviewed in" in lower_body or "verified purchase" in lower_body:
            review_signals += 1
        if review_signals == 0:
            continue
        if review_signals < 2 and "verified purchase" not in lower_body and "reviewed in " not in lower_body:
            continue
        key = review_key(review)
        if key in seen:
            continue
        seen.add(key)
        reviews.append(review)

    return reviews


def parse_amazon_reviews(soup: BeautifulSoup, source_url: str) -> List[Review]:
    reviews: List[Review] = []
    seen: Set[Tuple[str, str, str, str]] = set()

    for node in soup.select("div[data-hook='review']"):
        title_node = node.select_one("a[data-hook='review-title'], span[data-hook='review-title']")
        rating_node = node.select_one(
            "i[data-hook='review-star-rating'], i[data-hook='cmps-review-star-rating']"
        )
        author_node = node.select_one(".a-profile-name")
        date_node = node.select_one("span[data-hook='review-date']")
        body_node = node.select_one("span[data-hook='review-body'], div[data-hook='review-collapsed']")

        body = clean_text(body_node.get_text(" ", strip=True) if body_node else None)
        if not body or len(body) < 15:
            continue

        rating = None
        if rating_node:
            rating = to_float(rating_node.get_text(" ", strip=True))

        review = Review(
            title=clean_text(title_node.get_text(" ", strip=True) if title_node else None),
            rating=rating,
            author=clean_text(author_node.get_text(" ", strip=True) if author_node else None),
            date=normalize_date(clean_text(date_node.get_text(" ", strip=True) if date_node else None)),
            content=body,
            source_url=source_url,
        )
        key = review_key(review)
        if key in seen:
            continue
        seen.add(key)
        reviews.append(review)

    return reviews


def parse_product_title(soup: BeautifulSoup) -> Optional[str]:
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return clean_text(og.get("content"))
    if soup.title and soup.title.string:
        return clean_text(soup.title.string)
    heading = soup.find(["h1", "h2"])
    if heading:
        return clean_text(heading.get_text(" ", strip=True))
    return None


def is_generic_title_for_url(title: Optional[str], source_url: str) -> bool:
    if not title:
        return True

    normalized = clean_text(title)
    if not normalized:
        return True

    lowered = normalized.lower()
    host = normalize_netloc(urlparse(source_url).netloc)

    generic_titles = {
        "amazon.in",
        "amazon.com",
        "amazon.co.uk",
        "flipkart recaptcha",
        "are you a human?",
    }
    if lowered in generic_titles:
        return True

    if host and lowered in {host, host.replace("www.", ""), host.split(":")[0]}:
        return True

    if lowered in {"home", "online shopping"}:
        return True

    return False


def is_valid_crawl_url(candidate_url: str, seed_url: str) -> bool:
    parsed = urlparse(candidate_url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if not parsed.netloc:
        return False
    if not same_site(candidate_url, seed_url):
        return False

    lower_url = candidate_url.lower()
    if re.search(r"/(cart|checkout|account|login|register|wishlist)", lower_url):
        return False

    # Avoid malformed Amazon links like /dp//ref=...
    if AMAZON_HOST_RE.search(normalize_netloc(parsed.netloc)):
        if "/dp//" in parsed.path.lower():
            return False
        if "/dp/" in parsed.path.lower() or "/gp/product/" in parsed.path.lower() or "/gp/aw/d/" in parsed.path.lower():
            if not AMAZON_ASIN_RE.search(parsed.path):
                return False

    return True


def discover_review_links(soup: BeautifulSoup, current_url: str, seed_url: str) -> List[str]:
    ranked: Dict[str, int] = {}
    current_canonical = canonicalize_url(current_url)
    seed_host = normalize_netloc(urlparse(seed_url).netloc)
    seed_amazon_asin = extract_amazon_asin(seed_url) if is_amazon_url(seed_url) else None

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        absolute = canonicalize_url(urljoin(current_url, href))
        if absolute == current_canonical:
            continue
        if not is_valid_crawl_url(absolute, seed_url):
            continue

        if is_amazon_url(seed_url):
            candidate_path = (urlparse(absolute).path or "").lower()
            if seed_amazon_asin:
                asin_l = seed_amazon_asin.lower()
                if f"/product-reviews/{asin_l}" not in candidate_path and f"/dp/{asin_l}" not in candidate_path:
                    continue
            elif "/product-reviews/" not in candidate_path and "/customer-reviews/" not in candidate_path:
                continue
        elif "flipkart.com" in seed_host:
            candidate_path = (urlparse(absolute).path or "").lower()
            if "/product-reviews/" not in candidate_path and "/reviews" not in candidate_path:
                continue

        class_blob = ""
        if anchor.get("class"):
            class_blob = " ".join(str(x) for x in anchor.get("class"))
        rel_values = anchor.get("rel") or []
        if isinstance(rel_values, str):
            rel_values = [rel_values]
        rel_blob = " ".join(str(x) for x in rel_values).lower()

        blob = " ".join(
            [
                clean_text(anchor.get_text(" ", strip=True)) or "",
                str(anchor.get("aria-label", "")),
                str(anchor.get("title", "")),
                str(anchor.get("id", "")),
                class_blob,
                rel_blob,
            ]
        ).lower()

        score = 0
        if "next" in rel_blob:
            score += 120
        if NEXT_TEXT_RE.search(blob):
            score += 70
        if ALL_REVIEWS_TEXT_RE.search(blob):
            score += 90
        if REVIEW_LINK_RE.search(absolute):
            score += 55

        if score <= 0:
            continue
        previous = ranked.get(absolute, 0)
        if score > previous:
            ranked[absolute] = score

    ordered = sorted(ranked.items(), key=lambda item: (-item[1], item[0]))
    return [url for url, _score in ordered[:120]]


def parse_page_from_soup(soup: BeautifulSoup, page_url: str, seed_url: str) -> Dict[str, Any]:
    schema_data = parse_schema_product(soup, page_url)
    page_reviews: List[Review] = schema_data["reviews"]
    if not page_reviews and is_amazon_url(page_url):
        page_reviews = parse_amazon_reviews(soup, page_url)
    if not page_reviews:
        if is_amazon_url(page_url):
            path_l = (urlparse(page_url).path or "").lower()
            if "/product-reviews/" in path_l or "/customer-reviews/" in path_l:
                page_reviews = parse_generic_reviews(soup, page_url)
        else:
            page_reviews = parse_generic_reviews(soup, page_url)

    page_product_name = schema_data["product_name"] or parse_product_title(soup)
    if is_generic_title_for_url(page_product_name, page_url):
        page_product_name = infer_product_name_from_url(page_url)

    return {
        "product_name": page_product_name,
        "overall_rating": schema_data["overall_rating"],
        "total_reviews": schema_data["total_reviews"],
        "reviews": page_reviews,
        "next_links": discover_review_links(soup, page_url, seed_url),
    }


def process_page(url: str, seed_url: str, timeout: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "url": url,
        "ok": False,
        "error": None,
        "product_name": None,
        "overall_rating": None,
        "total_reviews": None,
        "reviews": [],
        "next_links": [],
    }

    try:
        html = fetch_html(get_thread_session(), url, timeout=timeout)
        soup = build_soup(html)
    except requests.RequestException as exc:
        result["error"] = f"Failed to fetch {url}: {exc}"
        return result
    except Exception as exc:  # pragma: no cover
        result["error"] = f"Unexpected fetch/parse error at {url}: {exc}"
        return result

    parsed = parse_page_from_soup(soup=soup, page_url=url, seed_url=seed_url)

    result["ok"] = True
    result["product_name"] = parsed["product_name"]
    result["overall_rating"] = parsed["overall_rating"]
    result["total_reviews"] = parsed["total_reviews"]
    result["reviews"] = parsed["reviews"]
    result["next_links"] = parsed["next_links"]
    return result


def add_unique_reviews(
    collected: List[Review],
    seen: Set[Tuple[str, str, str, str]],
    incoming: Iterable[Review],
    max_reviews: int,
) -> None:
    for review in incoming:
        key = review_key(review)
        if key in seen:
            continue
        seen.add(key)
        collected.append(review)
        if max_reviews > 0 and len(collected) >= max_reviews:
            break


def create_chrome_driver(
    timeout: int = 60,
    headless: bool = False,
    user_data_dir: Optional[str] = None,
) -> Any:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Selenium is not installed. Install it with: pip install selenium"
        ) from exc

    options = Options()
    options.add_argument(f"--user-agent={USER_AGENT}")
    options.add_argument("--lang=en-US")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if user_data_dir:
        options.add_argument(f"--user-data-dir={user_data_dir}")
    if headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(max(30, timeout))
    return driver


def crawl_reviews_with_browser(
    seed_url: str,
    platform: str,
    max_reviews: int,
    max_pages: int,
    timeout: int,
    browser_headless: bool = False,
    chrome_user_data_dir: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "url": seed_url,
        "platform": platform,
        "product_name": infer_product_name_from_url(seed_url),
        "overall_rating": None,
        "total_reviews": None,
        "pages_crawled": 0,
        "pages_discovered": 1,
        "failed_pages": 0,
        "reviews_collected": 0,
        "coverage_pct": None,
        "reviews": [],
        "blocked_by_antibot": False,
        "browser_fallback_attempted": True,
        "browser_fallback_used": False,
        "error": None,
    }

    try:
        driver = create_chrome_driver(
            timeout=timeout,
            headless=browser_headless,
            user_data_dir=chrome_user_data_dir,
        )
    except Exception as exc:
        payload["failed_pages"] = 1
        payload["error"] = f"Browser fallback unavailable: {exc}"
        return payload

    queue: Deque[str] = deque(build_seed_review_urls(seed_url))
    discovered: Set[str] = set(queue)
    visited: Set[str] = set()
    reviews: List[Review] = []
    seen_review_keys: Set[Tuple[str, str, str, str]] = set()
    first_error: Optional[str] = None
    prompted_for_manual_step = False

    try:
        while queue and payload["pages_crawled"] < max_pages:
            page_url = queue.popleft()
            if page_url in visited:
                continue
            visited.add(page_url)
            payload["pages_crawled"] += 1

            try:
                driver.get(page_url)
                time.sleep(1.8)

                current_url = canonicalize_url(driver.current_url or page_url)
                html = driver.page_source or ""
                block_reason = detect_antibot_reason(html, current_url)

                if block_reason and not browser_headless and not prompted_for_manual_step:
                    print(
                        "Browser fallback: solve login/CAPTCHA in the opened browser, "
                        "then press Enter here to continue..."
                    )
                    try:
                        input()
                    except EOFError:
                        pass
                    time.sleep(1.2)
                    current_url = canonicalize_url(driver.current_url or page_url)
                    html = driver.page_source or ""
                    block_reason = detect_antibot_reason(html, current_url)
                    prompted_for_manual_step = True

                if block_reason:
                    payload["failed_pages"] += 1
                    payload["blocked_by_antibot"] = True
                    if first_error is None:
                        first_error = f"Browser blocked at {current_url}: {block_reason}"
                    continue

                soup = build_soup(html)
                page_result = parse_page_from_soup(
                    soup=soup, page_url=current_url, seed_url=seed_url
                )

                if payload["product_name"] is None and page_result["product_name"]:
                    payload["product_name"] = page_result["product_name"]
                if (
                    payload["overall_rating"] is None
                    and page_result["overall_rating"] is not None
                ):
                    payload["overall_rating"] = page_result["overall_rating"]
                if page_result["total_reviews"] is not None:
                    if payload["total_reviews"] is None:
                        payload["total_reviews"] = page_result["total_reviews"]
                    else:
                        payload["total_reviews"] = max(
                            int(payload["total_reviews"]),
                            int(page_result["total_reviews"]),
                        )

                add_unique_reviews(reviews, seen_review_keys, page_result["reviews"], max_reviews)

                stop_requested = False
                if max_reviews > 0 and len(reviews) >= max_reviews:
                    stop_requested = True
                if payload["total_reviews"] and len(reviews) >= int(payload["total_reviews"]):
                    stop_requested = True

                if not stop_requested:
                    for next_link in page_result["next_links"]:
                        if next_link in discovered or next_link in visited:
                            continue
                        discovered.add(next_link)
                        queue.append(next_link)
                else:
                    break

            except Exception as exc:
                payload["failed_pages"] += 1
                if first_error is None:
                    first_error = f"Browser fetch failed at {page_url}: {exc}"

        payload["pages_discovered"] = len(discovered)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    payload["reviews"] = [asdict(review) for review in reviews]
    payload["reviews_collected"] = len(payload["reviews"])
    if payload["total_reviews"] and payload["reviews_collected"] > 0:
        coverage = (payload["reviews_collected"] / float(payload["total_reviews"])) * 100.0
        payload["coverage_pct"] = round(min(coverage, 100.0), 2)

    if payload["reviews_collected"] > 0:
        payload["browser_fallback_used"] = True
        payload["error"] = None
    else:
        payload["error"] = first_error or "Browser fallback could not extract any reviews."

    return payload


def extract_reviews_from_url(
    url: str,
    max_reviews: int = 0,
    max_pages: int = 120,
    timeout: int = 30,
    workers: int = 8,
    browser_fallback: bool = True,
    browser_headless: bool = False,
    chrome_user_data_dir: Optional[str] = None,
) -> Dict[str, Any]:
    seed_url = canonicalize_url(ensure_url_scheme(url))
    platform = detect_platform(seed_url)
    workers = max(1, min(int(workers), 32))

    payload: Dict[str, Any] = {
        "url": seed_url,
        "platform": platform,
        "product_name": infer_product_name_from_url(seed_url),
        "overall_rating": None,
        "total_reviews": None,
        "pages_crawled": 0,
        "pages_discovered": 1,
        "failed_pages": 0,
        "reviews_collected": 0,
        "coverage_pct": None,
        "reviews": [],
        "blocked_by_antibot": False,
        "browser_fallback_attempted": False,
        "browser_fallback_used": False,
        "error": None,
    }

    queue: Deque[str] = deque([seed_url])
    discovered: Set[str] = {seed_url}
    visited: Set[str] = set()

    reviews: List[Review] = []
    seen_review_keys: Set[Tuple[str, str, str, str]] = set()
    seed_error: Optional[str] = None
    first_failure_error: Optional[str] = None

    with ThreadPoolExecutor(max_workers=workers) as executor:
        while queue and payload["pages_crawled"] < max_pages:
            remaining_capacity = max_pages - payload["pages_crawled"]
            if remaining_capacity <= 0:
                break

            batch_limit = min(remaining_capacity, workers)
            batch: List[str] = []
            while queue and len(batch) < batch_limit:
                candidate_url = queue.popleft()
                if candidate_url in visited:
                    continue
                visited.add(candidate_url)
                batch.append(candidate_url)

            if not batch:
                continue

            payload["pages_crawled"] += len(batch)

            future_map = {
                executor.submit(process_page, page_url, seed_url, timeout): page_url
                for page_url in batch
            }

            stop_requested = False
            for future in as_completed(future_map):
                page_url = future_map[future]
                try:
                    page_result = future.result()
                except Exception as exc:  # pragma: no cover
                    payload["failed_pages"] += 1
                    if page_url == seed_url and seed_error is None:
                        seed_error = f"Unexpected worker error at {page_url}: {exc}"
                    continue

                if not page_result["ok"]:
                    payload["failed_pages"] += 1
                    err_text = (page_result.get("error") or "").lower()
                    if (
                        "access blocked at" in err_text
                        or "anti-bot challenge" in err_text
                        or "recaptcha" in err_text
                    ):
                        payload["blocked_by_antibot"] = True
                    if first_failure_error is None:
                        first_failure_error = page_result["error"] or f"Failed to fetch {page_url}"
                    if page_url == seed_url and seed_error is None:
                        seed_error = page_result["error"] or f"Failed to fetch {page_url}"
                    continue

                if payload["product_name"] is None:
                    payload["product_name"] = page_result["product_name"]

                if (
                    payload["overall_rating"] is None
                    and page_result["overall_rating"] is not None
                ):
                    payload["overall_rating"] = page_result["overall_rating"]

                if page_result["total_reviews"] is not None:
                    if payload["total_reviews"] is None:
                        payload["total_reviews"] = page_result["total_reviews"]
                    else:
                        payload["total_reviews"] = max(
                            int(payload["total_reviews"]), int(page_result["total_reviews"])
                        )

                add_unique_reviews(
                    reviews,
                    seen_review_keys,
                    page_result["reviews"],
                    max_reviews,
                )

                if max_reviews > 0 and len(reviews) >= max_reviews:
                    stop_requested = True
                if payload["total_reviews"] and len(reviews) >= int(payload["total_reviews"]):
                    stop_requested = True

                if not stop_requested:
                    for next_link in page_result["next_links"]:
                        if next_link in discovered or next_link in visited:
                            continue
                        discovered.add(next_link)
                        queue.append(next_link)

            if stop_requested:
                break

        payload["pages_discovered"] = len(discovered)

    payload["reviews"] = [asdict(review) for review in reviews]
    payload["reviews_collected"] = len(payload["reviews"])

    if payload["total_reviews"] and payload["reviews_collected"] > 0:
        coverage = (payload["reviews_collected"] / float(payload["total_reviews"])) * 100.0
        payload["coverage_pct"] = round(min(coverage, 100.0), 2)

    if not payload["reviews"]:
        if payload["blocked_by_antibot"]:
            payload["error"] = seed_error or first_failure_error or (
                "Blocked by anti-bot challenge. Try Selenium/Playwright with a real browser "
                "session, rotating proxies, and slower human-like navigation."
            )
        else:
            payload["error"] = seed_error or first_failure_error or (
                "No reviews extracted. The site may use JavaScript-only review APIs, "
                "require login, or present anti-bot challenges."
            )
    else:
        payload["error"] = None

    if (
        browser_fallback
        and payload["reviews_collected"] == 0
        and payload["blocked_by_antibot"]
    ):
        payload["browser_fallback_attempted"] = True
        browser_payload = crawl_reviews_with_browser(
            seed_url=seed_url,
            platform=platform,
            max_reviews=max_reviews,
            max_pages=max_pages,
            timeout=timeout,
            browser_headless=browser_headless,
            chrome_user_data_dir=chrome_user_data_dir,
        )
        if browser_payload.get("reviews_collected", 0) > 0:
            return browser_payload
        payload["error"] = browser_payload.get("error") or payload["error"]

    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch product reviews from a product URL and print normalized JSON."
    )
    parser.add_argument("url", nargs="?", help="Product URL")
    parser.add_argument(
        "--prompt",
        action="store_true",
        help="Prompt for product URL interactively (safe for long URLs with '&').",
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=0,
        help="Maximum number of reviews to return. Use 0 for all discovered reviews (default: 0).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=120,
        help="Maximum number of pages to crawl while discovering reviews (default: 120).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP request timeout per page in seconds (default: 30).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent worker threads for page fetch/parse (default: 8).",
    )
    parser.add_argument(
        "--no-browser-fallback",
        action="store_true",
        help="Disable Selenium browser fallback when requests mode is blocked.",
    )
    parser.add_argument(
        "--browser-headless",
        action="store_true",
        help="Run browser fallback headless (not recommended for manual CAPTCHA/login).",
    )
    parser.add_argument(
        "--chrome-user-data-dir",
        default=None,
        help="Optional Chrome user-data directory for Selenium fallback session reuse.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    input_url = args.url
    if args.prompt or not input_url:
        input_url = input("Enter product URL: ").strip()
    if not input_url:
        print(json.dumps({"error": "Missing product URL"}, indent=2, ensure_ascii=True))
        return

    result = extract_reviews_from_url(
        input_url,
        max_reviews=max(0, args.max_reviews),
        max_pages=max(1, args.max_pages),
        timeout=max(5, args.timeout),
        workers=max(1, args.workers),
        browser_fallback=not args.no_browser_fallback,
        browser_headless=bool(args.browser_headless),
        chrome_user_data_dir=args.chrome_user_data_dir,
    )
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
