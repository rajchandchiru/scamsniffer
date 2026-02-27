from __future__ import annotations

import argparse
import difflib
import json
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

BUYHATKE_BASE_URL = "https://buyhatke.com"
INSERT_REDIRECT_API = f"{BUYHATKE_BASE_URL}/api/insertURLForRedirect"
GET_REDIRECT_API = f"{BUYHATKE_BASE_URL}/api/getRedirectedURL"
PRODUCT_DATA_API = f"{BUYHATKE_BASE_URL}/api/productData"
POS_LIST_API = f"{BUYHATKE_BASE_URL}/api/posList"

PRICESPY_BASE_URL = "https://pricespy.co.uk"
PRICESPY_BFF_URL = f"{PRICESPY_BASE_URL}/_internal/bff"

KNOWN_INDIAN_DOMAINS = {
    # Marketplaces / retail.
    "flipkart.com",
    "amazon.in",
    "myntra.com",
    "ajio.com",
    "meesho.com",
    "snapdeal.com",
    "tatacliq.com",
    "jiomart.com",
    "bigbasket.com",
    "nykaa.com",
    "nykaafashion.com",
    "vijaysales.com",
    "croma.com",
    "reliancedigital.in",
    "shopclues.com",
    "paytmmall.com",
    "naaptol.com",
    "firstcry.com",
    "lenskart.com",
    "pepperfry.com",
    "homeshop18.com",
    "shoppersstop.com",
    # Category-specific / health / beauty.
    "pharmeasy.in",
    "netmeds.com",
    "1mg.com",
    "purplle.com",
    # Brand stores commonly used in India pricing context.
    "in.store.asus.com",
    "store.hp.com",
    "dell.com",
    "samsung.com",
    "mi.com",
    "oneplus.in",
    "boat-lifestyle.com",
}

PRICESPY_SEARCH_QUERY = """
query SearchPage($query: String!, $offset: Int, $limit: Int, $allProductsFilter: Boolean = false, $sort: SearchProductSortingEnum, $order: SearchOrder) {
  newSearch(query: $query, allProductsFilter: $allProductsFilter) {
    query
    results {
      products(offset: $offset, limit: $limit, sort: $sort, order: $order) {
        pageInfo {
          total
          pages
          offset
          limit
        }
        nodes {
          __typename
          ... on Product {
            id
            name
            pathName
            priceSummary {
              regular
              alternative
              count
            }
            aggregatedRating {
              score
              count
            }
          }
          ... on Offer {
            offerId: id
            name
            externalUri
            offerPrice {
              regular
            }
            store {
              id
              name
              featured
              currency
            }
          }
        }
      }
    }
  }
}
"""

PRICESPY_SUGGESTIONS_QUERY = """
query suggestions($query: String!) {
  searchSuggestions(query: $query) {
    __typename
    ... on SuggestedProduct {
      text
      id
      category
      price
    }
    ... on SuggestedSearchPhrase {
      text
    }
    ... on SuggestedShop {
      id
      text
    }
    ... on SuggestedCategoryFilter {
      text
      id
      name
    }
  }
}
"""

PRICESPY_PRODUCT_OFFERS_QUERY = """
query ProductOffers($id: Int!) {
  product(id: $id) {
    id
    name
    pathName
    category {
      name
      pathName
    }
    priceSummary {
      regular
      alternative
      count
    }
    prices {
      meta {
        itemsTotal
        storeStatistics {
          totalCount
          featuredCount
        }
      }
      nodes {
        shopOfferId
        name
        externalUri
        primaryMarket
        stock {
          status
          statusText
        }
        condition
        price {
          inclShipping
          exclShipping
          originalCurrency
        }
        offerPrices {
          price {
            inclShipping
            exclShipping
            endDate
          }
          originalPrice {
            inclShipping
            exclShipping
          }
          memberPrice {
            inclShipping
            exclShipping
            endDate
          }
        }
        store {
          id
          name
          featured
          hasLogo
          logo(width: _176)
          pathName
          currency
          countryCode
          userReviewSummary {
            rating
            count
            countTotal
          }
        }
        shipping {
          cheapest {
            deliveryDays {
              min
              max
            }
            shippingCost
            carrier
            sustainability
          }
        }
      }
    }
  }
}
"""


def is_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def prompt_product_url() -> str:
    while True:
        value = input("Enter product URL to compare prices: ").strip()
        if is_valid_url(value):
            return value
        print("Invalid URL. Please include http:// or https://")


def prompt_product_title() -> str:
    while True:
        value = input("Enter product title to search on PriceSpy: ").strip()
        if value:
            return value
        print("Product title cannot be empty.")


def is_indian_market_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    host_no_www = re.sub(r"^(www\.|m\.|mobile\.)", "", host)

    if host.endswith(".in") or host.endswith(".co.in"):
        return True

    if any(
        host_no_www == domain or host_no_www.endswith(f".{domain}")
        for domain in KNOWN_INDIAN_DOMAINS
    ):
        return True

    # Buyhatke country paths (e.g. /uk/...) indicate non-India pages.
    if "buyhatke.com" in host:
        country_path_match = re.match(r"^/([a-z]{2})/", path)
        if country_path_match and country_path_match.group(1) != "in":
            return False
        return True

    return False


def title_from_url_slug(url: str) -> Optional[str]:
    parsed = urlparse(url)
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if not parts:
        return None

    # Prefer human-readable slugs over technical tokens.
    blocked = {"dp", "gp", "product", "products", "item", "p", "ref"}
    candidates: List[str] = []
    for part in parts:
        cleaned = part.strip().strip("-_")
        if not cleaned:
            continue
        if cleaned.lower() in blocked:
            continue
        if re.fullmatch(r"[A-Za-z0-9]{8,20}", cleaned):
            continue
        candidates.append(cleaned)

    if not candidates:
        return None

    best = max(candidates, key=len)
    best = re.sub(r"[-_]+", " ", best)
    best = re.sub(r"\s+", " ", best).strip()
    return best or None


def infer_product_title_from_url(
    session: requests.Session, input_url: str
) -> Tuple[str, str]:
    resolved_url = input_url
    title: Optional[str] = None

    try:
        response = session.get(
            input_url,
            timeout=25,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                )
            },
        )
        response.raise_for_status()
        resolved_url = response.url or input_url

        soup = BeautifulSoup(response.text, "lxml")
        meta_og = soup.select_one("meta[property='og:title']")
        meta_name = soup.select_one("meta[name='title']")
        page_title = soup.title.get_text(" ", strip=True) if soup.title else None

        for candidate in (
            meta_og.get("content") if meta_og else None,
            meta_name.get("content") if meta_name else None,
            page_title,
        ):
            if isinstance(candidate, str):
                cleaned = re.sub(r"\s+", " ", candidate).strip()
                if cleaned:
                    title = cleaned
                    break
    except requests.RequestException:
        resolved_url = input_url

    if not title:
        title = title_from_url_slug(resolved_url) or title_from_url_slug(input_url)

    if not title:
        raise ValueError(
            "Could not infer product title from URL for PriceSpy search. "
            "Use `--platform pricespy --title \"...\"`."
        )

    return title, resolved_url


def build_pricespy_query_variants(product_title: str) -> List[str]:
    cleaned = re.sub(r"\s+", " ", product_title).strip()
    if not cleaned:
        return []

    variants: List[str] = [cleaned]
    lowered = cleaned.lower()

    # Extract core iPhone model phrases for accessories and phones.
    iphone_match = re.search(
        r"\biphone\s*\d+(?:\s*(?:pro max|pro|plus))?\b", lowered, re.IGNORECASE
    )
    if iphone_match:
        core = iphone_match.group(0)
        accessory_terms = []
        for term in ("case", "cover", "magsafe", "screen protector", "charger"):
            if term in lowered:
                accessory_terms.append(term)
        if accessory_terms:
            variants.append(" ".join([core] + accessory_terms))
        variants.append(core)

    # Extract model-like alphanumerics (e.g., RTX, TUF, phone model names).
    normalized = re.sub(r"[^A-Za-z0-9+ ]+", " ", cleaned)
    tokens = [t for t in normalized.split() if len(t) > 1]
    if tokens:
        variants.append(" ".join(tokens[:10]))
        variants.append(" ".join(tokens[:6]))

    # Remove noisy long-tail marketing descriptors.
    noise_terms = {
        "upgraded",
        "military",
        "grade",
        "shockproof",
        "translucent",
        "protection",
        "only",
        "compatible",
    }
    compact_tokens = [t for t in tokens if t.lower() not in noise_terms]
    if compact_tokens:
        variants.append(" ".join(compact_tokens[:8]))

    # Generic landing/sale page cleanup for non-product URLs.
    generic_terms = {
        "sale",
        "deals",
        "deal",
        "offer",
        "offers",
        "store",
        "shop",
        "shopping",
        "coupon",
        "discount",
        "official",
        "online",
        "buy",
        "at",
        "on",
        "in",
        "for",
        "with",
        "the",
        "omg",
    }
    month_token = re.compile(
        r"^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\d{0,2}$",
        re.IGNORECASE,
    )
    filtered_tokens = [
        t
        for t in tokens
        if t.lower() not in generic_terms
        and not month_token.match(t)
        and not re.fullmatch(r"\d{2,4}", t)
    ]
    if filtered_tokens:
        variants.append(" ".join(filtered_tokens[:6]))
        variants.append(" ".join(filtered_tokens[:3]))
        variants.append(filtered_tokens[0])

    # De-duplicate while preserving order.
    out: List[str] = []
    seen = set()
    for item in variants:
        val = item.strip()
        if not val:
            continue
        key = normalize_for_match(val)
        if key in seen:
            continue
        seen.add(key)
        out.append(val)
    return out


def pricespy_graphql_request(
    session: requests.Session, query: str, variables: Dict[str, Any]
) -> Dict[str, Any]:
    response = session.post(
        PRICESPY_BFF_URL,
        json={"query": query, "variables": variables},
        timeout=45,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()

    errors = payload.get("errors")
    if errors:
        first_error = errors[0] if isinstance(errors, list) and errors else {}
        message = first_error.get("message") if isinstance(first_error, dict) else str(first_error)
        raise ValueError(f"PriceSpy GraphQL error: {message}")

    return payload.get("data") or {}


def normalize_for_match(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def token_overlap_ratio(a: str, b: str) -> float:
    tokens_a = set(normalize_for_match(a).split())
    tokens_b = set(normalize_for_match(b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    return intersection / union if union else 0.0


def score_pricespy_product_match(
    search_title: str,
    product_name: str,
    price_count: Optional[int],
    category_text: Optional[str] = None,
) -> float:
    normalized_search = normalize_for_match(search_title)
    normalized_name = normalize_for_match(product_name)
    normalized_category = normalize_for_match(category_text or "")

    seq_ratio = difflib.SequenceMatcher(None, normalized_search, normalized_name).ratio()
    overlap = token_overlap_ratio(search_title, product_name)
    token_bonus = 0.15 if all(t in normalized_name for t in normalized_search.split() if t) else 0.0
    availability_bonus = 0.05 if (isinstance(price_count, int) and price_count > 0) else 0.0
    accessory_keywords = {
        "case",
        "cover",
        "protector",
        "charger",
        "cable",
        "accessory",
        "mount",
        "skin",
        "holder",
    }
    search_has_accessory = any(k in normalized_search for k in accessory_keywords)
    candidate_has_accessory = any(
        k in normalized_name or k in normalized_category for k in accessory_keywords
    )
    accessory_penalty = -0.25 if candidate_has_accessory and not search_has_accessory else 0.0

    return (seq_ratio * 0.6) + (overlap * 0.4) + token_bonus + availability_bonus + accessory_penalty


def pricespy_currency_symbol(code: Optional[str]) -> str:
    if not code:
        return ""

    mapping = {
        "GBP": "\u00a3",
        "USD": "$",
        "EUR": "\u20ac",
        "INR": "\u20b9",
    }
    return mapping.get(code.upper(), "")


def format_pricespy_currency(amount: Optional[float], currency_code: Optional[str]) -> Optional[str]:
    if amount is None:
        return None
    symbol = pricespy_currency_symbol(currency_code)
    if symbol:
        return f"{symbol}{amount:,.2f}".rstrip("0").rstrip(".")
    if currency_code:
        return f"{amount:,.2f} {currency_code.upper()}".rstrip("0").rstrip(".")
    return f"{amount:,.2f}".rstrip("0").rstrip(".")


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_currency_code(offer: Dict[str, Any]) -> Optional[str]:
    price_obj = offer.get("price") or {}
    store = offer.get("store") or {}

    code = price_obj.get("originalCurrency") or store.get("currency")
    if isinstance(code, str) and code.strip():
        return code.strip().upper()

    primary_market = str(offer.get("primaryMarket") or "").lower()
    country_code = str(store.get("countryCode") or "").lower()
    if primary_market in {"gb", "uk"} or country_code in {"gb", "uk"}:
        return "GBP"
    return None


def select_offer_price(offer: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    price_obj = offer.get("price") or {}
    offer_prices = offer.get("offerPrices") or {}
    live_offer = offer_prices.get("price") or {}
    original = offer_prices.get("originalPrice") or {}
    member = offer_prices.get("memberPrice") or {}

    currency_code = infer_currency_code(offer)

    for candidate in (
        live_offer.get("inclShipping"),
        price_obj.get("inclShipping"),
        member.get("inclShipping"),
        original.get("inclShipping"),
        live_offer.get("exclShipping"),
        price_obj.get("exclShipping"),
    ):
        parsed = coerce_float(candidate)
        if parsed is not None:
            return parsed, currency_code

    return None, currency_code


def shipping_text_from_offer(offer: Dict[str, Any], currency_code: Optional[str]) -> Optional[str]:
    shipping = offer.get("shipping") or {}
    cheapest = shipping.get("cheapest") or {}
    cost = coerce_float(cheapest.get("shippingCost"))
    if cost is None:
        return None
    if cost <= 0:
        return "Free delivery"
    formatted = format_pricespy_currency(cost, currency_code)
    return f"Shipping {formatted}" if formatted else None


def pricespy_search_product_candidates(
    session: requests.Session, product_title: str, limit: int = 50
) -> List[Dict[str, Any]]:
    data = pricespy_graphql_request(
        session=session,
        query=PRICESPY_SEARCH_QUERY,
        variables={
            "query": product_title,
            "offset": 0,
            "limit": limit,
            "allProductsFilter": True,
        },
    )

    products_data = (
        data.get("newSearch", {})
        .get("results", {})
        .get("products", {})
        .get("nodes", [])
    )

    candidates: List[Dict[str, Any]] = []
    for node in products_data:
        if node.get("__typename") != "Product":
            continue
        product_id = node.get("id")
        name = node.get("name")
        if not isinstance(product_id, int) or not isinstance(name, str):
            continue
        price_summary = node.get("priceSummary") or {}
        price_count = price_summary.get("count")
        category_path = ((node.get("category") or {}).get("path") or [])
        category_text = " ".join(
            p.get("name", "") for p in category_path if isinstance(p, dict)
        )
        candidates.append(
            {
                "id": product_id,
                "name": name,
                "path_name": node.get("pathName"),
                "category_text": category_text,
                "price_summary": price_summary,
                "score": score_pricespy_product_match(
                    product_title, name, price_count, category_text=category_text
                ),
            }
        )

    candidates.sort(key=lambda item: item["score"], reverse=True)
    return candidates


def pricespy_suggest_product_candidates(
    session: requests.Session, product_title: str
) -> List[Dict[str, Any]]:
    data = pricespy_graphql_request(
        session=session,
        query=PRICESPY_SUGGESTIONS_QUERY,
        variables={"query": product_title},
    )
    suggestions = data.get("searchSuggestions") or []

    candidates: List[Dict[str, Any]] = []
    for suggestion in suggestions:
        if suggestion.get("__typename") != "SuggestedProduct":
            continue
        product_id_raw = suggestion.get("id")
        name = suggestion.get("text")
        try:
            product_id = int(product_id_raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(name, str):
            continue
        candidates.append(
            {
                "id": product_id,
                "name": name,
                "path_name": None,
                "price_summary": {"regular": suggestion.get("price")},
                "score": score_pricespy_product_match(product_title, name, None),
            }
        )

    candidates.sort(key=lambda item: item["score"], reverse=True)
    return candidates


def pricespy_fetch_product_offers(
    session: requests.Session, product_id: int
) -> Dict[str, Any]:
    data = pricespy_graphql_request(
        session=session,
        query=PRICESPY_PRODUCT_OFFERS_QUERY,
        variables={"id": product_id},
    )
    product = data.get("product")
    if not isinstance(product, dict):
        raise ValueError(f"PriceSpy product not found for id={product_id}")

    prices = product.get("prices") or {}
    nodes = prices.get("nodes") or []
    meta = prices.get("meta") or {}

    offers: List[Dict[str, Any]] = []
    for node in nodes:
        store = node.get("store") or {}
        store_name = store.get("name")
        if not store_name:
            continue

        price_value, currency_code = select_offer_price(node)
        price_text = format_pricespy_currency(price_value, currency_code)
        shipping_text = shipping_text_from_offer(node, currency_code)

        stock = node.get("stock") or {}
        stock_status = stock.get("statusText") or stock.get("status")
        store_logo = store.get("logo")
        if isinstance(store_logo, dict):
            # Defensive fallback in case API returns object shape in future.
            store_logo = store_logo.get("url")
        shop_offer_id = node.get("shopOfferId")
        store_id = store.get("id")
        featured_store = bool(store.get("featured"))

        offers.append(
            {
                "store": store_name,
                "price": price_text,
                "price_value": price_value,
                "currency": currency_code,
                "shipping": shipping_text,
                "comparison": None,
                "icon_alt": store_name,
                "icon_url": store_logo,
                "product_url": node.get("externalUri"),
                "pid": str(shop_offer_id) if shop_offer_id is not None else None,
                "pos": int(store_id) if isinstance(store_id, int) else None,
                "trusted": featured_store,
                "stock_status": stock_status,
                "store_country": store.get("countryCode"),
            }
        )

    comparable = [o["price_value"] for o in offers if o["price_value"] is not None]
    if comparable:
        min_price = min(comparable)
        for offer in offers:
            value = offer.get("price_value")
            if value is None:
                continue
            if value == min_price:
                continue
            pct = round(abs((value - min_price) / min_price) * 100)
            if value < min_price:
                offer["comparison"] = f"{pct}% Cheaper"
            else:
                offer["comparison"] = f"{pct}% Higher"
    offers.sort(key=lambda item: (item["price_value"] is None, item["price_value"]))

    return {
        "product": product,
        "prices_meta": meta,
        "offers": offers,
    }


def normalize_host(host: str) -> str:
    host = host.lower().strip()
    for prefix in ("www.", "www2.", "m.", "in."):
        if host.startswith(prefix):
            host = host[len(prefix) :]
    return host


def decode_js_quoted_string(raw: str) -> str:
    try:
        return json.loads(f"\"{raw}\"")
    except json.JSONDecodeError:
        return raw


def extract_balanced(text: str, start: int, open_char: str, close_char: str) -> Tuple[str, int]:
    if start >= len(text) or text[start] != open_char:
        raise ValueError(f"Expected '{open_char}' at index {start}")

    depth = 0
    in_string: Optional[str] = None
    escaped = False

    for i in range(start, len(text)):
        ch = text[i]

        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_string:
                in_string = None
            continue

        if ch in ('"', "'"):
            in_string = ch
        elif ch == open_char:
            depth += 1
        elif ch == close_char:
            depth -= 1
            if depth == 0:
                return text[start : i + 1], i + 1

    raise ValueError("Could not extract balanced block")


def split_top_level_objects(js_array_text: str) -> List[str]:
    # input like: [{...},{...}]
    objects: List[str] = []
    i = 1
    while i < len(js_array_text) - 1:
        ch = js_array_text[i]
        if ch in {" ", "\n", "\r", "\t", ","}:
            i += 1
            continue
        if ch == "{":
            block, i = extract_balanced(js_array_text, i, "{", "}")
            objects.append(block)
            continue
        i += 1
    return objects


def parse_js_field_string(obj_text: str, key: str) -> Optional[str]:
    match = re.search(rf"{re.escape(key)}\s*:\s*\"((?:\\.|[^\"\\])*)\"", obj_text)
    if not match:
        return None
    return decode_js_quoted_string(match.group(1))


def parse_js_field_number(obj_text: str, key: str) -> Optional[float]:
    match = re.search(rf"{re.escape(key)}\s*:\s*(-?\d+(?:\.\d+)?)", obj_text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def parse_js_field_bool(obj_text: str, key: str) -> Optional[bool]:
    match = re.search(rf"{re.escape(key)}\s*:\s*(true|false)", obj_text)
    if not match:
        return None
    return match.group(1) == "true"


def format_price_value_with_currency(value: Optional[float], currency_symbol: str) -> Optional[str]:
    if value is None:
        return None
    if abs(value - round(value)) < 1e-9:
        amount = f"{int(round(value)):,}"
    else:
        amount = f"{value:,.2f}".rstrip("0").rstrip(".")
    return f"{currency_symbol}{amount}" if currency_symbol else amount


def parse_similar_products(html: str, currency_symbol: str) -> List[Dict[str, Any]]:
    marker = "similarProducts:"
    marker_idx = html.find(marker)
    if marker_idx == -1:
        return []

    array_start = html.find("[", marker_idx + len(marker))
    if array_start == -1:
        return []

    try:
        array_text, _ = extract_balanced(html, array_start, "[", "]")
    except ValueError:
        return []

    objects = split_top_level_objects(array_text)
    products: List[Dict[str, Any]] = []
    max_items = 5
    for obj in objects:
        if len(products) >= max_items:
            break
        name = parse_js_field_string(obj, "name")
        if not name:
            continue

        cur_price = parse_js_field_number(obj, "cur_price")
        last_price = parse_js_field_number(obj, "last_price")
        site_pos = parse_js_field_number(obj, "site_pos")
        internal_pid = parse_js_field_number(obj, "internalPid")
        price_drop_per = parse_js_field_number(obj, "price_drop_per")

        products.append(
            {
                "name": name,
                "site_name": parse_js_field_string(obj, "site_name"),
                "product_url": parse_js_field_string(obj, "link"),
                "image": parse_js_field_string(obj, "image"),
                "site_logo": parse_js_field_string(obj, "site_logo"),
                "site_pos": int(site_pos) if site_pos is not None else None,
                "pid": parse_js_field_string(obj, "pid"),
                "internal_pid": int(internal_pid) if internal_pid is not None else None,
                "current_price_value": cur_price,
                "current_price": format_price_value_with_currency(cur_price, currency_symbol),
                "last_price_value": last_price,
                "last_price": format_price_value_with_currency(last_price, currency_symbol),
                "price_drop_percent": price_drop_per,
                "date": parse_js_field_string(obj, "date"),
            }
        )

    return products


def extract_untrusted_offers(offers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flagged: List[Dict[str, Any]] = []
    for offer in offers:
        icon_url = str(offer.get("icon_url") or "").lower()
        trusted = offer.get("trusted")
        if trusted is False or "lookalike" in icon_url:
            flagged.append(offer)
    return flagged


def build_pricespy_lookalike_products(
    candidates: List[Dict[str, Any]], selected_product_id: Optional[int]
) -> List[Dict[str, Any]]:
    lookalikes: List[Dict[str, Any]] = []
    max_items = 5
    for candidate in candidates:
        if len(lookalikes) >= max_items:
            break
        candidate_id = candidate.get("id")
        if selected_product_id is not None and candidate_id == selected_product_id:
            continue

        path_name = candidate.get("path_name")
        if isinstance(path_name, str) and path_name.startswith("/"):
            product_url = f"{PRICESPY_BASE_URL}{path_name}"
        else:
            product_url = None

        price_summary = candidate.get("price_summary") or {}
        lookalikes.append(
            {
                "id": candidate_id,
                "name": candidate.get("name"),
                "path_name": path_name,
                "product_url": product_url,
                "score": round(float(candidate.get("score", 0.0)), 4),
                "min_price_value": coerce_float(price_summary.get("regular")),
                "alt_price_value": coerce_float(price_summary.get("alternative")),
                "offer_count": (
                    int(price_summary.get("count"))
                    if isinstance(price_summary.get("count"), int)
                    else None
                ),
            }
        )

    return lookalikes


def fetch_pos_map(session: requests.Session) -> Dict[str, int]:
    response = session.get(POS_LIST_API, timeout=20)
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    normalized: Dict[str, int] = {}
    for raw_host, pos in data.items():
        host = raw_host
        if "://" in host:
            host = urlparse(host).netloc or host
        host = host.split("/", 1)[0]
        normalized[normalize_host(host)] = int(pos)
    return normalized


def lookup_pos_for_url(pos_map: Dict[str, int], url: str) -> Optional[int]:
    host = normalize_host(urlparse(url).netloc)
    if not host:
        return None

    candidates = [host]
    parts = host.split(".")
    for i in range(1, len(parts)):
        candidates.append(".".join(parts[i:]))

    for candidate in candidates:
        if candidate in pos_map:
            return pos_map[candidate]

    return None


def candidate_pids_from_url(url: str) -> List[str]:
    parsed = urlparse(url)
    path = parsed.path or ""
    full_url = url
    query = parse_qs(parsed.query)

    candidates: List[str] = []

    # Query params first.
    for key in ("pid", "productId", "product_id", "sku", "skuId", "itemId", "asin", "ASIN"):
        values = query.get(key)
        if values:
            candidates.extend(v for v in values if v)

    # Common Amazon patterns.
    for pattern in (
        r"/dp/([A-Z0-9]{10})",
        r"/gp/product/([A-Z0-9]{10})",
        r"/gp/aw/d/([A-Z0-9]{10})",
        r"/product/([A-Z0-9]{10})",
        r"[?&]asin=([A-Z0-9]{10})",
    ):
        match = re.search(pattern, full_url, re.IGNORECASE)
        if match:
            candidates.append(match.group(1))

    # Generic ID-like tokens in path.
    for token in re.findall(r"[A-Za-z0-9]{8,32}", path):
        candidates.append(token)

    # Deduplicate while preserving order.
    seen = set()
    unique: List[str] = []
    for candidate in candidates:
        val = candidate.strip()
        if not val:
            continue
        key = val.upper()
        if key in seen:
            continue
        seen.add(key)
        unique.append(val)
    return unique


def resolve_redirected_url(
    session: requests.Session,
    product_url: str,
    poll_attempts: int = 10,
    poll_interval_seconds: float = 1.5,
) -> str:
    insert_response = session.post(
        INSERT_REDIRECT_API,
        json={"url": product_url},
        timeout=20,
        headers={"Content-Type": "application/json"},
    )
    insert_response.raise_for_status()
    insert_payload = insert_response.json()
    redirect_id = insert_payload.get("id")

    if not isinstance(redirect_id, int):
        return product_url

    resolved_url = product_url
    for _ in range(poll_attempts):
        poll_response = session.get(
            GET_REDIRECT_API,
            params={"id": redirect_id},
            timeout=20,
        )
        poll_response.raise_for_status()
        poll_payload = poll_response.json()
        status = poll_payload.get("status")

        if status == 1:
            data = poll_payload.get("data") or {}
            redirected = data.get("redirectedURL")
            if isinstance(redirected, str) and redirected.strip():
                resolved_url = redirected.strip()
            break
        if status == -1:
            break

        time.sleep(poll_interval_seconds)

    return resolved_url


def fetch_product_data(session: requests.Session, pos: int, pid: str) -> Optional[Dict[str, Any]]:
    response = session.get(
        PRODUCT_DATA_API,
        params={"pos": pos, "pid": pid},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != 1:
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    return data


def find_working_product_data(
    session: requests.Session,
    pos: int,
    pid_candidates: Iterable[str],
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    for pid in pid_candidates:
        data = fetch_product_data(session, pos, pid)
        if data:
            return pid, data
    return None, None


def extract_currency_symbol(html: str) -> str:
    match = re.search(r'currencySymbol:"((?:\\.|[^"\\])*)"', html)
    if not match:
        return ""
    return decode_js_quoted_string(match.group(1))


def parse_ditto_products(html: str, currency_symbol: str) -> List[Dict[str, Any]]:
    marker = "dittoProducts:"
    marker_idx = html.find(marker)
    if marker_idx == -1:
        return []

    array_start = html.find("[", marker_idx + len(marker))
    if array_start == -1:
        return []

    try:
        array_text, _ = extract_balanced(html, array_start, "[", "]")
    except ValueError:
        return []

    objects = split_top_level_objects(array_text)
    offers: List[Dict[str, Any]] = []

    for obj in objects:
        store = parse_js_field_string(obj, "site_name")
        product_url = parse_js_field_string(obj, "prodUrl")
        icon_url = parse_js_field_string(obj, "site_logo")
        price = parse_js_field_number(obj, "price")
        is_active_num = parse_js_field_number(obj, "isActive")
        oos_num = parse_js_field_number(obj, "oos")
        pid = parse_js_field_string(obj, "pid")
        pos = parse_js_field_number(obj, "pos")

        if not store or price is None:
            continue

        if currency_symbol:
            price_text = f"{currency_symbol}{price:g}"
        else:
            price_text = f"{price:g}"

        offers.append(
            {
                "store": store,
                "price": price_text,
                "price_value": price,
                "shipping": None,
                "comparison": None,
                "icon_alt": store,
                "icon_url": icon_url,
                "product_url": product_url,
                "pid": pid,
                "pos": int(pos) if pos is not None else None,
                "is_active": bool(int(is_active_num)) if is_active_num is not None else None,
                "out_of_stock": bool(int(oos_num)) if oos_num is not None else None,
            }
        )

    in_stock_prices = [
        item["price_value"]
        for item in offers
        if item["price_value"] is not None and not item.get("out_of_stock")
    ]
    if in_stock_prices:
        min_price = min(in_stock_prices)
        for item in offers:
            value = item.get("price_value")
            if value is None or value == min_price:
                continue
            pct = round(((value - min_price) / min_price) * 100)
            if pct > 0:
                item["comparison"] = f"{pct}% Higher"
            elif pct < 0:
                item["comparison"] = f"{abs(pct)}% Lower"
            else:
                item["comparison"] = "Same"

    return offers


def format_price_with_currency(value: float, currency_symbol: str) -> str:
    rounded = int(round(value))
    return f"{currency_symbol}{rounded:,}" if currency_symbol else f"{rounded:,}"


def parse_deals_list_products(
    html: str, currency_symbol: str, base_price: Optional[float]
) -> List[Dict[str, Any]]:
    marker = "dealsList:"
    marker_idx = html.find(marker)
    if marker_idx == -1:
        return []

    array_start = html.find("[", marker_idx + len(marker))
    if array_start == -1:
        return []

    try:
        array_text, _ = extract_balanced(html, array_start, "[", "]")
    except ValueError:
        return []

    objects = split_top_level_objects(array_text)
    offers: List[Dict[str, Any]] = []

    for obj in objects:
        store = parse_js_field_string(obj, "site_name")
        link = parse_js_field_string(obj, "link")
        icon_url = parse_js_field_string(obj, "site_logo")
        price_value = parse_js_field_number(obj, "price")
        trusted = parse_js_field_bool(obj, "trustedFlag")
        pid = parse_js_field_string(obj, "PID")
        pos = parse_js_field_number(obj, "position")

        if not store or price_value is None:
            continue

        comparison = None
        if base_price and base_price > 0 and price_value != base_price:
            pct = round(abs((price_value - base_price) / base_price) * 100)
            comparison = f"{pct}% Cheaper" if price_value < base_price else f"{pct}% Higher"

        offers.append(
            {
                "store": store,
                "price": format_price_with_currency(price_value, currency_symbol),
                "shipping": "Free delivery",
                "comparison": comparison,
                "icon_alt": store,
                "icon_url": icon_url,
                "product_url": link,
                "pid": pid,
                "pos": int(pos) if pos is not None else None,
                "trusted": trusted,
            }
        )

    return offers


def parse_canonical_url(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    canonical_el = soup.select_one("link[rel='canonical']")
    if not canonical_el:
        return None
    href = canonical_el.get("href")
    return href.strip() if isinstance(href, str) else None


def should_try_pricespy_fallback(resolved_url: str, product_data: Optional[Dict[str, Any]]) -> bool:
    host = urlparse(resolved_url).netloc.lower()
    site_name = str((product_data or {}).get("site_name") or "").lower()
    return host.endswith(".co.uk") or "uk" in site_name


def parse_compare_section_from_dom(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    target_section = None
    compare_title = None

    for section in soup.select("section.grid"):
        heading = section.find(
            "p", string=lambda s: bool(s and "Compare" in s and "Available Prices" in s)
        )
        if heading:
            compare_title = heading.get_text(" ", strip=True)
            target_section = section
            break

    if not target_section:
        return {"compare_title": None, "total_available_prices": 0, "offers": []}

    total_available_prices = 0
    match = re.search(r"Compare\s+(\d+)\s+Available Prices", compare_title or "", re.IGNORECASE)
    if match:
        total_available_prices = int(match.group(1))

    offers: List[Dict[str, Any]] = []
    for button in target_section.find_all("button"):
        store_el = button.select_one("p.capitalize")
        price_el = button.select_one("p.font-bold")
        if not store_el or not price_el:
            continue

        store = store_el.get_text(" ", strip=True)
        price_text = price_el.get_text(" ", strip=True)
        if not store or not price_text:
            continue

        shipping_el = button.select_one("p.text-gray-500")
        icon_el = button.find("img")

        comparison_text = None
        for p_tag in button.find_all("p"):
            text = p_tag.get_text(" ", strip=True)
            if re.search(r"(Higher|Lower|Same|Cheaper)", text, re.IGNORECASE):
                comparison_text = text
                break

        offers.append(
            {
                "store": store,
                "price": price_text,
                "shipping": shipping_el.get_text(" ", strip=True) if shipping_el else None,
                "comparison": comparison_text,
                "icon_alt": icon_el.get("alt") if icon_el else store,
                "icon_url": icon_el.get("src") if icon_el else None,
            }
        )

    if total_available_prices == 0:
        total_available_prices = len(offers)

    return {
        "compare_title": compare_title,
        "total_available_prices": total_available_prices,
        "offers": offers,
    }


def scrape_buyhatke_prices(input_url: str) -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Referer": f"{BUYHATKE_BASE_URL}/",
            "Origin": BUYHATKE_BASE_URL,
        }
    )

    # If user already provides a Buyhatke product page URL, use it directly.
    parsed_input = urlparse(input_url)
    is_buyhatke_url = "buyhatke.com" in parsed_input.netloc.lower()

    resolved_url = input_url
    product_data: Optional[Dict[str, Any]] = None
    parsed_pid: Optional[str] = None
    buyhatke_page_url: Optional[str] = input_url if is_buyhatke_url else None

    if not is_buyhatke_url:
        resolved_url = resolve_redirected_url(session, input_url)
        pos_map = fetch_pos_map(session)
        pos = lookup_pos_for_url(pos_map, resolved_url)
        if pos is None:
            raise ValueError(f"Could not map domain to Buyhatke position id: {resolved_url}")

        pid_candidates = candidate_pids_from_url(resolved_url)
        parsed_pid, product_data = find_working_product_data(session, pos, pid_candidates)
        if not product_data:
            raise ValueError(
                "Could not resolve product data from URL. "
                "Try passing a direct Buyhatke product page URL instead."
            )

        internal_pid = product_data.get("internalPid")
        site_pos = product_data.get("site_pos", pos)
        if internal_pid is None:
            raise ValueError("Product data missing internalPid.")

        buyhatke_page_url = f"{BUYHATKE_BASE_URL}/{site_pos}-{internal_pid}"

    if not buyhatke_page_url:
        raise ValueError("Could not determine Buyhatke page URL.")

    page_response = session.get(buyhatke_page_url, timeout=25)
    page_response.raise_for_status()
    page_response.encoding = "utf-8"
    html = page_response.text

    canonical_url = parse_canonical_url(html)
    currency_symbol = extract_currency_symbol(html)
    lookalike_products = parse_similar_products(html=html, currency_symbol=currency_symbol)

    dom_compare = parse_compare_section_from_dom(html)
    offers = dom_compare["offers"]
    compare_title = dom_compare["compare_title"]
    total_available_prices = dom_compare["total_available_prices"]

    if compare_title and total_available_prices > 0 and len(offers) < total_available_prices:
        deals_offers = parse_deals_list_products(
            html=html,
            currency_symbol=currency_symbol,
            base_price=(
                float(product_data["cur_price"])
                if product_data and isinstance(product_data.get("cur_price"), (int, float))
                else None
            ),
        )
        if len(deals_offers) == total_available_prices:
            offers = deals_offers

    if not offers:
        offers = parse_ditto_products(html, currency_symbol=currency_symbol)
        compare_title = f"Compare {len(offers)} Available Prices" if offers else None
        total_available_prices = len(offers)

    if not offers and should_try_pricespy_fallback(resolved_url, product_data):
        fallback_title = str((product_data or {}).get("name") or "").strip()
        if fallback_title:
            try:
                pricespy_result = scrape_pricespy_uk_compare(fallback_title)
                pricespy_result["input_url"] = input_url
                pricespy_result["resolved_url"] = resolved_url
                pricespy_result["buyhatke_page_url"] = buyhatke_page_url
                pricespy_result["canonical_url"] = canonical_url
                pricespy_result["parsed_pid"] = parsed_pid or pricespy_result.get("parsed_pid")
                pricespy_result["buyhatke_product_data"] = product_data
                pricespy_result["fallback_from_buyhatke"] = True
                pricespy_result["buyhatke_lookalike_products_count"] = len(lookalike_products)
                pricespy_result["buyhatke_lookalike_products"] = lookalike_products
                return pricespy_result
            except ValueError:
                pass

    lookalike_offers = extract_untrusted_offers(offers)

    return {
        "input_url": input_url,
        "resolved_url": resolved_url,
        "buyhatke_page_url": buyhatke_page_url,
        "canonical_url": canonical_url,
        "parsed_pid": parsed_pid,
        "currency_symbol": currency_symbol or None,
        "product_data": product_data,
        "compare_title": compare_title,
        "total_available_prices": total_available_prices,
        "offers_found": len(offers),
        "offers": offers,
        "lookalike_products_count": len(lookalike_products),
        "lookalike_products": lookalike_products,
        "lookalike_offers_count": len(lookalike_offers),
        "lookalike_offers": lookalike_offers,
    }


def scrape_pricespy_uk_compare(product_title: str) -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Referer": f"{PRICESPY_BASE_URL}/",
            "Origin": PRICESPY_BASE_URL,
        }
    )

    used_query = product_title
    candidates: List[Dict[str, Any]] = []

    for query_variant in build_pricespy_query_variants(product_title):
        candidates = pricespy_search_product_candidates(session, query_variant)
        if not candidates:
            candidates = pricespy_suggest_product_candidates(session, query_variant)
        if candidates:
            used_query = query_variant
            break

    if not candidates:
        raise ValueError(f"No PriceSpy products found for title: {product_title}")

    evaluated: List[Tuple[float, int, Dict[str, Any], Dict[str, Any]]] = []
    for candidate in candidates[:8]:
        try:
            data = pricespy_fetch_product_offers(session, int(candidate["id"]))
        except ValueError:
            continue
        offers_count = len(data["offers"])
        combined_score = float(candidate["score"]) + min(offers_count, 50) / 100.0
        evaluated.append((combined_score, offers_count, candidate, data))

    if not evaluated:
        raise ValueError("Could not load PriceSpy product details for candidates.")

    # Prefer products with actual offers, then strongest combined relevance+offer depth.
    evaluated.sort(key=lambda item: (item[1] > 0, item[0], item[1]), reverse=True)
    _, _, selected_candidate, selected_data = evaluated[0]

    product = selected_data["product"]
    offers = selected_data["offers"]
    prices_meta = selected_data["prices_meta"]
    selected_product_id = product.get("id") if isinstance(product.get("id"), int) else None
    lookalike_products = build_pricespy_lookalike_products(candidates, selected_product_id)
    lookalike_offers = extract_untrusted_offers(offers)
    path_name = product.get("pathName")
    if isinstance(path_name, str) and path_name.startswith("/"):
        product_page_url = f"{PRICESPY_BASE_URL}{path_name}"
    else:
        product_page_url = None

    compare_title = f"Compare {len(offers)} Available Prices" if offers else None
    parsed_pid = str(product.get("id")) if product.get("id") is not None else None

    currency_code = next((o.get("currency") for o in offers if o.get("currency")), "GBP")
    currency_symbol = pricespy_currency_symbol(currency_code)
    min_offer_price = min(
        (o["price_value"] for o in offers if isinstance(o.get("price_value"), (int, float))),
        default=None,
    )
    price_summary = product.get("priceSummary") or {}
    current_price = min_offer_price
    if current_price is None:
        current_price = coerce_float(price_summary.get("regular"))

    product_data = {
        "name": product.get("name"),
        "image": None,
        "link": product_page_url,
        "cur_price": current_price,
        "site_logo": None,
        "site_pos": None,
        "brand": None,
        "category": (product.get("category") or {}).get("name"),
        "site_name": "PriceSpy UK",
        "inStock": 1 if offers else 0,
        "pid": parsed_pid,
        "internalPid": product.get("id"),
        "thumbnailImages": [],
        "rating": None,
        "ratingCount": None,
        "price_summary": price_summary,
    }

    return {
        # Keep same core schema as Buyhatke response.
        "input_url": product_title,
        "resolved_url": product_page_url,
        "buyhatke_page_url": None,
        "canonical_url": product_page_url,
        "parsed_pid": parsed_pid,
        "currency_symbol": currency_symbol or None,
        "product_data": product_data,
        "compare_title": compare_title,
        "total_available_prices": len(offers),
        "offers_found": len(offers),
        "offers": offers,
        "lookalike_products_count": len(lookalike_products),
        "lookalike_products": lookalike_products,
        "lookalike_offers_count": len(lookalike_offers),
        "lookalike_offers": lookalike_offers,
        # PriceSpy-specific metadata.
        "source": "pricespy.co.uk",
        "input_title": product_title,
        "pricespy_query_used": used_query,
        "pricespy_page_url": product_page_url,
        "selected_product": {
            "id": product.get("id"),
            "name": product.get("name"),
            "path_name": path_name,
            "category": (product.get("category") or {}).get("name"),
            "price_summary": price_summary,
        },
        "search_candidates": [
            {
                "id": candidate["id"],
                "name": candidate["name"],
                "score": round(candidate["score"], 4),
                "path_name": candidate.get("path_name"),
            }
            for candidate in candidates[:10]
        ],
        "prices_meta": prices_meta,
    }


def scrape_auto_compare_by_url(input_url: str) -> Dict[str, Any]:
    def build_pricespy_result(routing: str, buyhatke_error: Optional[str] = None) -> Dict[str, Any]:
        session = requests.Session()
        inferred_title, resolved_url = infer_product_title_from_url(session, input_url)
        result = scrape_pricespy_uk_compare(product_title=inferred_title)
        result["input_url"] = input_url
        result["resolved_url"] = resolved_url
        result["input_title"] = inferred_title
        result["routing"] = routing
        if buyhatke_error:
            result["buyhatke_error"] = buyhatke_error
        return result

    if is_indian_market_url(input_url):
        try:
            result = scrape_buyhatke_prices(input_url=input_url)
            result["routing"] = "buyhatke_india"
            return result
        except ValueError as exc:
            try:
                return build_pricespy_result(
                    routing="buyhatke_india_fallback_pricespy",
                    buyhatke_error=str(exc),
                )
            except ValueError:
                raise exc

    return build_pricespy_result(routing="pricespy_non_india")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Buyhatke + PriceSpy scraper using requests + BeautifulSoup4 (no Selenium)."
    )
    parser.add_argument(
        "--platform",
        choices=["auto", "buyhatke", "pricespy"],
        default="auto",
        help="Data source to use.",
    )
    parser.add_argument(
        "--url",
        help="Product URL (used in auto/buyhatke modes).",
    )
    parser.add_argument(
        "--title",
        help="Product title (for pricespy mode).",
    )
    args = parser.parse_args()

    try:
        if args.platform == "buyhatke":
            input_url = args.url or prompt_product_url()
            result = scrape_buyhatke_prices(input_url=input_url)
        elif args.platform == "pricespy":
            product_title = args.title or prompt_product_title()
            result = scrape_pricespy_uk_compare(product_title=product_title)
        else:
            input_url = args.url or prompt_product_url()
            result = scrape_auto_compare_by_url(input_url=input_url)
    except requests.RequestException as exc:
        print(f"Network/API error: {exc}")
        return
    except ValueError as exc:
        print(f"Error: {exc}")
        return

    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
