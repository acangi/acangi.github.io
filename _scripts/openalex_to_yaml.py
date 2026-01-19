#!/usr/bin/env python3
"""
Fetch all public works linked to your ORCID from the OpenAlex API
and write them into _data/*.yml plus _bibliography/papers.bib
for the Jekyll publications page.
"""
import os
import requests
import yaml
import sys
import pathlib
import json
import html
from typing import List, Dict, Any, Optional
from pyiso4.ltwa import Abbreviate
import re
import xml.etree.ElementTree as ET


# Create an abbreviator instance globally
abbreviator = Abbreviate.create()

# --- PATHS ---
ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT_DIR / "_data"
BIBLIOGRAPHY_DIR = ROOT_DIR / "_bibliography"
BIBLIOGRAPHY_FILE = BIBLIOGRAPHY_DIR / "papers.bib"
ARTICLES_FILE = OUTPUT_DIR / "articles.yml"
PREPRINTS_FILE = OUTPUT_DIR / "preprints.yml"
OTHERS_FILE = OUTPUT_DIR / "others.yml"
PREPRINTS_UNPUBLISHED_FILE = OUTPUT_DIR / "preprints-unpublished.yml"
ARTICLES_JSON_FILE = OUTPUT_DIR / "articles.json"
PREPRINTS_JSON_FILE = OUTPUT_DIR / "preprints.json"

# --- CONFIGURATION ---
ORCID_ID = (os.getenv("ORCID_ID") or "0000-0001-9162-262X").strip()
ARXIV_AUTHOR_NAME = os.getenv("ARXIV_AUTHOR_NAME", "Attila Cangi")
TIMEOUT = 30
OPENALEX_MAX_PAGES = int(os.getenv("OPENALEX_MAX_PAGES", "0"))
OPENALEX_USER_AGENT = os.getenv("OPENALEX_USER_AGENT")
ORCID_REGEX = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")

KIND_MAP = {
    "journal-article": "article",
    "article": "article",
    "report": "report",
    "book-chapter": "chapter",
    "book": "book",
    "posted-content": "preprint",
    "preprint": "preprint",
    "proceedings-article": "talk",
}


def normalize_whitespace(value: str) -> str:
    """Collapse repeated whitespace and strip surrounding spaces."""
    return re.sub(r"\s+", " ", value).strip()


def strip_tags(value: str) -> str:
    """Remove simple HTML/MathML tags from a string."""
    return re.sub(r"<[^>]+>", "", value)


def format_bibtex_value(value: Optional[str]) -> Optional[str]:
    """Normalize and sanitize a BibTeX field value."""
    if not value:
        return None
    cleaned = strip_tags(value)
    cleaned = html.unescape(cleaned)
    return normalize_whitespace(cleaned)


def normalize_title(title: Optional[str]) -> Optional[str]:
    """Lower-case and normalize spacing for title comparisons."""
    if not title:
        return None
    return normalize_whitespace(title).lower()


def normalize_doi(doi: Optional[str]) -> Optional[str]:
    """Return a canonical DOI string without protocol prefixes."""
    if not doi:
        return None
    normalized = doi.strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
    return normalized


def fetch_publications(orcid: str) -> List[Dict[str, Any]]:
    """Fetch all public works for a given ORCID from the OpenAlex API."""
    records, seen = [], set()
    base_url = f"https://api.openalex.org/works?filter=author.orcid:{orcid}&per-page=200"
    url = base_url + "&cursor=*"
    print("Fetching publications from OpenAlex...")

    page_count = 0
    seen_cursors = set()
    headers = {"User-Agent": OPENALEX_USER_AGENT} if OPENALEX_USER_AGENT else {}
    while url:
        page_count += 1
        if OPENALEX_MAX_PAGES and page_count > OPENALEX_MAX_PAGES:
            print(f"Reached OPENALEX_MAX_PAGES={OPENALEX_MAX_PAGES}, stopping early.")
            break

        try:
            response = requests.get(url, timeout=TIMEOUT, headers=headers)
            response.raise_for_status()
            page = response.json()

            for work in page.get("results", []):
                if work["id"] in seen:
                    continue
                seen.add(work["id"])
                records.append(work)

            cursor = page.get("meta", {}).get("next_cursor")
            print(f"OpenAlex page {page_count}: {len(records)} records total.")
            if cursor and cursor in seen_cursors:
                print("OpenAlex returned a repeated cursor; stopping to avoid a loop.")
                break
            if cursor:
                seen_cursors.add(cursor)
            url = f"{base_url}&cursor={cursor}" if cursor else None

        except requests.exceptions.RequestException as exc:
            print(f"Error fetching data from OpenAlex: {exc}", file=sys.stderr)
            sys.exit(1)
    print(f"Fetched {len(records)} records from OpenAlex.")
    return records


def fetch_from_arxiv(author_name: str) -> List[Dict[str, Any]]:
    """Fetch preprints for a given author from the arXiv API."""
    if not author_name:
        return []
    author_query = author_name.strip().replace('"', "")
    if not author_query:
        return []

    base_url = "http://export.arxiv.org/api/query"
    params = {
        "search_query": f'au:"{author_query}"',
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "start": 0,
        "max_results": 50,
    }

    print(f"Fetching publications from arXiv for author '{author_name}'...")
    try:
        response = requests.get(base_url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entries = root.findall("atom:entry", ns)

        records = []
        for entry in entries:
            records.append(format_arxiv_entry(entry))

        print(f"Fetched {len(records)} records from arXiv.")
        return records

    except requests.exceptions.RequestException as exc:
        print(f"Error fetching data from arXiv: {exc}", file=sys.stderr)
        return []
    except ET.ParseError as exc:
        print(f"Error parsing arXiv response: {exc}", file=sys.stderr)
        return []


def format_arxiv_entry(entry: ET.Element) -> Dict[str, Any]:
    """Format a single arXiv entry into a publication record."""
    ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}

    authors = []
    for author in entry.findall("atom:author", ns):
        name = author.find("atom:name", ns)
        if name is not None and name.text:
            authors.append(normalize_whitespace(name.text))

    doi_element = entry.find("arxiv:doi", ns)
    doi = doi_element.text if doi_element is not None else None

    if not doi:
        link_doi_element = entry.find('atom:link[@title="doi"]', ns)
        if link_doi_element is not None:
            doi_url = link_doi_element.get("href")
            if doi_url and "doi.org" in doi_url:
                doi = doi_url.split("doi.org/")[-1]

    href = entry.find("atom:id", ns).text
    pdf = None
    if href and "/abs/" in href:
        pdf = href.replace("/abs/", "/pdf/") + ".pdf"

    title_element = entry.find("atom:title", ns)
    title = (
        normalize_whitespace(title_element.text)
        if title_element is not None and title_element.text
        else None
    )

    published_element = entry.find("atom:published", ns)
    published_text = (
        normalize_whitespace(published_element.text)
        if published_element is not None and published_element.text
        else None
    )
    year = int(published_text.split("-")[0]) if published_text else None
    date = published_text.split("T")[0] if published_text and "T" in published_text else published_text

    return {
        "title": title,
        "author": "; ".join(authors),
        "year": year,
        "date": date,
        "journal": "arXiv",
        "doi": doi,
        "href": href,
        "pdf": pdf,
        "path": doi if doi else href,
        "kind": "preprint",
    }


def classify_and_format_publication(work: Dict[str, Any]) -> Dict[str, Any]:
    """Classify and format a single publication record."""
    authors = "; ".join(a["author"]["display_name"] for a in work.get("authorships", []))
    kind = KIND_MAP.get(work.get("type"), work.get("type", "other"))

    primary_location = work.get("primary_location") or {}
    source = primary_location.get("source") or {}
    journal = source.get("display_name")
    if journal:
        journal = abbreviator(journal, remove_part=True)

    # Normalize specific journal abbreviations
    if journal and re.match(r"Phys\. rev\., B\.?/?Physical rev\., B", journal):
        journal = "Phys. Rev. B"
    elif journal and re.match(r"Phys\. rev\., A/?Physical rev\., A", journal):
        journal = "Phys. Rev. A"

    # Reclassify based on journal for specific cases
    if journal and journal.startswith("arXiv"):
        kind = "preprint"
    elif kind == "article" and (
        journal in [
            None,
            "APS",
            "Bull. Am. Phys. Soc.",
            "APS March Meeting Abstracts",
            "APS Div. Plasma Phys. Meet. Abstr.",
            "APS March Meet. Abstr.",
        ]
        or (journal and (journal.startswith("APS Division") or journal.startswith("OSTI") or journal.startswith("PhDT")))
    ):
        kind = "talk"

    doi = work.get("doi")
    if doi and doi.startswith("https://doi.org/"):
        href = doi
    elif doi:
        href = f"https://doi.org/{doi}"
    else:
        href = work.get("id")

    best_oa = work.get("best_oa_location") or {}
    pdf_url = best_oa.get("pdf_url")
    if not pdf_url:
        pdf_url = primary_location.get("pdf_url")
    if not pdf_url:
        open_access = work.get("open_access") or {}
        pdf_url = open_access.get("oa_url")

    return {
        "title": work.get("title"),
        "author": authors,
        "year": work.get("publication_year"),
        "date": work.get("publication_date"),
        "journal": journal,
        "doi": doi,
        "href": href,
        "pdf": pdf_url,
        "path": doi,
        "kind": kind,
    }


def write_yaml_files(
    records: List[Dict[str, Any]],
    unpublished_preprints: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Sort records and write them to categorized YAML files."""
    records.sort(key=lambda record: (record.get("year") or 0, record.get("date") or ""), reverse=True)

    articles = [record for record in records if record["kind"] == "article"]
    preprints = [record for record in records if record["kind"] == "preprint"]
    others = [record for record in records if record["kind"] not in ["article", "preprint"]]

    preprints_unpublished: List[Dict[str, Any]] = []
    if unpublished_preprints:
        article_dois = {
            normalize_doi(record.get("doi"))
            for record in articles
            if record.get("doi")
        }
        article_titles = {
            normalize_title(record.get("title"))
            for record in articles
            if record.get("title")
        }

        seen_unpublished_keys = set()
        for record in unpublished_preprints:
            if record.get("kind") == "article":
                continue

            doi_norm = normalize_doi(record.get("doi"))
            title_norm = normalize_title(record.get("title"))

            if doi_norm:
                key = f"doi:{doi_norm}"
            elif title_norm:
                key = f"title:{title_norm}"
            else:
                key = record.get("href")

            if doi_norm and doi_norm in article_dois:
                continue
            if title_norm and title_norm in article_titles:
                continue
            if key and key in seen_unpublished_keys:
                continue

            seen_unpublished_keys.add(key)
            preprints_unpublished.append(record)

        preprints_unpublished.sort(
            key=lambda record: (record.get("year") or 0, record.get("date") or ""),
            reverse=True,
        )

    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    header = "# This file is automatically generated. Do not edit manually."

    outputs = [
        (ARTICLES_FILE, articles, "peer-reviewed articles"),
        (PREPRINTS_FILE, preprints, "preprints"),
        (OTHERS_FILE, others, "other publications"),
    ]

    if preprints_unpublished:
        outputs.append((PREPRINTS_UNPUBLISHED_FILE, preprints_unpublished, "unpublished preprints"))

    for path, data, name in outputs:
        with path.open("w", encoding="utf-8") as file:
            file.write(header)
            file.write("\n")
            yaml.dump(data, file, allow_unicode=True, sort_keys=False, indent=2)
        print(f"Wrote {len(data)} {name} to {path}")


def write_json_files(articles: List[Dict[str, Any]], preprints: List[Dict[str, Any]]) -> None:
    """Write JSON files for journal articles and preprints."""
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    def serialize(records: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
        payload = []
        for record in records:
            payload.append(
                {
                    "title": record.get("title") or "",
                    "year": record.get("year"),
                    "authors": (record.get("author") or "").replace("; ", ", "),
                    "venue": record.get("journal") or "",
                    "doi": normalize_doi(record.get("doi")) or "",
                    "url": record.get("href") or "",
                    "type": kind,
                    "pdf": record.get("pdf") or "",
                }
            )
        return payload

    articles_payload = serialize(articles, "journal")
    preprints_payload = serialize(preprints, "preprint")

    with ARTICLES_JSON_FILE.open("w", encoding="utf-8") as file:
        json.dump(articles_payload, file, ensure_ascii=True, indent=2)
    print(f"Wrote {len(articles_payload)} journal articles to {ARTICLES_JSON_FILE}")

    with PREPRINTS_JSON_FILE.open("w", encoding="utf-8") as file:
        json.dump(preprints_payload, file, ensure_ascii=True, indent=2)
    print(f"Wrote {len(preprints_payload)} preprints to {PREPRINTS_JSON_FILE}")


def bibtex_type_for_kind(kind: str) -> str:
    """Map internal kind values to BibTeX entry types."""
    return {
        "article": "article",
        "preprint": "misc",
        "report": "techreport",
        "talk": "inproceedings",
        "chapter": "incollection",
        "book": "book",
    }.get(kind, "misc")


def make_bibtex_key(record: Dict[str, Any], used_keys: Dict[str, int]) -> str:
    """Create a stable BibTeX key from author, year, and title."""
    authors = record.get("author") or ""
    first_author = authors.split(";")[0].strip() if authors else "unknown"
    last_name = first_author.split()[-1] if first_author else "unknown"
    year = record.get("year") or (record.get("date") or "")[:4] or "n.d."
    title = record.get("title") or ""
    title = format_bibtex_value(title) or "untitled"
    title_word = re.sub(r"[^A-Za-z0-9]", "", title.split()[0]) if title else "untitled"
    base = f"{last_name}{year}{title_word}"
    key = re.sub(r"[^A-Za-z0-9_-]", "", base)

    count = used_keys.get(key, 0)
    used_keys[key] = count + 1
    if count:
        key = f"{key}{chr(ord('a') + count)}"
    return key


def write_bibtex_file(records: List[Dict[str, Any]]) -> None:
    """Write all records to a BibTeX file for Jekyll Scholar."""
    BIBLIOGRAPHY_DIR.mkdir(exist_ok=True, parents=True)
    used_keys: Dict[str, int] = {}
    entries: List[str] = []

    for record in records:
        entry_type = bibtex_type_for_kind(record.get("kind", "misc"))
        key = make_bibtex_key(record, used_keys)
        authors = format_bibtex_value(record.get("author") or "")
        if authors:
            authors = authors.replace("; ", " and ")

        title = format_bibtex_value(record.get("title"))
        year = record.get("year") or (record.get("date") or "")[:4]
        journal = format_bibtex_value(record.get("journal"))
        doi = normalize_doi(record.get("doi"))
        url = record.get("href") or record.get("pdf")

        fields: Dict[str, Optional[str]] = {
            "title": title,
            "author": authors,
            "year": str(year) if year else None,
            "journal": journal if entry_type == "article" else journal,
            "doi": doi,
            "url": url,
        }

        lines = [f"@{entry_type}{{{key},"]
        for field, value in fields.items():
            if value:
                lines.append(f"  {field} = {{{value}}},")
        lines.append("}")
        entries.append("\n".join(lines))

    header = "% This file is automatically generated. Do not edit manually."
    with BIBLIOGRAPHY_FILE.open("w", encoding="utf-8") as file:
        file.write(header)
        file.write("\n\n")
        file.write("\n\n".join(entries))
        file.write("\n")
    print(f"Wrote {len(entries)} BibTeX entries to {BIBLIOGRAPHY_FILE}")


def main() -> None:
    """Fetch, classify, and write publications."""
    if os.getenv("SKIP_OPENALEX", "").lower() in {"1", "true", "yes"}:
        print("SKIP_OPENALEX is set; skipping OpenAlex/arXiv fetch.")
        return

    if not ORCID_ID or not ORCID_REGEX.match(ORCID_ID):
        print(
            f"Invalid ORCID_ID '{ORCID_ID}'. Set a valid ORCID_ID env var.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Using ORCID_ID={ORCID_ID}")

    # Fetch from OpenAlex
    openalex_publications = fetch_publications(ORCID_ID)
    formatted_publications = [
        classify_and_format_publication(publication)
        for publication in openalex_publications
    ]

    # Fetch from arXiv and combine
    arxiv_publications = fetch_from_arxiv(ARXIV_AUTHOR_NAME)

    # Deduplicate arXiv publications against OpenAlex publications
    openalex_dois = {
        normalize_doi(publication.get("doi"))
        for publication in formatted_publications
        if publication.get("doi")
    }
    openalex_titles = {
        normalize_title(publication.get("title"))
        for publication in formatted_publications
        if publication.get("title")
    }

    unique_arxiv_pubs = []
    for publication in arxiv_publications:
        is_duplicate = False
        doi_norm = normalize_doi(publication.get("doi"))
        title_norm = normalize_title(publication.get("title"))

        if doi_norm and doi_norm in openalex_dois:
            is_duplicate = True
        elif title_norm and title_norm in openalex_titles:
            is_duplicate = True

        if not is_duplicate:
            unique_arxiv_pubs.append(publication)
            if doi_norm:
                openalex_dois.add(doi_norm)
            if title_norm:
                openalex_titles.add(title_norm)

    if unique_arxiv_pubs:
        print(f"Found {len(unique_arxiv_pubs)} new unique publications from arXiv.")
        formatted_publications.extend(unique_arxiv_pubs)

    write_yaml_files(formatted_publications, unpublished_preprints=unique_arxiv_pubs)
    write_bibtex_file(formatted_publications)
    articles = [record for record in formatted_publications if record.get("kind") == "article"]
    preprints = [record for record in formatted_publications if record.get("kind") == "preprint"]
    write_json_files(articles, preprints)


if __name__ == "__main__":
    main()
