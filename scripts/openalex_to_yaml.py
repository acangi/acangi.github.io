#!/usr/bin/env python3
"""
Fetch all public works linked to your ORCID from the OpenAlex API
and write them into data/publications.yml for Quarto’s listing page.
"""
import os
import requests
import yaml
import sys
import pathlib
from typing import List, Dict, Any, Optional
from pyiso4.ltwa import Abbreviate
import re
import xml.etree.ElementTree as ET


# Create an abbreviator instance globally
abbreviator = Abbreviate.create()

# --- CONFIGURATION ---
ORCID_ID = os.getenv("ORCID_ID", "0000-0001-9162-262X")
ARXIV_AUTHOR_NAME = os.getenv("ARXIV_AUTHOR_NAME", "Attila Cangi")
OUTPUT_DIR = pathlib.Path("data")
ARTICLES_FILE = OUTPUT_DIR / "articles.yml"
PREPRINTS_FILE = OUTPUT_DIR / "preprints.yml"
OTHERS_FILE = OUTPUT_DIR / "others.yml"
PREPRINTS_UNPUBLISHED_FILE = OUTPUT_DIR / "preprints-unpublished.yml"
TIMEOUT = 30
OPENALEX_MAX_PAGES = int(os.getenv("OPENALEX_MAX_PAGES", "0"))
OPENALEX_USER_AGENT = os.getenv("OPENALEX_USER_AGENT")

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
            normalized = normalized[len(prefix):]
    return normalized

def fetch_publications(orcid: str) -> List[Dict[str, Any]]:
    """Fetches all public works for a given ORCID from the OpenAlex API."""
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

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from OpenAlex: {e}", file=sys.stderr)
            sys.exit(1)
    print(f"Fetched {len(records)} records from OpenAlex.")
    return records

def fetch_from_arxiv(author_name: str) -> List[Dict[str, Any]]:
    """Fetches preprints for a given author from the arXiv API."""
    if not author_name:
        return []
    author_query = author_name.strip().replace('"', '')
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
        
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('atom:entry', ns)
        
        records = []
        for entry in entries:
            records.append(format_arxiv_entry(entry))
        
        print(f"Fetched {len(records)} records from arXiv.")
        return records

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from arXiv: {e}", file=sys.stderr)
        return []
    except ET.ParseError as e:
        print(f"Error parsing arXiv response: {e}", file=sys.stderr)
        return []

def format_arxiv_entry(entry: ET.Element) -> Dict[str, Any]:
    """Formats a single arXiv entry into a publication record."""
    ns = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
    
    authors = []
    for author in entry.findall('atom:author', ns):
        name = author.find('atom:name', ns)
        if name is not None and name.text:
            authors.append(normalize_whitespace(name.text))

    doi_element = entry.find('arxiv:doi', ns)
    doi = doi_element.text if doi_element is not None else None
    
    if not doi:
        link_doi_element = entry.find('atom:link[@title="doi"]', ns)
        if link_doi_element is not None:
            doi_url = link_doi_element.get('href')
            if doi_url and 'doi.org' in doi_url:
                doi = doi_url.split('doi.org/')[-1]

    href = entry.find('atom:id', ns).text
    
    title_element = entry.find('atom:title', ns)
    title = normalize_whitespace(title_element.text) if title_element is not None and title_element.text else None

    published_element = entry.find('atom:published', ns)
    published_text = normalize_whitespace(published_element.text) if published_element is not None and published_element.text else None
    year = int(published_text.split('-')[0]) if published_text else None
    date = published_text.split('T')[0] if published_text and 'T' in published_text else published_text

    return {
        "title": title,
        "author": "; ".join(authors),
        "year": year,
        "date": date,
        "journal": "arXiv",
        "doi": doi,
        "href": href,
        "path": doi if doi else href,
        "kind": "preprint",
    }

def classify_and_format_publication(work: Dict[str, Any]) -> Dict[str, Any]:
    """Classifies and formats a single publication record."""
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
    elif kind == "article" and (journal in [None, "APS", "Bull. Am. Phys. Soc.", "APS March Meeting Abstracts", "APS Div. Plasma Phys. Meet. Abstr.", "APS March Meet. Abstr."] or
                               (journal and (journal.startswith("APS Division") or
                                             journal.startswith("OSTI") or
                                             journal.startswith("PhDT")))):
        kind = "talk"

    doi = work.get("doi")
    if doi and doi.startswith("https://doi.org/"):
        href = doi
    elif doi:
        href = f"https://doi.org/{doi}"
    else:
        href = work.get("id")

    return {
        "title": work.get("title"),
        "author": authors,
        "year": work.get("publication_year"),
        "date": work.get("publication_date"),
        "journal": journal,
        "doi": doi,
        "href": href,
        "path": doi,
        "kind": kind,
    }

def write_yaml_files(records: List[Dict[str, Any]], unpublished_preprints: Optional[List[Dict[str, Any]]] = None):
    """Sorts records and writes them to categorized YAML files."""
    records.sort(key=lambda r: (r.get("year") or 0, r.get("date") or ""), reverse=True)

    articles = [r for r in records if r["kind"] == "article"]
    preprints = [r for r in records if r["kind"] == "preprint"]
    others = [r for r in records if r["kind"] not in ["article", "preprint"]]

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
            key=lambda r: (r.get("year") or 0, r.get("date") or ""),
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
        with path.open("w", encoding="utf-8") as f:
            f.write(header)
            f.write("\n")
            yaml.dump(data, f, allow_unicode=True, sort_keys=False, indent=2)
        print(f"Wrote {len(data)} {name} to {path}")


def main():
    """Main function to fetch, classify, and write publications."""
    # Fetch from OpenAlex
    openalex_publications = fetch_publications(ORCID_ID)
    formatted_publications = [classify_and_format_publication(p) for p in openalex_publications]

    # Fetch from arXiv and combine
    arxiv_publications = fetch_from_arxiv(ARXIV_AUTHOR_NAME)
    
    # Deduplicate arXiv publications against OpenAlex publications
    openalex_dois = {
        normalize_doi(p.get('doi'))
        for p in formatted_publications
        if p.get('doi')
    }
    openalex_titles = {
        normalize_title(p.get('title'))
        for p in formatted_publications
        if p.get('title')
    }
    
    unique_arxiv_pubs = []
    for pub in arxiv_publications:
        is_duplicate = False
        doi_norm = normalize_doi(pub.get("doi"))
        title_norm = normalize_title(pub.get("title"))

        if doi_norm and doi_norm in openalex_dois:
            is_duplicate = True
        elif title_norm and title_norm in openalex_titles:
            is_duplicate = True

        if not is_duplicate:
            unique_arxiv_pubs.append(pub)
            if doi_norm:
                openalex_dois.add(doi_norm)
            if title_norm:
                openalex_titles.add(title_norm)

    if unique_arxiv_pubs:
        print(f"Found {len(unique_arxiv_pubs)} new unique publications from arXiv.")
        formatted_publications.extend(unique_arxiv_pubs)

    write_yaml_files(formatted_publications, unpublished_preprints=unique_arxiv_pubs)

if __name__ == "__main__":
    main()
