#!/usr/bin/env python3
"""
Fetch all public works linked to your ORCID from the OpenAlex API
and write them into _data/*.yml plus _bibliography/papers.bib
for the Jekyll publications page.

Publications-page policy (see also _pages/publications.md and
_plugins/publications-policy.rb): the page lists journal articles and
preprints that have not yet been published in a journal. Every other output is
still fetched and tracked, but marked ``publication_page = {false}``.
"""
import os
import requests
import yaml
import sys
import pathlib
import json
import html
import unicodedata
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
CITATIONS_FILE = OUTPUT_DIR / "citations.yml"
# Hand-maintained overrides; the importer reads this file but never writes it.
POLICY_OVERRIDES_FILE = OUTPUT_DIR / "publication_policy.yml"

# --- CONFIGURATION ---
ORCID_ID = (os.getenv("ORCID_ID") or "0000-0001-9162-262X").strip()
ARXIV_AUTHOR_NAME = (os.getenv("ARXIV_AUTHOR_NAME") or "Attila Cangi").strip()
TIMEOUT = 30
OPENALEX_MAX_PAGES = int(os.getenv("OPENALEX_MAX_PAGES", "0"))
OPENALEX_USER_AGENT = os.getenv("OPENALEX_USER_AGENT")
ORCID_REGEX = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")

KIND_MAP = {
    "journal-article": "article",
    "article": "article",
    "review": "article",
    "letter": "article",
    "erratum": "article",
    "book-chapter": "chapter",
    "book-section": "chapter",
    "book": "book",
    "report": "report",
    "posted-content": "preprint",
    "preprint": "preprint",
    "dissertation": "thesis",
    # OpenAlex calls papers published in conference proceedings
    # "proceedings-article" (older API) and "conference-paper". They are
    # *papers*, not talks: keep them as proceedings so they get an
    # @inproceedings entry with a booktitle.
    "proceedings-article": "proceedings",
    "conference-paper": "proceedings",
    # Meeting abstracts are not peer-reviewed proceedings papers. They are
    # published as talks (@misc with a "Conference abstract" note) so they stay
    # visible without being presented as proceedings.
    "conference-abstract": "talk",
    "other": "other",
    "dataset": "dataset",
    "software": "software",
    "supplementary-materials": "supplementary-materials",
    "peer-review": "peer-review",
    "editorial": "editorial",
    "paratext": "paratext",
}

# --- Publications-page policy -------------------------------------------------
# The publications page lists journal articles and preprints that have not yet
# been published in a journal - nothing else. Conference talks and abstracts,
# conference papers, book chapters, books, technical reports, theses, datasets,
# software, supplementary material, repository-only records, peer-review
# records, editorials and paratext are still fetched and tracked (they remain
# in papers.bib and _data/others.yml) but are marked publication_page=false,
# because none of them is a journal article or an unpublished preprint.
#
# Preprints are shown only while no journal version is known. The check is
# deliberately conservative: a preprint is hidden only on strong evidence -
# a conclusive arXiv journal reference, a publisher DOI on the arXiv record, an
# exact (normalised) DOI/title match with a journal article, or a duplicate of
# an already-listed preprint. Near-matches with reworded titles and ambiguous
# journal references are reported for human review instead of being hidden.
#
# A journal article is listed once. A second record with the same DOI, or a
# DOI-less copy with the same title, first author and year, is a duplicate and
# stays off the page; identical titles with distinct DOIs are different
# publications (an erratum, a corrigendum, a companion paper) and are kept.
#
# A repository deposit (Zenodo, OSTI, figshare, OSF, ... DOI or venue) is not a
# journal article, whatever OpenAlex type it carries: it is tracked as "other".
#
# Unknown OpenAlex types are not journal articles, so they are off the page,
# but they are counted and reported loudly: a new type must never change the
# page without a trace.
PUBLICATION_PAGE_KINDS = {"article", "preprint"}

NON_PUBLICATION_KINDS = {
    "talk",
    "proceedings",
    "chapter",
    "book",
    "report",
    "thesis",
    "other",
    "dataset",
    "software",
    "supplementary-materials",
    "peer-review",
    "editorial",
    "paratext",
}

KNOWN_KINDS = set(KIND_MAP.values()) | {"other", "talk"}

# Human-readable labels for outputs that are not plain journal articles.
KIND_NOTES = {
    "talk": "Conference talk",
    "preprint": "Preprint",
    "chapter": "Book chapter",
    "book": "Book",
    "proceedings": "Conference paper",
    "report": "Technical report",
    "thesis": "Dissertation",
    "other": "Other research output",
    "dataset": "Dataset",
    "software": "Software",
    "supplementary-materials": "Supplementary material",
    "peer-review": "Peer review",
    "editorial": "Editorial",
    "paratext": "Paratext",
}

# Raw OpenAlex types whose note is more specific than the internal kind.
RAW_TYPE_NOTES = {
    "conference-abstract": "Conference abstract",
    "conference-paper": "Conference paper",
    "other": "Other research output",
}

# Venues that publish meeting abstracts or repository records rather than
# peer-reviewed proceedings. Papers announced in these venues are talks.
TALK_VENUE_MARKERS = (
    "bull. am. phys. soc.",
    "aps march meet",
    "aps march meeting",
    "aps div.",
    "aps division",
    "meeting abstracts",
    "meet. abstr.",
)


# Publications highlighted on the homepage (selected_papers in _pages/about.md).
# Tracked by DOI so the flags survive a regeneration of papers.bib.
SELECTED_DOIS = {
    "10.1016/j.cpc.2025.109654",  # MALA, Comput. Phys. Commun. 314, 109654 (2025)
    "10.1038/s43588-024-00707-3",  # Bridging the gap in electronic structure calculations, Nat. Comput. Sci. (2024)
    "10.1073/pnas.2408897121",  # Probing iron in Earth's core, PNAS (2024)
    "10.1088/2632-2153/ad3159",  # Inverting the Kohn-Sham equations, MLST (2024)
    "10.1038/s41524-023-01070-z",  # Predicting electronic structures at any length scale, npj Comput. Mater. (2023)
    "10.25080/majora-212e5952-006",  # atoMEC, SciPy proceedings (2022)
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
    """Normalize title text for resilient publication/preprint comparisons."""
    if not title:
        return None
    cleaned = format_bibtex_value(title)
    if not cleaned:
        return None
    normalized = unicodedata.normalize("NFKD", cleaned)
    normalized = normalized.replace("−", "-").replace("–", "-").replace("—", "-")
    normalized = normalized.replace("’", "'").replace("‘", "'")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return normalize_whitespace(normalized)


def normalize_doi(doi: Optional[str]) -> Optional[str]:
    """Return a canonical DOI string without protocol prefixes."""
    if not doi:
        return None
    normalized = doi.strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
    return normalized


def first_author_surname(author: Optional[str]) -> str:
    """Return the first author's surname, lower-cased, for coarse matching."""
    first = (author or "").split(";")[0].strip()
    if not first:
        return ""
    if "," in first:
        return first.split(",")[0].strip().lower()
    return first.split()[-1].lower()


def has_published_version_doi(record: Dict[str, Any]) -> bool:
    """Return True when an arXiv record carries a publisher DOI.

    Only a DOI owned by a publisher is evidence of a journal version. Repository
    DOIs (Zenodo, OSTI, figshare, OSF, ...) and the arXiv DOI itself say nothing
    about publication, so such records are matched against known published
    titles/DOIs in ``mark_publication_page_records`` instead.
    """
    doi_norm = normalize_doi(record.get("doi"))
    if not doi_norm:
        return False

    journal = (record.get("journal") or "").lower()
    href = (record.get("href") or "").lower()
    is_arxiv_record = "arxiv" in journal or "arxiv.org" in href
    if not is_arxiv_record:
        return False
    # Covers the arXiv DOI and every repository prefix (Zenodo, OSTI, ...).
    return not is_repository_doi(doi_norm)


def load_scholar_citation_index() -> Dict[str, List[Dict[str, Optional[str]]]]:
    """Load Google Scholar IDs from the citations cache, keyed by normalized title."""
    if not CITATIONS_FILE.exists():
        return {}
    try:
        with CITATIONS_FILE.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file) or {}
    except yaml.YAMLError as exc:
        print(f"Warning: Could not parse {CITATIONS_FILE}: {exc}", file=sys.stderr)
        return {}

    papers = data.get("papers") or {}
    index: Dict[str, List[Dict[str, Optional[str]]]] = {}
    for pub_id, info in papers.items():
        title_norm = normalize_title(info.get("title"))
        if not title_norm:
            continue
        year = info.get("year")
        year_str = str(year) if year is not None else None
        index.setdefault(title_norm, []).append({"year": year_str, "id": pub_id})
    return index


def attach_google_scholar_ids(
    records: List[Dict[str, Any]],
    citation_index: Dict[str, List[Dict[str, Optional[str]]]],
) -> int:
    """Attach google_scholar_id fields by matching titles (and years when possible)."""
    if not citation_index:
        return 0
    matched = 0
    for record in records:
        title_norm = normalize_title(record.get("title"))
        if not title_norm:
            continue
        candidates = citation_index.get(title_norm)
        if not candidates:
            continue
        year_value = record.get("year") or (record.get("date") or "")[:4]
        year_str = str(year_value) if year_value else None
        pub_id = None
        if year_str:
            for candidate in candidates:
                if candidate.get("year") == year_str:
                    pub_id = candidate.get("id")
                    break
        if not pub_id and len(candidates) == 1:
            pub_id = candidates[0].get("id")
        if pub_id:
            record["google_scholar_id"] = pub_id
            matched += 1
    return matched


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

    # Authors often record the journal version of a preprint here. It is the
    # strongest signal that the preprint is no longer unpublished.
    journal_ref_element = entry.find("arxiv:journal_ref", ns)
    journal_ref = (
        normalize_whitespace(journal_ref_element.text)
        if journal_ref_element is not None and journal_ref_element.text
        else None
    )

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
        "journal_ref": journal_ref,
    }


def abbreviate_venue(name: Optional[str]) -> Optional[str]:
    """Abbreviate a venue name, falling back to the raw name on failure."""
    if not name:
        return None
    cleaned = normalize_whitespace(strip_tags(name))
    if not cleaned:
        return None
    try:
        abbreviated = abbreviator(cleaned, remove_part=True)
    except Exception:  # pragma: no cover - defensive; pyiso4 can be brittle
        return cleaned
    return normalize_whitespace(abbreviated) if abbreviated else cleaned


def is_talk_venue(venue: Optional[str]) -> bool:
    """Return True for meeting-abstract or repository venues that are not proceedings."""
    if not venue:
        return False
    lowered = venue.lower()
    if lowered.strip() == "aps":
        return True
    return any(marker in lowered for marker in TALK_VENUE_MARKERS)


# Venues that host repository copies or datasets rather than journals.
REPOSITORY_VENUE_MARKERS = (
    "arxiv",
    "zenodo",
    "osti",
    "juser",
    "figshare",
    "rodare",
    "proquest",
)

# Venue words that mark conference proceedings or abstracts.
CONFERENCE_VENUE_MARKERS = (
    "conf",
    "symp",
    "workshop",
    "congress",
    "meeting",
)

# DOI prefixes of repositories and preprint servers. A record carrying one of
# these DOIs is not a journal article, whatever its OpenAlex type says.
REPOSITORY_DOI_PREFIXES = (
    "10.48550/",  # arXiv
    "10.5281/",  # Zenodo
    "10.2172/",  # OSTI
    "10.34734/",  # JuSER
    "10.60893/",  # AIP figshare
    "10.14278/",  # RODARE
    "10.26226/",  # Morressier conference material
    "10.11578/",  # OSTI software
    "10.17605/",  # OSF
    "10.6084/",  # figshare (data)
    "10.5061/",  # Dryad
    "10.17632/",  # Mendeley Data
    "10.7910/",  # Harvard Dataverse
    "10.1594/",  # PANGAEA
)

ARXIV_DOI_PREFIX = "10.48550/arxiv."
ARXIV_VERSION_SUFFIX = re.compile(r"v\d+$")


def is_repository_doi(doi: Optional[str]) -> bool:
    """Return True when the DOI belongs to a repository or preprint server."""
    normalized = normalize_doi(doi)
    return bool(normalized) and normalized.startswith(REPOSITORY_DOI_PREFIXES)


def is_conference_venue(venue: Optional[str]) -> bool:
    """Return True when the venue name looks like a conference or proceedings."""
    if not venue:
        return False
    lowered = venue.lower()
    return any(marker in lowered for marker in CONFERENCE_VENUE_MARKERS)


def is_repository_record(
    venue: Optional[str],
    doi: Optional[str],
    source_type: Optional[str] = None,
) -> bool:
    """Return True when a record is a repository deposit, not a journal article.

    A repository DOI is conclusive: no journal is assigned one of these
    prefixes. A repository venue is conclusive only when the publisher-side
    source type does not identify a journal, so a journal article whose landing
    page is mirrored in a repository is not lost.
    """
    if is_repository_doi(doi):
        return True
    if (source_type or "").strip().lower() == "journal":
        return False
    if not venue:
        return False
    lowered = venue.lower()
    return any(marker in lowered for marker in REPOSITORY_VENUE_MARKERS)


def looks_like_journal_article(
    venue: Optional[str],
    doi: Optional[str],
    source_type: Optional[str] = None,
) -> bool:
    """Conservatively decide whether a record is a *published journal article*.

    OpenAlex types a few published journal papers as ``other``. For those, the
    publisher's own source type (``journal``) is the strongest evidence. Legacy
    bibliography entries carry no source type, so the venue name and a
    non-repository DOI have to stand in for it. Both paths refuse repository
    venues, conference venues, talk venues and repository DOIs: a record that
    cannot be shown to be a journal article stays off the publications page.
    """
    if source_type:
        # The publisher's own source type is authoritative, but a repository
        # DOI still means the record is a repository copy, not the article.
        return source_type.strip().lower() == "journal" and not is_repository_doi(doi)
    if not venue:
        return False
    if is_talk_venue(venue) or is_conference_venue(venue):
        return False
    if any(marker in venue.lower() for marker in REPOSITORY_VENUE_MARKERS):
        return False
    # Without a publisher DOI there is no evidence of a journal version.
    return bool(doi) and not is_repository_doi(doi)


def arxiv_id(record: Dict[str, Any]) -> Optional[str]:
    """Return the arXiv identifier of a record, if it carries one."""
    doi_norm = normalize_doi(record.get("doi")) or ""
    if doi_norm.startswith(ARXIV_DOI_PREFIX):
        return ARXIV_VERSION_SUFFIX.sub("", doi_norm[len(ARXIV_DOI_PREFIX) :].rstrip("/")) or None
    href = record.get("href") or ""
    match = re.search(r"arxiv\.org/(?:abs|pdf)/([^/?#]+)", href, re.IGNORECASE)
    if match:
        return ARXIV_VERSION_SUFFIX.sub("", match.group(1).removesuffix(".pdf")) or None
    return None


def arxiv_ids_from_locations(work: Dict[str, Any]) -> List[str]:
    """Collect arXiv identifiers from every location of an OpenAlex work.

    OpenAlex lists the repository copies of a journal article under
    ``locations``. The arXiv identifier found there is publisher-side evidence
    that a preprint of the same work exists, which lets the importer hide the
    preprint even when the published version was retitled.
    """
    identifiers = set()
    for location in work.get("locations") or []:
        for url in (location.get("landing_page_url"), location.get("pdf_url")):
            match = re.search(r"arxiv\.org/(?:abs|pdf)/([^/?#]+)", url or "", re.IGNORECASE)
            if match:
                identifier = ARXIV_VERSION_SUFFIX.sub(
                    "", match.group(1).removesuffix(".pdf").rstrip("/")
                )
                if identifier:
                    identifiers.add(identifier)
    doi_norm = normalize_doi(work.get("doi")) or ""
    if doi_norm.startswith(ARXIV_DOI_PREFIX):
        identifier = ARXIV_VERSION_SUFFIX.sub(
            "", doi_norm[len(ARXIV_DOI_PREFIX) :].rstrip("/")
        )
        if identifier:
            identifiers.add(identifier)
    return sorted(identifiers)


def load_preprint_overrides(path: Optional[pathlib.Path] = None) -> List[Dict[str, str]]:
    """Load the hand-maintained list of preprints with a known journal version.

    The file (_data/publication_policy.yml) is curated, not generated: it
    documents preprints that can only be linked to their published version
    through arXiv identifiers that older bibliography entries do not carry.
    A missing or unreadable file is not fatal - the importer then relies on
    OpenAlex and the bibliography alone.
    """
    path = path or POLICY_OVERRIDES_FILE
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        print(f"Warning: could not read {path}: {exc}", file=sys.stderr)
        return []
    entries: List[Dict[str, str]] = []
    for entry in data.get("superseded_preprints") or []:
        if not isinstance(entry, dict):
            continue
        parsed = {
            name: str(entry.get(name) or "").strip()
            for name in ("title", "arxiv", "published_as")
        }
        # An override without a title or arXiv id cannot match anything; keep it
        # out of the policy rather than letting it look effective.
        if not parsed["title"] and not parsed["arxiv"]:
            continue
        entries.append(parsed)
    return entries


# Phrases that arXiv authors put in ``arxiv:journal_ref`` to describe a
# *submission*, not a publication. Treating them as proof would hide a preprint
# that has not appeared in a journal.
UNPUBLISHED_JOURNAL_REF_MARKERS = (
    "submitted",
    "in preparation",
    "in progress",
    "under review",
    "in review",
    "to be submitted",
    "unpublished",
    "not published",
    "draft",
    "preprint",
)

JOURNAL_REF_DIGIT = re.compile(r"\d")


def is_conclusive_journal_ref(value: Optional[str]) -> bool:
    """Return True when a journal reference cites a published article.

    ``arxiv:journal_ref`` is author-declared, and authors also use it for
    submission status ("Submitted to Phys. Rev. B", "In preparation"). Such a
    value is not evidence of publication. A reference is only accepted when it
    is not a status note and carries a volume, page, article number or year: a
    bare journal name is too weak to hide a preprint.
    """
    text = normalize_whitespace(value or "").lower()
    if not text:
        return False
    if any(marker in text for marker in UNPUBLISHED_JOURNAL_REF_MARKERS):
        return False
    return bool(JOURNAL_REF_DIGIT.search(text))


def has_published_journal_ref(record: Dict[str, Any]) -> bool:
    """Return True when an arXiv record declares a *published* journal reference.

    Absent for most arXiv records, which is why title/DOI matching remains the
    primary check. Inconclusive values are reported in
    ``mark_publication_page_records`` instead of being hidden silently.
    """
    return is_conclusive_journal_ref(record.get("journal_ref"))


def find_possible_duplicate_preprints(
    records: List[Dict[str, Any]],
    threshold: float = 0.85,
) -> List[Dict[str, Any]]:
    """Report preprints that look like they were published under a new title.

    Reworded titles ("... at the European XFEL" vs "... at the European X-ray
    Free Electron Laser") defeat exact matching. Rather than hiding such a
    preprint on a guess, the importer reports the pair so a human can decide.
    Only pairs that share the first author's surname and are at most two years
    apart are considered, which keeps the report short and actionable.
    """
    import difflib

    articles = [record for record in records if effective_kind(record) == "article"]
    preprints = [record for record in records if effective_kind(record) == "preprint"]
    candidates: List[Dict[str, Any]] = []
    for preprint in preprints:
        preprint_title = normalize_title(preprint.get("title"))
        if not preprint_title:
            continue
        preprint_surname = first_author_surname(preprint.get("author"))
        preprint_year = int(preprint.get("year") or 0)
        best: Optional[Dict[str, Any]] = None
        for article in articles:
            if first_author_surname(article.get("author")) != preprint_surname:
                continue
            article_year = int(article.get("year") or 0)
            if preprint_year and article_year and abs(preprint_year - article_year) > 2:
                continue
            article_title = normalize_title(article.get("title"))
            if not article_title:
                continue
            ratio = difflib.SequenceMatcher(None, preprint_title, article_title).ratio()
            if ratio >= threshold and (best is None or ratio > best["ratio"]):
                best = {
                    "preprint": preprint,
                    "article": article,
                    "ratio": round(ratio, 3),
                }
        if best is not None:
            candidates.append(best)
    return candidates


def merge_arxiv_records(
    records: List[Dict[str, Any]],
    arxiv_records: List[Dict[str, Any]],
) -> int:
    """Append the arXiv records that ``records`` does not already contain.

    Deduplication compares normalised DOIs and titles, first against the
    OpenAlex records and then against arXiv records already accepted. When an
    arXiv record is dropped as a duplicate, its ``journal_ref`` - which only the
    arXiv API provides - is transferred to the retained preprint. Otherwise
    deduplication would discard the only evidence that the preprint has a
    journal version, and the published-version check could never fire.

    Returns the number of records appended.
    """
    by_doi: Dict[str, Dict[str, Any]] = {}
    by_title: Dict[str, Dict[str, Any]] = {}
    for record in records:
        doi = normalize_doi(record.get("doi"))
        if doi:
            by_doi.setdefault(doi, record)
        title = normalize_title(record.get("title"))
        if title:
            by_title.setdefault(title, record)

    added = 0
    for record in arxiv_records:
        doi = normalize_doi(record.get("doi"))
        title = normalize_title(record.get("title"))
        existing = by_doi.get(doi) if doi else None
        if existing is None and title:
            existing = by_title.get(title)
        if existing is not None:
            if effective_kind(existing) == "preprint":
                transfer_journal_ref(existing, record)
            continue
        records.append(record)
        added += 1
        if doi:
            by_doi[doi] = record
        if title:
            by_title[title] = record
    return added


def transfer_journal_ref(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    """Copy a conclusive ``journal_ref`` onto a retained preprint, if needed."""
    source_ref = (source.get("journal_ref") or "").strip()
    if not source_ref:
        return
    target_ref = (target.get("journal_ref") or "").strip()
    if not target_ref or (
        not is_conclusive_journal_ref(target_ref)
        and is_conclusive_journal_ref(source_ref)
    ):
        target["journal_ref"] = source_ref


def classify_and_format_publication(work: Dict[str, Any]) -> Dict[str, Any]:
    """Classify and format a single publication record.

    The returned ``kind`` drives both the BibTeX entry type and the
    publication-page policy. Venues are attached per kind so that a missing
    venue can never produce a dangling "In , year" line.
    """
    authors = "; ".join(
        authorship["author"]["display_name"]
        for authorship in work.get("authorships", [])
        if authorship.get("author") and authorship["author"].get("display_name")
    )
    raw_type = (work.get("type") or "other").strip().lower()
    kind = KIND_MAP.get(raw_type, raw_type or "other")

    primary_location = work.get("primary_location") or {}
    source = primary_location.get("source") or {}
    source_name = normalize_whitespace(source.get("display_name") or "") or None
    source_type = (source.get("type") or "").strip().lower() or None
    journal = abbreviate_venue(source_name)

    # Normalize specific journal abbreviations
    if journal and re.match(r"Phys\. rev\., B\.?/?Physical rev\., B", journal):
        journal = "Phys. Rev. B"
    elif journal and re.match(r"Phys\. rev\., A/?Physical rev\., A", journal):
        journal = "Phys. Rev. A"

    # A proceedings paper announced in a meeting-abstract venue is a talk.
    # Keep genuine proceedings papers as proceedings (with a booktitle).
    if kind in {"article", "proceedings"} and is_talk_venue(journal):
        kind = "talk"
    elif journal and journal.lower().startswith("phdt"):
        # ProQuest's dissertation index is a thesis, not a talk/proceeding.
        kind = "thesis"

    if journal and journal.lower().startswith("arxiv"):
        kind = "preprint"

    # OpenAlex files a few genuinely published journal papers under "other"
    # (for example Nature Computational Science and PRL records). The source
    # type "journal" is the publisher-side evidence; without it these papers
    # would vanish from a page that lists journal articles only.
    if kind == "other" and looks_like_journal_article(
        source_name, work.get("doi"), source_type
    ):
        kind = "article"

    # A repository deposit is not a journal article, whatever OpenAlex's type
    # says. Zenodo, OSTI, figshare and OSF records stay tracked as "other" so
    # they never produce an @article entry or reach the publications page.
    if kind == "article" and is_repository_record(
        source_name, work.get("doi"), source_type
    ):
        kind = "other"

    # Book chapters and proceedings papers need the *container* title. Use the
    # un-abbreviated source name so "In <booktitle>" is readable and accurate.
    booktitle = source_name if kind in {"proceedings", "chapter"} else None

    # Every record carries a note so the page can label non-article outputs
    # without the layout having to guess. Unknown types are labelled explicitly
    # instead of being hidden.
    note = KIND_NOTES.get(kind)
    if raw_type in RAW_TYPE_NOTES and kind == KIND_MAP.get(raw_type):
        note = RAW_TYPE_NOTES[raw_type]
    elif kind not in KNOWN_KINDS:
        note = f"OpenAlex type '{raw_type}' (unmapped)"

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
        "booktitle": booktitle,
        "note": note,
        "source_type": source_type,
        "doi": doi,
        "href": href,
        "pdf": pdf_url,
        "path": doi,
        "kind": kind,
        "journal_ref": work.get("journal_ref"),
        "arxiv_ids": arxiv_ids_from_locations(work),
    }


def looks_like_preprint(record: Dict[str, Any]) -> bool:
    """Return True when a record is an arXiv preprint rather than a journal item."""
    journal = (record.get("journal") or "").lower()
    doi_norm = normalize_doi(record.get("doi")) or ""
    return "arxiv" in journal or doi_norm.startswith(ARXIV_DOI_PREFIX)


def effective_kind(record: Dict[str, Any]) -> str:
    """Resolve the policy-relevant kind of a record.

    Importer records already carry a classified ``kind``. Legacy bibliography
    entries (and records that only have raw BibTeX types) may carry ``other``
    for a genuine journal article or ``article`` for a repository deposit or an
    arXiv preprint, so the same conservative checks used by the classifier are
    applied here as well. The recorded ``kind`` is never rewritten: BibTeX entry
    types in the existing bibliography are kept as they are.
    """
    kind = record.get("kind") or "other"
    if kind == "article":
        # Older imports typed preprints and repository deposits as @article.
        # They are not journal articles and must not be shown as such.
        if looks_like_preprint(record):
            return "preprint"
        if is_repository_record(
            record.get("journal"), record.get("doi"), record.get("source_type")
        ):
            return "other"
        return "article"
    if kind != "other":
        return kind
    if looks_like_preprint(record):
        return "preprint"
    if looks_like_journal_article(record.get("journal"), record.get("doi")):
        return "article"
    return "other"


def article_preference(record: Dict[str, Any], index: int) -> tuple:
    """Rank duplicate candidates; lower is better.

    A record with a DOI carries the publisher's identifier, so it wins over a
    DOI-less copy. The input position breaks ties, keeping the choice
    deterministic for a given import.
    """
    doi = normalize_doi(record.get("doi"))
    journal = (record.get("journal") or "").strip()
    return (0 if doi else 1, 0 if journal else 1, index)


def find_duplicate_articles(
    records: List[Dict[str, Any]],
    kinds: Dict[int, str],
) -> Dict[int, int]:
    """Map duplicate journal-article record ids to the record that is kept.

    A journal article can appear twice in one import (OpenAlex lists the same
    work under two identifiers, or a legacy record is imported again). The page
    must list it once.

    Two articles are the same work when they share a normalised DOI. A title is
    only used as a fallback together with the same first author and year, and
    only when the group carries at most one DOI. Identical titles with distinct
    DOIs are legitimate distinct publications - an erratum, a corrigendum or a
    companion paper - and are never merged.
    """
    indices = [
        index
        for index, record in enumerate(records)
        if kinds[id(record)] == "article"
    ]
    parent = {index: index for index in indices}

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    by_doi: Dict[str, List[int]] = {}
    by_title: Dict[tuple, List[int]] = {}
    for index in indices:
        record = records[index]
        doi = normalize_doi(record.get("doi"))
        if doi:
            by_doi.setdefault(doi, []).append(index)
        title = normalize_title(record.get("title"))
        surname = first_author_surname(record.get("author"))
        year = str(record.get("year") or (record.get("date") or ""))[:4]
        if title and surname and year:
            by_title.setdefault((title, surname, year), []).append(index)

    groups: List[List[int]] = []
    groups.extend(members for members in by_doi.values() if len(members) > 1)
    for members in by_title.values():
        if len(members) < 2:
            continue
        dois = {normalize_doi(records[index].get("doi")) for index in members}
        dois.discard(None)
        if len(dois) > 1:
            continue
        groups.append(members)

    for members in groups:
        for index in members[1:]:
            union(members[0], index)

    components: Dict[int, List[int]] = {}
    for index in indices:
        components.setdefault(find(index), []).append(index)

    duplicates: Dict[int, int] = {}
    for members in components.values():
        if len(members) < 2:
            continue
        kept = min(members, key=lambda index: article_preference(records[index], index))
        for index in members:
            if index != kept:
                duplicates[id(records[index])] = id(records[kept])
    return duplicates


def mark_publication_page_records(records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Mark every record with an explicit publication-page decision.

    The page lists journal articles and preprints that have not been published
    in a journal. Every record receives ``publication_page`` as True or False,
    so the decision is auditable and the layout never has to guess.

    A duplicate journal article is listed once: records sharing a DOI, or a
    DOI-less copy sharing title, first author and year with another article,
    are duplicates (see ``find_duplicate_articles``).

    A preprint is only hidden on strong evidence: a conclusive arXiv journal
    reference, a publisher DOI on the record, an exact normalised DOI/title
    match with a journal article, or an equal DOI/title/arXiv identifier of an
    already listed preprint. Reworded titles and inconclusive journal
    references are reported for review instead of being hidden (see
    ``find_possible_duplicate_preprints`` and ``is_conclusive_journal_ref``).
    """
    kinds = {id(record): effective_kind(record) for record in records}
    duplicate_articles = find_duplicate_articles(records, kinds)
    published_dois = {
        normalize_doi(record.get("doi"))
        for record in records
        if kinds[id(record)] == "article" and record.get("doi")
    }
    published_titles = {
        normalize_title(record.get("title"))
        for record in records
        if kinds[id(record)] == "article" and record.get("title")
    }
    published_arxiv_ids = {
        identifier
        for record in records
        if kinds[id(record)] == "article"
        for identifier in (record.get("arxiv_ids") or [])
    }
    published_arxiv_ids.update(
        identifier
        for record in records
        if kinds[id(record)] == "article"
        for identifier in [arxiv_id(record)]
        if identifier
    )
    overrides = load_preprint_overrides()
    override_arxiv_ids = {entry["arxiv"] for entry in overrides if entry["arxiv"]}
    override_titles = {
        normalize_title(entry["title"]) for entry in overrides if entry["title"]
    }
    seen_dois = set()
    seen_titles = set()
    seen_arxiv_ids = set()
    curated_hidden = 0
    counts: Dict[str, int] = {}

    for record in records:
        record.pop("publication_page", None)
        record.pop("unpublished_preprint", None)
        kind = kinds[id(record)]

        if kind == "article":
            if id(record) in duplicate_articles:
                kept = next(
                    item
                    for item in records
                    if id(item) == duplicate_articles[id(record)]
                )
                print(
                    "Warning: duplicate journal article suppressed: "
                    f"{record.get('title')!r} "
                    f"({normalize_doi(record.get('doi')) or 'no DOI'}) in favour of "
                    f"{kept.get('title')!r} "
                    f"({normalize_doi(kept.get('doi')) or 'no DOI'}).",
                    file=sys.stderr,
                )
                record["publication_page"] = False
                counts["article:duplicate"] = counts.get("article:duplicate", 0) + 1
            else:
                record["publication_page"] = True
                counts["article:on-page"] = counts.get("article:on-page", 0) + 1
            continue

        if kind not in PUBLICATION_PAGE_KINDS:
            record["publication_page"] = False
            bucket = "off-page" if kind in KNOWN_KINDS else "unmapped-off-page"
            counts[f"{kind}:{bucket}"] = counts.get(f"{kind}:{bucket}", 0) + 1
            continue

        doi_norm = normalize_doi(record.get("doi"))
        title_norm = normalize_title(record.get("title"))
        arxiv_candidates = set(record.get("arxiv_ids") or [])
        single_arxiv_id = arxiv_id(record)
        if single_arxiv_id:
            arxiv_candidates.add(single_arxiv_id)

        if has_published_journal_ref(record) or has_published_version_doi(record):
            record["publication_page"] = False
            counts["preprint:superseded"] = counts.get("preprint:superseded", 0) + 1
            continue
        if (arxiv_candidates & override_arxiv_ids) or (
            title_norm and title_norm in override_titles
        ):
            record["publication_page"] = False
            curated_hidden += 1
            counts["preprint:curated-superseded"] = (
                counts.get("preprint:curated-superseded", 0) + 1
            )
            continue
        if doi_norm and doi_norm in published_dois:
            record["publication_page"] = False
            counts["preprint:published"] = counts.get("preprint:published", 0) + 1
            continue
        if title_norm and title_norm in published_titles:
            record["publication_page"] = False
            counts["preprint:published"] = counts.get("preprint:published", 0) + 1
            continue
        # OpenAlex lists the arXiv copy among the locations of the journal
        # article. That identifier is evidence of a published version even
        # when the article and the preprint carry different titles.
        if arxiv_candidates & published_arxiv_ids:
            record["publication_page"] = False
            counts["preprint:published"] = counts.get("preprint:published", 0) + 1
            continue
        if (
            (doi_norm and doi_norm in seen_dois)
            or (title_norm and title_norm in seen_titles)
            or (arxiv_candidates & seen_arxiv_ids)
        ):
            record["publication_page"] = False
            counts["preprint:duplicate"] = counts.get("preprint:duplicate", 0) + 1
            continue

        if doi_norm:
            seen_dois.add(doi_norm)
        if title_norm:
            seen_titles.add(title_norm)
        seen_arxiv_ids.update(arxiv_candidates)
        record["unpublished_preprint"] = True
        record["publication_page"] = True
        counts["preprint:on-page"] = counts.get("preprint:on-page", 0) + 1

    total_on_page = sum(count for key, count in counts.items() if key.endswith(":on-page"))
    total_off_page = sum(count for key, count in counts.items() if not key.endswith(":on-page"))
    print(
        f"Publication policy: {total_on_page} of {len(records)} records on the "
        f"publications page, {total_off_page} tracked but not presented as "
        "publications."
    )
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")
    if curated_hidden:
        print(
            f"  {curated_hidden} preprint(s) hidden by curated overrides in "
            f"{POLICY_OVERRIDES_FILE.relative_to(ROOT_DIR)}."
        )
    unmapped = sorted(
        {str(record.get("kind")) for record in records if record.get("kind") not in KNOWN_KINDS}
    )
    if unmapped:
        print(
            "Warning: unmapped OpenAlex types are not journal articles, so they "
            "are off the publications page: " + ", ".join(unmapped),
            file=sys.stderr,
        )
    for record in records:
        if kinds.get(id(record)) != "preprint" or not record.get("publication_page"):
            continue
        journal_ref = (record.get("journal_ref") or "").strip()
        if journal_ref and not is_conclusive_journal_ref(journal_ref):
            print(
                "Warning: arxiv journal_ref is not conclusive evidence of a "
                f"journal version; kept on the page for review: {journal_ref!r} "
                f"({record.get('title')!r})",
                file=sys.stderr,
            )
    for candidate in find_possible_duplicate_preprints(records):
        preprint = candidate["preprint"]
        article = candidate["article"]
        if not preprint.get("publication_page"):
            continue
        print(
            "Warning: preprint may already be published as a journal article "
            f"({candidate['ratio']} title similarity); kept on the page for "
            f"review: {preprint.get('title')!r} vs {article.get('title')!r}",
            file=sys.stderr,
        )
    return counts


def write_yaml_files(records: List[Dict[str, Any]]) -> None:
    """Sort records and write them to categorized YAML files."""
    records.sort(
        key=lambda record: (record.get("year") or 0, record.get("date") or ""),
        reverse=True,
    )

    articles = [record for record in records if record["kind"] == "article"]
    preprints = [record for record in records if record["kind"] == "preprint"]
    others = [record for record in records if record["kind"] not in ["article", "preprint"]]
    preprints_unpublished = [
        record for record in preprints if record.get("unpublished_preprint")
    ]

    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    header = "# This file is automatically generated. Do not edit manually."

    outputs = [
        (ARTICLES_FILE, articles, "peer-reviewed articles"),
        (PREPRINTS_FILE, preprints, "preprints"),
        (OTHERS_FILE, others, "other publications"),
        (PREPRINTS_UNPUBLISHED_FILE, preprints_unpublished, "unpublished preprints"),
    ]

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


def bibtex_type_for_kind(kind: str) -> str:
    """Map internal kind values to BibTeX entry types.

    Talks deliberately become @misc, not @inproceedings: a conference talk or
    meeting abstract must not be presented as a peer-reviewed proceedings
    paper. Genuine proceedings papers use the separate ``proceedings`` kind.
    """
    return {
        "article": "article",
        "preprint": "misc",
        "talk": "misc",
        "report": "techreport",
        "chapter": "incollection",
        "book": "book",
        "proceedings": "inproceedings",
        "thesis": "phdthesis",
    }.get(kind, "misc")


def bibtex_venue_field(entry_type: str, record: Dict[str, Any]) -> tuple:
    """Return the BibTeX field name and value that carry the venue."""
    if entry_type in {"inproceedings", "incollection"}:
        return "booktitle", record.get("booktitle")
    if entry_type == "techreport":
        return "institution", record.get("journal")
    if entry_type == "book":
        return "publisher", record.get("journal")
    if entry_type in {"phdthesis", "mastersthesis"}:
        return "school", record.get("journal")
    return "journal", record.get("journal")


# Fields that are curated by hand in papers.bib and must survive a nightly
# regeneration. Anything generated (title, venue, doi, ids, flags) is excluded.
PRESERVED_CURATED_FIELDS = (
    "abbr",
    "additional_info",
    "annotation",
    "award",
    "award_name",
    "bibtex_show",
    "blog",
    "code",
    "html",
    "poster",
    "preview",
    "slides",
    "video",
    "website",
)


def _balanced_braced_value(text: str, start: int) -> tuple:
    """Return the braced value beginning at ``start`` and the index after it."""
    depth = 1
    index = start
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:index], index + 1
        index += 1
    return text[start:], len(text)


def parse_bibtex_entries(text: str) -> List[Dict[str, Any]]:
    """Parse BibTeX entries into type/key/fields records.

    This is intentionally small: it only needs to read the bibliography this
    script itself writes (plus hand edits), including nested braces in titles.
    """
    entries: List[Dict[str, Any]] = []
    position = 0
    while True:
        match = re.search(r"@(\w+)\s*\{\s*([^,\s]+)\s*,", text[position:])
        if not match:
            break
        entry_type = match.group(1).lower()
        key = match.group(2)
        cursor = position + match.end()
        fields: Dict[str, str] = {}

        while cursor < len(text):
            while cursor < len(text) and text[cursor] in " \t\r\n,":
                cursor += 1
            if cursor >= len(text) or text[cursor] == "}":
                cursor += 1
                break
            field_match = re.match(r"([A-Za-z][\w-]*)\s*=\s*", text[cursor:])
            if not field_match:
                break
            name = field_match.group(1).lower()
            cursor += field_match.end()

            if cursor < len(text) and text[cursor] == "{":
                value, cursor = _balanced_braced_value(text, cursor + 1)
            elif cursor < len(text) and text[cursor] == '"':
                closing = text.find('"', cursor + 1)
                closing = len(text) if closing == -1 else closing
                value = text[cursor + 1 : closing]
                cursor = closing + 1
            else:
                closing = text.find(",", cursor)
                closing = len(text) if closing == -1 else closing
                value = text[cursor:closing]
                cursor = closing
            fields[name] = normalize_whitespace(value)

        entries.append({"type": entry_type, "key": key, "fields": fields})
        position = cursor
    return entries


def _truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def load_existing_bib_metadata(path: pathlib.Path = BIBLIOGRAPHY_FILE) -> Dict[str, Any]:
    """Index the current bibliography by DOI and title.

    Both indexes keep *every* candidate instead of overwriting on collision:
    a bibliography legitimately contains records that share a normalized title
    (a preprint and its published version, an OSTI report and the journal
    article). Keeping all candidates lets the lookup below refuse ambiguous
    matches rather than letting the last entry silently win and donate its
    ``selected`` flag or curated fields to the wrong record.
    """
    metadata: Dict[str, Any] = {
        "by_doi": {},
        "by_title": {},
        "entries": 0,
        "duplicate_dois": {},
        "duplicate_titles": {},
    }
    if not path.exists():
        return metadata

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"Warning: could not read {path}: {exc}", file=sys.stderr)
        return metadata

    for entry in parse_bibtex_entries(text):
        fields = entry["fields"]
        info = {
            "key": entry["key"],
            "doi": normalize_doi(fields.get("doi")),
            "title": normalize_title(fields.get("title")),
            "selected": _truthy(fields.get("selected")),
            "curated": {
                name: fields[name]
                for name in PRESERVED_CURATED_FIELDS
                if fields.get(name)
            },
        }
        if info["doi"]:
            metadata["by_doi"].setdefault(info["doi"], []).append(info)
        if info["title"]:
            metadata["by_title"].setdefault(info["title"], []).append(info)
        metadata["entries"] += 1

    for index, duplicates in (
        ("by_doi", "duplicate_dois"),
        ("by_title", "duplicate_titles"),
    ):
        metadata[duplicates] = {
            value: [candidate["key"] for candidate in candidates]
            for value, candidates in metadata[index].items()
            if len(candidates) > 1
        }
    return metadata


def lookup_existing_metadata(
    record: Dict[str, Any],
    metadata: Dict[str, Any],
    claimed: Optional[set] = None,
    report: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Find the previous bib entry for a record without guessing.

    DOI wins over title. A title fallback only matches when exactly one
    unclaimed candidate exists *and* it does not carry a different DOI (that
    would be another version of the same work, whose curated fields must not
    leak across). Each previous entry can be claimed once, so a DOI or title
    collision can never hand the same ``selected`` flag or curated fields to
    two records.
    """
    claimed = claimed if claimed is not None else set()

    doi = normalize_doi(record.get("doi"))
    if doi:
        candidates = [
            candidate
            for candidate in metadata["by_doi"].get(doi, [])
            if candidate["key"] not in claimed
        ]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            if report is not None:
                keys = ", ".join(candidate["key"] for candidate in candidates)
                report.append(f"duplicate DOI {doi} in the existing bibliography ({keys}); not reusing metadata")
            return None

    title = normalize_title(record.get("title"))
    if not title:
        return None

    candidates = [
        candidate
        for candidate in metadata["by_title"].get(title, [])
        if candidate["key"] not in claimed
    ]
    if len(candidates) > 1:
        if report is not None:
            keys = ", ".join(candidate["key"] for candidate in candidates)
            report.append(f"ambiguous title match ({keys}); not reusing curated metadata")
        return None
    if not candidates:
        return None

    candidate = candidates[0]
    candidate_doi = candidate.get("doi")
    if doi and candidate_doi and candidate_doi != doi:
        if report is not None:
            report.append(
                f"record {doi} shares a title with {candidate['key']} ({candidate_doi}); "
                "not reusing its curated metadata"
            )
        return None
    return candidate


def write_bibtex_file(records: List[Dict[str, Any]]) -> None:
    """Write all records to a BibTeX file for Jekyll Scholar.

    Manual ``selected`` flags and curated display fields from an existing
    papers.bib are preserved; SELECTED_DOIS only seeds records that were not
    present before, so unselecting a paper is not undone overnight.
    """
    BIBLIOGRAPHY_DIR.mkdir(exist_ok=True, parents=True)
    previous = load_existing_bib_metadata(BIBLIOGRAPHY_FILE)
    for doi, keys in sorted(previous["duplicate_dois"].items()):
        print(
            f"Warning: duplicate DOI {doi} in the existing bibliography "
            f"({', '.join(keys)}); metadata is not reused for these entries.",
            file=sys.stderr,
        )
    for keys in sorted(previous["duplicate_titles"].values(), key=lambda value: value[0]):
        print(
            f"Warning: duplicate title in the existing bibliography "
            f"({', '.join(keys)}); DOI matches still apply, ambiguous title "
            "matches are ignored.",
            file=sys.stderr,
        )
    used_keys: Dict[str, int] = {}
    entries: List[str] = []
    claimed: set = set()
    report: List[str] = []
    matched = 0

    for record in records:
        entry_type = bibtex_type_for_kind(record.get("kind", "misc"))
        key = make_bibtex_key(record, used_keys)
        authors = format_bibtex_value(record.get("author") or "")
        if authors:
            authors = authors.replace("; ", " and ")

        title = format_bibtex_value(record.get("title"))
        year = record.get("year") or (record.get("date") or "")[:4]
        venue_field, venue_value = bibtex_venue_field(entry_type, record)
        venue = format_bibtex_value(venue_value)
        doi = normalize_doi(record.get("doi"))
        url = record.get("href") or record.get("pdf")
        google_scholar_id_value = record.get("google_scholar_id")
        google_scholar_id = str(google_scholar_id_value) if google_scholar_id_value else None
        badge_enabled = "true" if doi else None
        publication_page = "true" if record.get("publication_page") else "false"
        unpublished_preprint = "true" if record.get("unpublished_preprint") else None
        note = record.get("note")
        journal_ref = format_bibtex_value(record.get("journal_ref"))

        prior = lookup_existing_metadata(record, previous, claimed, report)
        if prior is not None:
            # The existing file is the editorial source of truth for selection.
            # Claiming the entry guarantees no other record inherits it.
            claimed.add(prior["key"])
            matched += 1
            selected = prior["selected"]
            curated = prior["curated"]
        else:
            selected = bool(doi and doi in SELECTED_DOIS)
            curated = {}

        fields: Dict[str, Optional[str]] = {
            "title": title,
            "author": authors,
            "year": str(year) if year else None,
            venue_field: venue,
            "doi": doi,
            "url": url,
            "note": note,
            "journal_ref": journal_ref,
            "altmetric": badge_enabled,
            "dimensions": badge_enabled,
            "google_scholar_id": google_scholar_id,
            "publication_page": publication_page,
            "unpublished_preprint": unpublished_preprint,
            "selected": "true" if selected else None,
        }
        for name, value in curated.items():
            fields.setdefault(name, value)

        lines = [f"@{entry_type}{{{key},"]
        for field, value in fields.items():
            if value:
                lines.append(f"  {field} = {{{value}}},")
        lines.append("}")
        entries.append("\n".join(lines))

    header = (
        "% This file is automatically generated. Do not edit manually.\n"
        "% Manual `selected` flags and curated display fields are preserved\n"
        "% when the bibliography is regenerated."
    )
    with BIBLIOGRAPHY_FILE.open("w", encoding="utf-8") as file:
        file.write(header)
        file.write("\n\n")
        file.write("\n\n".join(entries))
        file.write("\n")
    print(f"Wrote {len(entries)} BibTeX entries to {BIBLIOGRAPHY_FILE}")
    for message in sorted(set(report)):
        print(f"Warning: {message}", file=sys.stderr)
    if previous["entries"]:
        print(
            f"Preserved editorial metadata from {previous['entries']} existing entries "
            f"({matched} records matched by DOI/title)."
        )


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

    # Deduplicate arXiv publications against OpenAlex publications. A dropped
    # arXiv record still donates its journal_ref to the retained preprint.
    added = merge_arxiv_records(formatted_publications, arxiv_publications)
    if added:
        print(f"Found {added} new unique publications from arXiv.")

    citation_index = load_scholar_citation_index()
    if citation_index:
        matched = attach_google_scholar_ids(formatted_publications, citation_index)
        print(f"Matched {matched} publications to Google Scholar IDs.")

    mark_publication_page_records(formatted_publications)
    write_yaml_files(formatted_publications)
    write_bibtex_file(formatted_publications)
    articles = [record for record in formatted_publications if record.get("kind") == "article"]
    preprints = [record for record in formatted_publications if record.get("kind") == "preprint"]
    write_json_files(articles, preprints)


if __name__ == "__main__":
    main()
