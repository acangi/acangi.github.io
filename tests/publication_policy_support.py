"""Shared, read-only helpers for the publication-policy tests.

Nothing here touches the network, the importer pipeline or the real
bibliography on disk. The importer module is imported for its pure functions;
`_scripts/openalex_to_yaml.py` only defines paths and helpers at import time.
"""
import importlib.util
import pathlib
import sys
from typing import Any, Dict, List

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "_scripts" / "openalex_to_yaml.py"
REAL_BIB = REPO_ROOT / "_bibliography" / "papers.bib"

# BibTeX entry types that map one-to-one onto importer kinds. @misc is
# resolved per record below, because the bibliography stores preprints,
# repository records, datasets and talks all as @misc.
TYPE_KIND = {
    "article": "article",
    "inproceedings": "proceedings",
    "incollection": "chapter",
    "techreport": "report",
    "book": "book",
    "phdthesis": "thesis",
}


def load_importer() -> Any:
    spec = importlib.util.spec_from_file_location("openalex_to_yaml", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def bib_kind(entry: Dict[str, Any]) -> str:
    """Return the policy kind of a legacy BibTeX entry.

    @misc covers arXiv preprints, repository records, datasets and talks. An
    arXiv venue or arXiv DOI is the only evidence of a preprint; everything
    else stays ``other`` so the venue-based policy check decides.
    """
    entry_type = entry["type"]
    if entry_type == "misc":
        fields = entry["fields"]
        journal = (fields.get("journal") or "").lower()
        doi = (fields.get("doi") or "").lower()
        if "arxiv" in journal or "10.48550/arxiv" in doi:
            return "preprint"
        return "other"
    return TYPE_KIND.get(entry_type, "other")


def records_from_bib(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Turn parsed BibTeX entries into importer-shaped records (no matching)."""
    records: List[Dict[str, Any]] = []
    for entry in entries:
        fields = entry["fields"]
        records.append(
            {
                "key": entry["key"],
                "title": fields.get("title"),
                "author": (fields.get("author") or "").replace(" and ", "; "),
                "year": fields.get("year"),
                "journal": fields.get("journal"),
                "booktitle": fields.get("booktitle"),
                "note": fields.get("note"),
                "doi": fields.get("doi"),
                "href": fields.get("url"),
                "kind": bib_kind(entry),
            }
        )
    return records


def marked_records(importer: Any, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Run the importer's policy over bibliography entries, quietly."""
    import contextlib
    import io

    records = records_from_bib(entries)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        importer.mark_publication_page_records(records)
    return records
