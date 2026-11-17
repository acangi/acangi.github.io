"""Tests for the publications-page policy and OpenAlex importer semantics.

The publications page lists *journal articles* and *preprints that have not yet
been published in a journal* - nothing else. Conference talks and abstracts,
conference papers, book chapters, books, technical reports, theses, datasets,
software, supplementary material and repository-only records are still tracked
in the bibliography but carry ``publication_page = {false}``.

Run with:
    python -m unittest discover -s tests -v

These tests never touch the network and never write to the real
_bibliography/papers.bib: the importer's output paths are redirected to a
temporary directory or only pure functions are called.
"""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest
from typing import Any, Optional

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from publication_policy_support import (  # noqa: E402
    REAL_BIB,
    load_importer,
    marked_records,
    records_from_bib,
)

IMPORTER = load_importer()


def work(
    openalex_type,
    *,
    title="A test paper",
    doi=None,
    source=None,
    source_type=None,
    year=2024,
    authors=("Attila Cangi",),
    journal_ref=None,
):
    """Build a minimal OpenAlex work payload."""
    primary_location = {}
    if source is not None:
        source_payload = {"display_name": source}
        if source_type is not None:
            source_payload["type"] = source_type
        primary_location["source"] = source_payload
    return {
        "type": openalex_type,
        "title": title,
        "doi": doi,
        "publication_year": year,
        "publication_date": f"{year}-01-01",
        "primary_location": primary_location,
        "best_oa_location": {},
        "open_access": {},
        "authorships": [{"author": {"display_name": name}} for name in authors],
        "journal_ref": journal_ref,
    }


OSTI = "OSTI OAI (U.S. Department of Energy Office of Scientific and Technical Information)"

# Records from the saved OpenAlex snapshot that the page must no longer show:
# every conference-abstract, conference-paper and "other" record there is a
# talk, proceedings paper or repository-only record, not a journal article.
SNAPSHOT_NON_ARTICLE_RECORDS = [
    ("conference-abstract", "A new approach to density functional theory", "Bulletin of the American Physical Society", None),
    ("conference-abstract", "Collisions and Correlation Effects in Warm Dense Matter", "APS Division of Plasma Physics Meeting Abstracts", None),
    ("conference-abstract", "Modifying the Random Phase Approximation for the Average Atom", "APS", None),
    ("conference-paper", "A machine learning surrogate for density functional theory based on the local density of state.", OSTI, "10.2172/2003393"),
    ("conference-paper", "Deep Spiking Quantum Neural Network for Noisy Image Classification", "International Joint Conference on Neural Networks (IJCNN)", "10.1109/ijcnn54540.2023.10191509"),
    ("conference-paper", "atoMEC: An open-source average-atom Python code", "Proceedings of the Python in Science Conferences", "10.25080/majora-212e5952-006"),
    ("conference-paper", "Towards all-electron treatment in electronic structure machine learning", "JuSER (Forschungszentrum Jülich)", "10.34734/fzj-2026-01614"),
    ("other", "Examining the shock response of iron using molecular-spin dynamics", None, "10.26226/m.62a9d1f09deaf4fdb6b3ff1e"),
    ("other", "Scalable Machine Learning Model for Energy Decomposition Analysis in Aqueous Systems", "AIP Publishing", "10.60893/figshare.jcp.c.8144699"),
]


@contextlib.contextmanager
def sandbox_bib(existing_text: Optional[str] = None):
    """Redirect bibliography output to a temporary dir and yield its path."""
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir = pathlib.Path(tempdir)
        bib = tempdir / "papers.bib"
        if existing_text is not None:
            bib.write_text(existing_text, encoding="utf-8")
        old_dir, old_file = IMPORTER.BIBLIOGRAPHY_DIR, IMPORTER.BIBLIOGRAPHY_FILE
        IMPORTER.BIBLIOGRAPHY_DIR = tempdir
        IMPORTER.BIBLIOGRAPHY_FILE = bib
        try:
            yield bib
        finally:
            IMPORTER.BIBLIOGRAPHY_DIR, IMPORTER.BIBLIOGRAPHY_FILE = old_dir, old_file


def run_writer(records):
    """Run mark + write in a sandbox and return parsed entries and stderr."""
    stdout, stderr = io.StringIO(), io.StringIO()
    with sandbox_bib() as bib:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            IMPORTER.mark_publication_page_records(records)
            IMPORTER.write_bibtex_file(records)
        entries = IMPORTER.parse_bibtex_entries(bib.read_text(encoding="utf-8"))
    return entries, stdout.getvalue(), stderr.getvalue()


class PolicyDeclarationTests(unittest.TestCase):
    def test_page_kinds_are_articles_and_preprints_only(self):
        self.assertEqual(IMPORTER.PUBLICATION_PAGE_KINDS, {"article", "preprint"})

    def test_excluded_kinds_complete_the_known_kinds(self):
        page_kinds = IMPORTER.PUBLICATION_PAGE_KINDS
        excluded = IMPORTER.NON_PUBLICATION_KINDS
        self.assertTrue(page_kinds.isdisjoint(excluded))
        self.assertEqual(page_kinds | excluded, IMPORTER.KNOWN_KINDS)


class JournalArticleTests(unittest.TestCase):
    def test_journal_article_is_on_page_without_a_type_note(self):
        record = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                source="Physical Review Letters",
                source_type="journal",
                doi="10.1103/physrevlett.120.000001",
            )
        )
        self.assertEqual(record["kind"], "article")
        self.assertTrue(record["journal"])
        self.assertIsNone(record["note"])
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertTrue(record["publication_page"])

    def test_journal_article_without_venue_is_safe(self):
        record = IMPORTER.classify_and_format_publication(work("journal-article", source=None))
        self.assertEqual(record["kind"], "article")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertTrue(record["publication_page"])

        with sandbox_bib() as bib:
            with contextlib.redirect_stdout(io.StringIO()):
                IMPORTER.write_bibtex_file([record])
            output = IMPORTER.parse_bibtex_entries(bib.read_text(encoding="utf-8"))
        self.assertNotIn("journal", output[0]["fields"])
        self.assertNotIn("booktitle", output[0]["fields"])

    def test_other_type_with_journal_source_is_rescued_as_article(self):
        """OpenAlex files a few published journal papers under "other"."""
        record = IMPORTER.classify_and_format_publication(
            work(
                "other",
                title="Bridging the gap in electronic structure calculations via machine learning",
                doi="10.1038/s43588-024-00707-3",
                source="Nature Computational Science",
                source_type="journal",
            )
        )
        self.assertEqual(record["kind"], "article")
        self.assertIsNone(record["note"])
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertTrue(record["publication_page"])

    def test_other_type_without_journal_evidence_is_off_page(self):
        record = IMPORTER.classify_and_format_publication(
            work(
                "other",
                title="Scalable Machine Learning Model for Energy Decomposition Analysis",
                doi="10.60893/figshare.jcp.c.8144699",
                source="AIP Publishing",
                source_type="repository",
            )
        )
        self.assertEqual(record["kind"], "other")
        self.assertEqual(record["note"], "Other research output")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertFalse(record["publication_page"])

    def test_conference_paper_in_a_journal_issue_stays_a_journal_article(self):
        """A conference report published in a journal is still an @article."""
        record = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                title="International Conference on Strongly Coupled Coulomb Systems",
                doi="10.1002/ctpp.202300110",
                source="Contributions to Plasma Physics",
                source_type="journal",
            )
        )
        self.assertEqual(record["kind"], "article")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertTrue(record["publication_page"])


class ExcludedOutputTests(unittest.TestCase):
    def assertOffPage(self, record):
        self.assertIsNone(record.get("unpublished_preprint"))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertFalse(record["publication_page"], record["title"])

    def test_proceedings_papers_are_off_page(self):
        for openalex_type in ("proceedings-article", "conference-paper"):
            record = IMPORTER.classify_and_format_publication(
                work(openalex_type, source="International Joint Conference on Neural Networks (IJCNN)")
            )
            self.assertEqual(record["kind"], "proceedings")
            self.assertEqual(record["note"], "Conference paper")
            self.assertOffPage(record)

    def test_conference_abstract_and_meeting_abstract_are_off_page(self):
        abstract = IMPORTER.classify_and_format_publication(
            work("conference-abstract", source="Bulletin of the American Physical Society")
        )
        self.assertEqual(abstract["kind"], "talk")
        self.assertOffPage(abstract)

        meeting = IMPORTER.classify_and_format_publication(
            work("journal-article", source="Bulletin of the American Physical Society")
        )
        self.assertEqual(meeting["kind"], "talk")
        self.assertOffPage(meeting)

    def test_dissertation_venue_is_off_page(self):
        record = IMPORTER.classify_and_format_publication(work("journal-article", source="PhDT"))
        self.assertEqual(record["kind"], "thesis")
        self.assertEqual(IMPORTER.bibtex_type_for_kind(record["kind"]), "phdthesis")
        self.assertOffPage(record)

    def test_datasets_software_and_supplementary_material_are_off_page(self):
        for openalex_type, note in (
            ("dataset", "Dataset"),
            ("software", "Software"),
            ("supplementary-materials", "Supplementary material"),
        ):
            record = IMPORTER.classify_and_format_publication(
                work(openalex_type, source="Zenodo (CERN European Organization for Nuclear Research)")
            )
            self.assertEqual(record["note"], note)
            self.assertOffPage(record)

    def test_peer_review_editorial_and_paratext_are_off_page(self):
        for openalex_type in ("peer-review", "editorial", "paratext"):
            record = IMPORTER.classify_and_format_publication(
                work(openalex_type, source="Some Journal")
            )
            self.assertOffPage(record)

    def test_reports_books_and_chapters_are_off_page(self):
        for openalex_type, kind in (
            ("report", "report"),
            ("book", "book"),
            ("book-chapter", "chapter"),
        ):
            record = IMPORTER.classify_and_format_publication(
                work(openalex_type, source="Some Publisher")
            )
            self.assertEqual(record["kind"], kind)
            self.assertOffPage(record)

    def test_repository_records_are_off_page(self):
        # OSTI "conference paper" records are repository copies of talks.
        record = IMPORTER.classify_and_format_publication(work("conference-paper", source=OSTI))
        self.assertOffPage(record)

    def test_unmapped_type_is_off_page_and_reported(self):
        record = IMPORTER.classify_and_format_publication(
            work("some-new-openalex-type", source="Some Venue")
        )
        self.assertEqual(record["kind"], "some-new-openalex-type")
        self.assertEqual(record["note"], "OpenAlex type 'some-new-openalex-type' (unmapped)")
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            counts = IMPORTER.mark_publication_page_records([record])
        self.assertFalse(record["publication_page"])
        self.assertEqual(counts["some-new-openalex-type:unmapped-off-page"], 1)
        self.assertIn("off the publications page", errors.getvalue())


class PreprintTests(unittest.TestCase):
    def preprint(self, **kwargs):
        kwargs.setdefault("title", "A standalone preprint")
        kwargs.setdefault("doi", "10.48550/arXiv.2401.00002")
        kwargs.setdefault("source", "arXiv (Cornell University)")
        kwargs.setdefault("source_type", "repository")
        return IMPORTER.classify_and_format_publication(work("posted-content", **kwargs))

    def article(self, **kwargs):
        kwargs.setdefault("title", "Same paper")
        kwargs.setdefault("doi", "10.1234/paper")
        kwargs.setdefault("source", "Nature Computational Science")
        kwargs.setdefault("source_type", "journal")
        return IMPORTER.classify_and_format_publication(work("journal-article", **kwargs))

    def test_unpublished_preprint_is_on_page(self):
        preprint = self.preprint()
        self.assertEqual(preprint["kind"], "preprint")
        self.assertEqual(preprint["note"], "Preprint")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertTrue(preprint["publication_page"])
        self.assertTrue(preprint["unpublished_preprint"])

    def test_preprint_with_same_title_as_article_is_hidden(self):
        published = self.article()
        preprint = self.preprint(title="Same paper")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([published, preprint])
        self.assertTrue(published["publication_page"])
        self.assertFalse(preprint["publication_page"])
        self.assertNotIn("unpublished_preprint", preprint)

    def test_preprint_with_same_doi_as_article_is_hidden(self):
        published = self.article(doi="10.1234/paper")
        preprint = self.preprint(doi="10.1234/paper")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([published, preprint])
        self.assertFalse(preprint["publication_page"])

    def test_duplicate_preprints_are_listed_once(self):
        first = self.preprint(title="A preprint", doi="10.48550/arXiv.2401.00001")
        duplicate = self.preprint(title="A preprint", doi="10.48550/arXiv.2401.00001")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([first, duplicate])
        self.assertTrue(first["publication_page"])
        self.assertFalse(duplicate["publication_page"])

    def test_duplicate_arxiv_identifier_alone_is_enough(self):
        first = self.preprint(title="First title", doi="10.48550/arXiv.2401.00001")
        duplicate = self.preprint(title="Reworded title", doi="10.48550/arXiv.2401.00001")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([first, duplicate])
        self.assertTrue(first["publication_page"])
        self.assertFalse(duplicate["publication_page"])

    def test_preprint_with_arxiv_journal_ref_is_hidden(self):
        preprint = self.preprint(journal_ref="Phys. Rev. B 109, L241112 (2024)")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertFalse(preprint["publication_page"])

    def test_preprint_listed_in_article_locations_is_hidden(self):
        """OpenAlex lists the arXiv copy among the journal article's locations."""
        published_work = work(
            "journal-article",
            title="The published, possibly retitled version",
            doi="10.1103/physrevb.109.l241112",
            source="Physical Review B",
            source_type="journal",
        )
        published_work["locations"] = [
            {"landing_page_url": "https://doi.org/10.1103/physrevb.109.l241112"},
            {"landing_page_url": "http://arxiv.org/abs/2501.00001"},
        ]
        published = IMPORTER.classify_and_format_publication(published_work)
        self.assertEqual(published["arxiv_ids"], ["2501.00001"])

        preprint = self.preprint(
            title="A completely different preprint title",
            doi="10.48550/arXiv.2501.00001",
        )
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([published, preprint])
        self.assertFalse(preprint["publication_page"])
        self.assertEqual(counts["preprint:published"], 1)

    def test_arxiv_versions_are_ignored_when_matching(self):
        work_record = work("journal-article", doi="10.1103/x", source="Physical Review B", source_type="journal")
        work_record["locations"] = [{"landing_page_url": "http://arxiv.org/abs/2403.02776v3"}]
        published = IMPORTER.classify_and_format_publication(work_record)
        preprint = self.preprint(doi="10.48550/arXiv.2403.02776")
        preprint["href"] = "http://arxiv.org/abs/2403.02776v2"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([published, preprint])
        self.assertFalse(preprint["publication_page"])

    def test_preprint_matched_through_location_ids_only(self):
        """A preprint record may carry its arXiv id only in `arxiv_ids`."""
        published_work = work(
            "journal-article",
            title="Corrections to Thomas-Fermi densities",
            doi="10.1103/physrevlett.114.050401",
            source="Physical Review Letters",
            source_type="journal",
        )
        published_work["locations"] = [{"landing_page_url": "http://arxiv.org/abs/1409.6802"}]
        published = IMPORTER.classify_and_format_publication(published_work)

        preprint_work = work(
            "posted-content",
            title="Uniform approximations for fermionic densities",
            doi=None,
            source="arXiv",
            source_type="repository",
        )
        preprint_work["locations"] = [{"landing_page_url": "http://arxiv.org/abs/1409.6802"}]
        preprint_work["id"] = "https://openalex.org/W123"
        preprint = IMPORTER.classify_and_format_publication(preprint_work)
        self.assertIsNone(preprint["doi"])

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([published, preprint])
        self.assertFalse(preprint["publication_page"])

    def test_preprint_with_publisher_doi_is_hidden(self):
        preprint = self.preprint(doi="10.1103/physrevb.109.l241112")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertFalse(preprint["publication_page"])

    def test_repository_preprint_is_not_assumed_published(self):
        preprint = IMPORTER.classify_and_format_publication(
            work(
                "posted-content",
                title="A repository preprint",
                doi="10.60893/figshare.jcp.c.1234567",
                source="AIP Publ.",
                source_type="repository",
            )
        )
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertTrue(preprint["publication_page"])


class PreprintVersionEvidenceTests(unittest.TestCase):
    """Only a *publisher* DOI proves that a preprint has a journal version."""

    def test_has_published_version_doi_rejects_repository_dois(self):
        for doi in (
            "10.5281/zenodo.12345",  # Zenodo
            "10.2172/2003393",  # OSTI
            "10.60893/figshare.jcp.c.8144699",  # AIP figshare
            "10.17605/OSF.IO/ABC12",  # OSF
            "10.48550/arXiv.2401.00001",  # arXiv itself
        ):
            record = IMPORTER.classify_and_format_publication(
                work(
                    "posted-content",
                    source="arXiv (Cornell University)",
                    source_type="repository",
                    doi=doi,
                )
            )
            self.assertFalse(
                IMPORTER.has_published_version_doi(record), doi
            )

    def test_has_published_version_doi_accepts_a_publisher_doi(self):
        record = IMPORTER.classify_and_format_publication(
            work(
                "posted-content",
                source="arXiv (Cornell University)",
                source_type="repository",
                doi="10.1103/physrevb.109.l241112",
            )
        )
        self.assertTrue(IMPORTER.has_published_version_doi(record))

    def test_zenodo_doi_does_not_hide_an_unpublished_preprint(self):
        """A repository DOI is not evidence of a journal version."""
        preprint = IMPORTER.classify_and_format_publication(
            work(
                "posted-content",
                title="A preprint with a data DOI",
                source="arXiv (Cornell University)",
                source_type="repository",
                doi="10.5281/zenodo.12345",
            )
        )
        self.assertEqual(preprint["kind"], "preprint")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertTrue(preprint["publication_page"])
        self.assertTrue(preprint["unpublished_preprint"])

    def test_osti_doi_does_not_hide_an_unpublished_preprint(self):
        preprint = IMPORTER.classify_and_format_publication(
            work(
                "posted-content",
                title="A preprint with an OSTI DOI",
                source="arXiv",
                source_type="repository",
                doi="10.2172/2003393",
            )
        )
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertTrue(preprint["publication_page"])


class DuplicateArticleTests(unittest.TestCase):
    """The importer must not list the same journal article twice."""

    def article(self, **kwargs):
        kwargs.setdefault("title", "A journal article")
        kwargs.setdefault("doi", "10.1103/physrevb.100.000001")
        kwargs.setdefault("source", "Physical Review B")
        kwargs.setdefault("source_type", "journal")
        return IMPORTER.classify_and_format_publication(
            work("journal-article", **kwargs)
        )

    def test_articles_with_the_same_doi_are_listed_once(self):
        first = self.article(title="First title")
        duplicate = self.article(title="Retitled duplicate record")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([first, duplicate])
        self.assertTrue(first["publication_page"])
        self.assertFalse(duplicate["publication_page"])
        self.assertEqual(counts.get("article:duplicate"), 1)
        self.assertEqual(counts.get("article:on-page"), 1)

    def test_doi_less_copy_of_an_article_is_listed_once(self):
        with_doi = self.article(title="A journal article")
        without_doi = self.article(title="A journal article", doi=None)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([with_doi, without_doi])
        self.assertTrue(with_doi["publication_page"])
        self.assertFalse(without_doi["publication_page"])
        self.assertEqual(counts.get("article:duplicate"), 1)

    def test_the_record_with_a_doi_is_preferred_regardless_of_order(self):
        without_doi = self.article(title="A journal article", doi=None)
        with_doi = self.article(title="A journal article")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([without_doi, with_doi])
        self.assertFalse(without_doi["publication_page"])
        self.assertTrue(with_doi["publication_page"])

    def test_same_title_with_distinct_dois_keeps_both(self):
        """Errata, corrigenda and companion papers share a title legitimately."""
        original = self.article(doi="10.1103/physrevb.100.000001")
        companion = self.article(doi="10.1103/physrevb.100.000002")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([original, companion])
        self.assertTrue(original["publication_page"])
        self.assertTrue(companion["publication_page"])
        self.assertNotIn("article:duplicate", counts)

    def test_duplicate_groups_are_merged_transitively(self):
        """A DOI duplicate and a separately titled DOI-less copy are one work."""
        first = self.article(title="Shared title")
        retitled = self.article(title="Retitled record with the same DOI")
        without_doi = self.article(title="Shared title", doi=None)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records(
                [first, retitled, without_doi]
            )
        self.assertTrue(first["publication_page"])
        self.assertFalse(retitled["publication_page"])
        self.assertFalse(without_doi["publication_page"])
        self.assertEqual(counts.get("article:duplicate"), 2)

    def test_same_title_without_dois_needs_the_same_first_author(self):
        first = self.article(doi=None, authors=("A. Someone",))
        second = self.article(doi=None, authors=("B. Other",))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([first, second])
        self.assertTrue(first["publication_page"])
        self.assertTrue(second["publication_page"])
        self.assertNotIn("article:duplicate", counts)

    def test_same_title_without_dois_needs_the_same_year(self):
        first = self.article(doi=None, year=2024)
        second = self.article(doi=None, year=2023)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([first, second])
        self.assertTrue(first["publication_page"])
        self.assertTrue(second["publication_page"])
        self.assertNotIn("article:duplicate", counts)

    def test_duplicate_warning_names_the_suppressed_record(self):
        first = self.article(title="Shared title")
        duplicate = self.article(title="Shared title")
        errors = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
            IMPORTER.mark_publication_page_records([first, duplicate])
        self.assertIn("duplicate journal article", errors.getvalue())


class RepositoryArticleTests(unittest.TestCase):
    """A repository deposit is tracked, but it is not a journal article."""

    def test_repository_typed_journal_article_is_not_an_article(self):
        record = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                title="A Zenodo deposit mislabelled as a journal article",
                doi="10.5281/zenodo.12345",
                source="Zenodo (CERN European Organization for Nuclear Research)",
                source_type="repository",
            )
        )
        self.assertEqual(record["kind"], "other")
        self.assertEqual(record["note"], "Other research output")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([record])
        self.assertFalse(record["publication_page"])
        self.assertEqual(counts.get("other:off-page"), 1)

    def test_repository_venue_alone_is_not_an_article(self):
        record = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                title="An OSTI repository record",
                doi=None,
                source="OSTI OAI (U.S. Department of Energy Office of Scientific and Technical Information)",
                source_type="repository",
            )
        )
        self.assertEqual(record["kind"], "other")

    def test_journal_article_mirrored_in_a_repository_stays_an_article(self):
        """The publisher's source type wins over a repository landing page."""
        record = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                title="A journal article with a repository landing page",
                doi="10.1103/physrevb.100.000001",
                source="Zenodo (CERN European Organization for Nuclear Research)",
                source_type="journal",
            )
        )
        self.assertEqual(record["kind"], "article")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertTrue(record["publication_page"])

    def test_legacy_article_with_a_repository_doi_is_off_page(self):
        record = {
            "key": "Legacy2020Deposit",
            "title": "A repository deposit typed as an article",
            "journal": "Zenodo",
            "doi": "10.5281/zenodo.12345",
            "kind": "article",
            "year": "2020",
        }
        self.assertEqual(IMPORTER.effective_kind(record), "other")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertFalse(record["publication_page"])

    def test_legacy_article_at_an_arxiv_venue_is_a_preprint(self):
        record = {
            "key": "Legacy2024Preprint",
            "title": "An arXiv preprint typed as an article",
            "journal": "arXiv",
            "href": "http://arxiv.org/abs/2401.00001v1",
            "kind": "article",
            "year": "2024",
        }
        self.assertEqual(IMPORTER.effective_kind(record), "preprint")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertTrue(record["publication_page"])
        self.assertTrue(record["unpublished_preprint"])

    def test_legacy_article_preprint_still_honours_a_curated_override(self):
        record = {
            "key": "Gawne2024Ultrahighb",
            "title": "Ultrahigh Resolution X-ray Thomson Scattering Measurements at the European XFEL",
            "journal": "arXiv",
            "href": "http://arxiv.org/abs/2403.02776v3",
            "kind": "article",
            "year": "2024",
        }
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([record])
        self.assertFalse(record["publication_page"])


class JournalRefEvidenceTests(unittest.TestCase):
    """arxiv:journal_ref is evidence only when it cites a real publication."""

    def preprint(self, journal_ref):
        return IMPORTER.classify_and_format_publication(
            work(
                "posted-content",
                title="A preprint with a journal reference",
                source="arXiv",
                source_type="repository",
                doi="10.48550/arXiv.2401.00001",
                journal_ref=journal_ref,
            )
        )

    def visible(self, preprint):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([preprint])
        return preprint["publication_page"]

    def test_submitted_journal_ref_does_not_hide_the_preprint(self):
        self.assertTrue(self.visible(self.preprint("Submitted to Phys. Rev. B")))

    def test_in_preparation_journal_ref_does_not_hide_the_preprint(self):
        self.assertTrue(self.visible(self.preprint("In preparation")))

    def test_bare_venue_journal_ref_does_not_hide_the_preprint(self):
        self.assertTrue(self.visible(self.preprint("Phys. Rev. B")))

    def test_bare_journal_ref_is_reported_for_review(self):
        preprint = self.preprint("Phys. Rev. B")
        errors = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
            IMPORTER.mark_publication_page_records([preprint])
        self.assertIn("journal_ref", errors.getvalue())
        self.assertIn("kept on the page for review", errors.getvalue())

    def test_conclusive_journal_ref_still_hides_the_preprint(self):
        self.assertFalse(self.visible(self.preprint("Phys. Rev. B 109, L241112 (2024)")))

    def test_journal_ref_is_written_to_the_bibliography(self):
        record = self.preprint("Phys. Rev. B 109, L241112 (2024)")
        entries, _, _ = run_writer([record])
        self.assertEqual(
            entries[0]["fields"].get("journal_ref"),
            "Phys. Rev. B 109, L241112 (2024)",
        )


class ArxivMergeTests(unittest.TestCase):
    """Deduplication must not throw away arXiv-only publication evidence."""

    def preprint(self, **kwargs):
        kwargs.setdefault("title", "A preprint")
        kwargs.setdefault("doi", "10.48550/arXiv.2401.00009")
        kwargs.setdefault("source", "arXiv")
        kwargs.setdefault("source_type", "repository")
        return IMPORTER.classify_and_format_publication(
            work("posted-content", **kwargs)
        )

    def test_journal_ref_survives_doi_deduplication(self):
        openalex = self.preprint()
        arxiv = self.preprint(journal_ref="Phys. Rev. B 109, L241112 (2024)")

        self.assertEqual(IMPORTER.merge_arxiv_records([openalex], [arxiv]), 0)
        self.assertEqual(
            openalex.get("journal_ref"), "Phys. Rev. B 109, L241112 (2024)"
        )
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records([openalex])
        self.assertFalse(openalex["publication_page"])

    def test_journal_ref_survives_title_deduplication(self):
        openalex = self.preprint(doi=None)
        arxiv = self.preprint(doi=None, journal_ref="Phys. Rev. B 109, 1 (2024)")

        self.assertEqual(IMPORTER.merge_arxiv_records([openalex], [arxiv]), 0)
        self.assertEqual(openalex.get("journal_ref"), "Phys. Rev. B 109, 1 (2024)")

    def test_conclusive_journal_ref_replaces_an_inconclusive_one(self):
        openalex = self.preprint(journal_ref="Submitted to Phys. Rev. B")
        arxiv = self.preprint(journal_ref="Phys. Rev. B 109, 1 (2024)")

        IMPORTER.merge_arxiv_records([openalex], [arxiv])
        self.assertEqual(openalex.get("journal_ref"), "Phys. Rev. B 109, 1 (2024)")

    def test_arxiv_record_without_duplicate_is_appended(self):
        existing = [self.preprint(title="First preprint")]
        new = self.preprint(title="Second preprint", doi="10.48550/arXiv.2401.00010")

        self.assertEqual(IMPORTER.merge_arxiv_records(existing, [new]), 1)
        self.assertEqual(len(existing), 2)
        self.assertIs(existing[1], new)

    def test_journal_ref_is_not_copied_onto_a_journal_article(self):
        published = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                title="Same title",
                doi="10.1103/physrevb.100.000001",
                source="Physical Review B",
                source_type="journal",
            )
        )
        arxiv = self.preprint(
            title="Same title", journal_ref="Phys. Rev. B 109, 1 (2024)"
        )

        self.assertEqual(IMPORTER.merge_arxiv_records([published], [arxiv]), 0)
        self.assertIsNone(published.get("journal_ref"))


class SnapshotRegressionTests(unittest.TestCase):
    """Snapshot records that used to be shown must now be excluded by policy."""

    def build_records(self):
        return [
            IMPORTER.classify_and_format_publication(
                work(openalex_type, title=title, source=source, doi=doi)
            )
            for openalex_type, title, source, doi in SNAPSHOT_NON_ARTICLE_RECORDS
        ]

    def test_all_snapshot_talks_and_proceedings_are_off_page(self):
        records = self.build_records()
        self.assertEqual(len(records), 9)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records(records)
        self.assertEqual(
            [record["title"] for record in records if record["publication_page"]], []
        )
        self.assertEqual(counts.get("proceedings:off-page"), 4)
        self.assertEqual(counts.get("talk:off-page"), 3)
        self.assertEqual(counts.get("other:off-page"), 2)

    def test_snapshot_records_keep_honest_bibtex_types(self):
        records = self.build_records()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            IMPORTER.mark_publication_page_records(records)
        by_type = {}
        for record in records:
            by_type.setdefault(record["kind"], set()).add(
                IMPORTER.bibtex_type_for_kind(record["kind"])
            )
        self.assertEqual(by_type["proceedings"], {"inproceedings"})
        self.assertEqual(by_type["talk"], {"misc"})
        self.assertEqual(by_type["other"], {"misc"})
        for record in records:
            if record["kind"] == "proceedings":
                self.assertTrue(record["booktitle"], record["title"])
                self.assertEqual(record["note"], "Conference paper")


class ExistingBibliographyTests(unittest.TestCase):
    def setUp(self):
        self.entries = IMPORTER.parse_bibtex_entries(REAL_BIB.read_text(encoding="utf-8"))

    def test_real_bib_parses_completely(self):
        self.assertEqual(len(self.entries), 125)
        self.assertTrue(all(entry["fields"].get("title") for entry in self.entries))

    def test_metadata_index_reads_selection_and_curated_fields(self):
        metadata = IMPORTER.load_existing_bib_metadata(REAL_BIB)
        self.assertEqual(metadata["entries"], 125)
        self.assertTrue(metadata["by_doi"])
        self.assertTrue(metadata["by_title"])
        selected = sum(
            1
            for candidates in metadata["by_doi"].values()
            for info in candidates
            if info["selected"]
        )
        self.assertEqual(selected, 6)
        self.assertEqual(
            sum(
                len(info["curated"])
                for candidates in metadata["by_title"].values()
                for info in candidates
            ),
            0,
        )

    def test_real_bib_duplicate_titles_are_detected_not_overwritten(self):
        metadata = IMPORTER.load_existing_bib_metadata(REAL_BIB)
        self.assertEqual(metadata["duplicate_dois"], {})
        self.assertEqual(len(metadata["duplicate_titles"]), 6)
        for candidates in metadata["by_doi"].values():
            self.assertEqual(len(candidates), 1)
        self.assertEqual(
            sum(len(candidates) for candidates in metadata["by_title"].values()),
            125,
        )

    def test_policy_on_real_bibliography_keeps_only_articles_and_preprints(self):
        """62 of 125 entries belong on the page; nothing else does."""
        records = marked_records(IMPORTER, self.entries)
        on_page = [record for record in records if record["publication_page"]]

        self.assertEqual(len(on_page), 62)
        self.assertEqual(len([r for r in records if not r["publication_page"]]), 63)

        kinds = IMPORTER.effective_kind
        self.assertEqual(
            len([r for r in on_page if kinds(r) == "article"]), 54
        )
        self.assertEqual(
            len([r for r in on_page if kinds(r) == "preprint"]), 8
        )
        self.assertEqual(
            {kinds(r) for r in on_page}, {"article", "preprint"}
        )

    def test_curated_overrides_match_exactly_one_real_preprint(self):
        overrides = IMPORTER.load_preprint_overrides()
        self.assertEqual(len(overrides), 5)
        records = marked_records(IMPORTER, self.entries)
        preprints = [
            record
            for record in records
            if IMPORTER.effective_kind(record) == "preprint"
        ]
        for override in overrides:
            matched = [
                record
                for record in preprints
                if (override["arxiv"] and IMPORTER.arxiv_id(record) == override["arxiv"])
                or (
                    override["title"]
                    and IMPORTER.normalize_title(record["title"])
                    == IMPORTER.normalize_title(override["title"])
                )
            ]
            self.assertEqual(len(matched), 1, override)
            self.assertFalse(matched[0]["publication_page"], override["title"])
            self.assertTrue(
                override["published_as"], "each override names the journal version"
            )

    def test_real_bibliography_excludes_talks_reports_and_datasets(self):
        records = {r["key"]: r for r in marked_records(IMPORTER, self.entries)}
        for key in (
            "Cangi2021Firstprinciples",  # OSTI conference record
            "Tranchida2019Magnetostriction",  # APS meeting abstract
            "States2018Summer",  # technical report
            "Hossein2025Data",  # dataset
            "AIP2025Supplementary",  # supplementary material
            "Cangi2011Leading",  # dissertation (PhDT)
            "Konar2023Random",  # conference paper
            "Callow2022atoMEC",  # SciPy proceedings
        ):
            self.assertFalse(records[key]["publication_page"], key)

    def test_real_bibliography_rescues_mistyped_journal_articles(self):
        records = {r["key"]: r for r in marked_records(IMPORTER, self.entries)}
        # Stored as @misc by an older importer, but published in journals.
        for key in ("Cangi2024Bridging", "Ribeiro2015Corrections"):
            self.assertEqual(IMPORTER.effective_kind(records[key]), "article", key)
            self.assertTrue(records[key]["publication_page"], key)

    def test_real_bibliography_has_no_duplicate_visible_titles_or_dois(self):
        records = marked_records(IMPORTER, self.entries)
        on_page = [record for record in records if record["publication_page"]]
        titles = [IMPORTER.normalize_title(r["title"]) for r in on_page]
        dois = [IMPORTER.normalize_doi(r["doi"]) for r in on_page if r.get("doi")]
        self.assertEqual(len(titles), len(set(titles)))
        self.assertEqual(len(dois), len(set(dois)))

    def test_reworded_preprint_is_detected_and_curated(self):
        """The Gawne preprint is the same paper under a reworded title."""
        records = marked_records(IMPORTER, self.entries)
        pairs = IMPORTER.find_possible_duplicate_preprints(records)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["preprint"]["key"], "Gawne2024Ultrahighb")
        self.assertEqual(pairs[0]["article"]["key"], "Gawne2024Ultrahigh")
        self.assertGreaterEqual(pairs[0]["ratio"], 0.85)
        by_key = {r["key"]: r for r in records}
        # Curated in _data/publication_policy.yml because the bibliography
        # carries no arXiv identifier that would link the two records.
        self.assertFalse(by_key["Gawne2024Ultrahighb"]["publication_page"])

    def test_mark_reports_a_reworded_preprint_that_is_not_curated(self):
        article = {
            "title": "A machine-learning surrogate model for warm dense matter",
            "author": "T. Dornheim; A. Cangi",
            "year": "2023",
            "journal": "Phys. Plasmas",
            "doi": "10.1063/example",
            "kind": "article",
        }
        preprint = {
            "title": "A machine-learning surrogate model for warm dense matter simulations",
            "author": "T. Dornheim; A. Cangi",
            "year": "2022",
            "journal": "arXiv",
            "doi": "10.48550/arXiv.2212.99999",
            "kind": "preprint",
        }
        errors = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
            IMPORTER.mark_publication_page_records([article, preprint])
        self.assertIn("kept on the page for review", errors.getvalue())

    def test_roundtrip_preserves_types_selection_and_curated_fields(self):
        """Regenerating from existing records must not change entry types or edit choices."""
        records = records_from_bib(self.entries)
        before_types = [entry["type"] for entry in self.entries]
        before_selected = {
            entry["key"]: entry["fields"].get("selected")
            for entry in self.entries
            if entry["fields"].get("selected") == "true"
        }
        before_curated = {
            entry["key"]: {
                name: entry["fields"][name]
                for name in IMPORTER.PRESERVED_CURATED_FIELDS
                if entry["fields"].get(name)
            }
            for entry in self.entries
        }

        stdout, stderr = io.StringIO(), io.StringIO()
        with sandbox_bib(existing_text=REAL_BIB.read_text(encoding="utf-8")) as bib:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                IMPORTER.mark_publication_page_records(records)
                IMPORTER.write_bibtex_file(records)
            output = IMPORTER.parse_bibtex_entries(bib.read_text(encoding="utf-8"))

        self.assertEqual(len(output), len(self.entries))
        self.assertEqual([entry["type"] for entry in output], before_types)
        self.assertEqual(
            {
                entry["key"]: entry["fields"].get("selected")
                for entry in output
                if entry["fields"].get("selected") == "true"
            },
            before_selected,
        )
        after_by_key = {entry["key"]: entry["fields"] for entry in output}
        for key, curated in before_curated.items():
            for name, value in curated.items():
                self.assertEqual(after_by_key[key].get(name), value, f"{key}.{name}")
        for entry in output:
            self.assertIn(entry["fields"].get("publication_page"), {"true", "false"})
        self.assertIn("duplicate title in the existing bibliography", stderr.getvalue())


class CuratedOverrideTests(unittest.TestCase):
    def test_override_hides_preprint_by_arxiv_identifier(self):
        published = IMPORTER.classify_and_format_publication(
            work(
                "journal-article",
                title="Retitled journal version",
                doi="10.1103/physrevb.109.l241112",
                source="Physical Review B",
                source_type="journal",
            )
        )
        preprint = IMPORTER.classify_and_format_publication(
            work(
                "posted-content",
                title="The original preprint title",
                doi=None,
                source="arXiv",
                source_type="repository",
            )
        )
        preprint["href"] = "http://arxiv.org/abs/2403.02776v3"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            counts = IMPORTER.mark_publication_page_records([published, preprint])
        self.assertFalse(preprint["publication_page"])
        self.assertEqual(counts.get("preprint:curated-superseded"), 1)

    def test_missing_override_file_is_not_fatal(self):
        with tempfile.TemporaryDirectory() as tempdir:
            missing = pathlib.Path(tempdir) / "publication_policy.yml"
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                overrides = IMPORTER.load_preprint_overrides(missing)
        self.assertEqual(overrides, [])
        self.assertIn("could not read", errors.getvalue())

    def test_override_without_title_or_arxiv_is_ignored(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = pathlib.Path(tempdir) / "publication_policy.yml"
            path.write_text(
                "superseded_preprints:\n  - published_as: 10.1234/x\n  - 42\n",
                encoding="utf-8",
            )
            overrides = IMPORTER.load_preprint_overrides(path)
        self.assertEqual(overrides, [])


class SelectionPreservationTests(unittest.TestCase):
    def test_existing_unselected_record_is_not_reselected(self):
        seed_doi = sorted(IMPORTER.SELECTED_DOIS)[0]
        existing = f"""@article{{Someone2020Paper,
  title = {{A paper}},
  author = {{Someone, A.}},
  year = {{2020}},
  journal = {{Journal}},
  doi = {{{seed_doi}}},
}}"""
        records = [
            {
                "title": "A paper",
                "author": "A. Someone",
                "year": "2020",
                "journal": "Journal",
                "doi": seed_doi,
                "kind": "article",
            }
        ]
        with sandbox_bib(existing_text=existing) as bib:
            with contextlib.redirect_stdout(io.StringIO()):
                IMPORTER.mark_publication_page_records(records)
                IMPORTER.write_bibtex_file(records)
            output = IMPORTER.parse_bibtex_entries(bib.read_text(encoding="utf-8"))
        self.assertNotIn("selected", output[0]["fields"])

    def test_new_record_gets_seeded_but_curated_fields_survive(self):
        seed_doi = sorted(IMPORTER.SELECTED_DOIS)[1]
        existing = """@article{Someone2020Paper,
  title = {A paper},
  author = {Someone, A.},
  year = {2020},
  journal = {Journal},
  doi = {10.9999/other},
  selected = {true},
  abbr = {J.},
  preview = {preview.png},
}"""
        records = [
            {
                "title": "A paper",
                "author": "A. Someone",
                "year": "2020",
                "journal": "Journal",
                "doi": "10.9999/other",
                "kind": "article",
            },
            {
                "title": "Brand new",
                "author": "A. Someone",
                "year": "2024",
                "journal": "Journal",
                "doi": seed_doi,
                "kind": "article",
            },
        ]
        with sandbox_bib(existing_text=existing) as bib:
            with contextlib.redirect_stdout(io.StringIO()):
                IMPORTER.mark_publication_page_records(records)
                IMPORTER.write_bibtex_file(records)
            output = {
                entry["fields"]["doi"]: entry["fields"]
                for entry in IMPORTER.parse_bibtex_entries(bib.read_text(encoding="utf-8"))
            }
        self.assertEqual(output["10.9999/other"].get("abbr"), "J.")
        self.assertEqual(output["10.9999/other"].get("preview"), "preview.png")
        self.assertEqual(output["10.9999/other"].get("selected"), "true")
        self.assertEqual(output[seed_doi].get("selected"), "true")


class CuratedMetadataCollisionTests(unittest.TestCase):
    """Curation must follow the record, never a colliding title or DOI."""

    def write_and_read(self, existing, records):
        stdout, stderr = io.StringIO(), io.StringIO()
        with sandbox_bib(existing_text=existing) as bib:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                IMPORTER.mark_publication_page_records(records)
                IMPORTER.write_bibtex_file(records)
            entries = IMPORTER.parse_bibtex_entries(bib.read_text(encoding="utf-8"))
        return entries, stderr.getvalue()

    def test_title_collision_does_not_leak_curated_fields_between_versions(self):
        existing = """@article{JournalVersion,
  title = {Same work},
  author = {Someone, A.},
  year = {2024},
  journal = {Journal},
  doi = {10.1234/journal},
  selected = {true},
  preview = {journal.png},
}

@techreport{ReportVersion,
  title = {Same work},
  author = {Someone, A.},
  year = {2022},
  institution = {OSTI},
  doi = {10.2172/report},
}"""
        records = [
            {
                "title": "Same work",
                "author": "A. Someone",
                "year": "2024",
                "journal": "Journal",
                "doi": "10.1234/journal",
                "kind": "article",
            },
            {
                "title": "Same work",
                "author": "A. Someone",
                "year": "2022",
                "journal": "OSTI",
                "doi": "10.2172/report",
                "kind": "report",
            },
        ]
        entries, _ = self.write_and_read(existing, records)
        by_doi = {entry["fields"]["doi"]: entry["fields"] for entry in entries}
        self.assertEqual(by_doi["10.1234/journal"].get("preview"), "journal.png")
        self.assertEqual(by_doi["10.1234/journal"].get("selected"), "true")
        self.assertNotIn("preview", by_doi["10.2172/report"])
        self.assertNotIn("selected", by_doi["10.2172/report"])
        self.assertEqual(by_doi["10.2172/report"].get("publication_page"), "false")

    def test_title_match_with_different_doi_does_not_copy_curated_fields(self):
        existing = """@article{OldVersion,
  title = {Same work},
  author = {Someone, A.},
  year = {2020},
  journal = {Journal},
  doi = {10.1234/old},
  selected = {true},
  preview = {old.png},
}"""
        records = [
            {
                "title": "Same work",
                "author": "A. Someone",
                "year": "2024",
                "journal": "Journal",
                "doi": "10.1234/new",
                "kind": "article",
            }
        ]
        entries, stderr = self.write_and_read(existing, records)
        self.assertNotIn("preview", entries[0]["fields"])
        self.assertNotIn("selected", entries[0]["fields"])
        self.assertIn("shares a title", stderr)

    def test_legacy_entry_without_doi_still_matches_by_title(self):
        existing = """@article{LegacyEntry,
  title = {Same work},
  author = {Someone, A.},
  year = {2020},
  journal = {Journal},
  selected = {true},
  abbr = {J.},
}"""
        records = [
            {
                "title": "Same work",
                "author": "A. Someone",
                "year": "2020",
                "journal": "Journal",
                "doi": None,
                "kind": "article",
            }
        ]
        entries, _ = self.write_and_read(existing, records)
        self.assertEqual(entries[0]["fields"].get("abbr"), "J.")
        self.assertEqual(entries[0]["fields"].get("selected"), "true")

    def test_duplicate_doi_in_existing_bib_is_reported_not_merged(self):
        existing = """@article{FirstEntry,
  title = {Same work},
  author = {Someone, A.},
  year = {2024},
  journal = {Journal},
  doi = {10.1234/dup},
  preview = {first.png},
}

@article{SecondEntry,
  title = {Same work},
  author = {Someone, A.},
  year = {2024},
  journal = {Journal},
  doi = {10.1234/dup},
  preview = {second.png},
}"""
        records = [
            {
                "title": "Same work",
                "author": "A. Someone",
                "year": "2024",
                "journal": "Journal",
                "doi": "10.1234/dup",
                "kind": "article",
            }
        ]
        entries, stderr = self.write_and_read(existing, records)
        self.assertNotIn("preview", entries[0]["fields"])
        self.assertIn("duplicate DOI", stderr)

    def test_one_prior_entry_cannot_be_claimed_twice(self):
        existing = """@article{OnlyEntry,
  title = {Same work},
  author = {Someone, A.},
  year = {2024},
  journal = {Journal},
  doi = {10.1234/dup},
  selected = {true},
  preview = {only.png},
}"""
        records = [
            {
                "title": "Same work",
                "author": "A. Someone",
                "year": "2024",
                "journal": "Journal",
                "doi": "10.1234/dup",
                "kind": "article",
            },
            {
                "title": "Same work (duplicate import)",
                "author": "A. Someone",
                "year": "2024",
                "journal": "Journal",
                "doi": "10.1234/dup",
                "kind": "article",
            },
        ]
        entries, _ = self.write_and_read(existing, records)
        previews = [entry for entry in entries if entry["fields"].get("preview")]
        selections = [entry for entry in entries if entry["fields"].get("selected") == "true"]
        self.assertEqual(len(previews), 1)
        self.assertEqual(len(selections), 1)


if __name__ == "__main__":
    unittest.main()
