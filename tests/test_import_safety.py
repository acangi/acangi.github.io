"""Fail-closed tests for the scheduled OpenAlex/arXiv publication import.

The nightly workflow runs ``_scripts/openalex_to_yaml.py`` and then commits
whatever it wrote. When the arXiv request failed, the importer used to log the
error and return an empty list, so the bibliography and ``_data/*.yml`` were
rewritten without the arXiv-only preprints. A missing ``ARXIV_AUTHOR_NAME`` did
the same thing silently. Both cases must abort the run *before* any write.

These tests never touch the network: ``requests.get`` is replaced by a mock
that fails on any unexpected call, and every output path of the importer is
redirected to a temporary directory.

Run with:
    python -m unittest tests.test_import_safety -v
"""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import requests

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from publication_policy_support import load_importer  # noqa: E402

IMPORTER = load_importer()

# Every path the importer can write. Redirecting all of them keeps the real
# _data/ and _bibliography/ files out of reach.
OUTPUT_PATHS = {
    "OUTPUT_DIR": ("_data",),
    "BIBLIOGRAPHY_DIR": ("_bibliography",),
    "BIBLIOGRAPHY_FILE": ("_bibliography", "papers.bib"),
    "ARTICLES_FILE": ("_data", "articles.yml"),
    "PREPRINTS_FILE": ("_data", "preprints.yml"),
    "OTHERS_FILE": ("_data", "others.yml"),
    "PREPRINTS_UNPUBLISHED_FILE": ("_data", "preprints-unpublished.yml"),
    "ARTICLES_JSON_FILE": ("_data", "articles.json"),
    "PREPRINTS_JSON_FILE": ("_data", "preprints.json"),
    "CITATIONS_FILE": ("_data", "citations.yml"),
    "POLICY_OVERRIDES_FILE": ("_data", "publication_policy.yml"),
}

SENTINEL = "untouched\n"

WRITERS = ("write_yaml_files", "write_bibtex_file", "write_json_files")

# Hand-maintained YAML that the importer only reads. It must still be valid
# YAML in the sandbox, otherwise main() would fail for the wrong reason.
OUTPUT_SEEDS = {
    "CITATIONS_FILE": "papers: {}\n",
    "POLICY_OVERRIDES_FILE": "superseded_preprints: []\n",
}

ARXIV_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>https://arxiv.org/abs/2607.24997v1</id>
    <published>2026-07-27T00:00:00Z</published>
    <title>MANDALA: a test preprint</title>
    <author><name>Attila Cangi</name></author>
  </entry>
</feed>
"""

EMPTY_ARXIV_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"></feed>
"""


def arxiv_feed_with(entry_body: str) -> bytes:
    """Wrap one entry body in a minimal arXiv Atom feed."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        f"<entry>{entry_body}</entry></feed>"
    ).encode("utf-8")


class FakeResponse:
    """Minimal stand-in for ``requests.Response``."""

    def __init__(self, content=b"", status_code=200, reason="OK"):
        self.content = content
        self.status_code = status_code
        self.reason = reason
        self.url = "https://export.arxiv.org/api/query"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} {self.reason}", response=self
            )


class NetworkForbidden(AssertionError):
    """Raised when a test accidentally performs a real HTTP request."""


@contextlib.contextmanager
def sandbox_outputs():
    """Redirect every importer output path to a temp dir seeded with sentinels."""
    with tempfile.TemporaryDirectory() as tempdir:
        root = pathlib.Path(tempdir)
        paths = {}
        for attribute, parts in OUTPUT_PATHS.items():
            path = root.joinpath(*parts)
            if attribute.endswith("DIR"):
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(OUTPUT_SEEDS.get(attribute, SENTINEL), encoding="utf-8")
            paths[attribute] = path

        originals = {attribute: getattr(IMPORTER, attribute) for attribute in paths}
        for attribute, path in paths.items():
            setattr(IMPORTER, attribute, path)
        try:
            yield root, paths
        finally:
            for attribute, path in originals.items():
                setattr(IMPORTER, attribute, path)


def openalex_work(
    title="A journal article",
    doi="10.1103/physrevb.109.014101",
    authors=("Attila Cangi",),
):
    """Build a minimal OpenAlex work payload for the mocked OpenAlex fetch."""
    return {
        "id": f"https://openalex.org/W{abs(hash(title)) % 10000}",
        "type": "journal-article",
        "title": title,
        "doi": doi,
        "publication_year": 2024,
        "publication_date": "2024-01-01",
        "primary_location": {
            "source": {"display_name": "Physical Review B", "type": "journal"}
        },
        "best_oa_location": {},
        "open_access": {},
        "authorships": [{"author": {"display_name": name}} for name in authors],
    }


def atom_entry(identifier, title, author="Attila Cangi", published="2026-07-27"):
    """Build one arXiv Atom entry."""
    return (
        f"<entry><id>https://arxiv.org/abs/{identifier}v1</id>"
        f"<published>{published}T00:00:00Z</published>"
        f"<title>{title}</title>"
        f"<author><name>{author}</name></author></entry>"
    )


def arxiv_feed(*entries):
    """Build an arXiv Atom feed from entry fragments."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">' + "".join(entries) + "</feed>"
    ).encode("utf-8")


def bib_preprint(key, title, arxiv, author="Attila Cangi", year="2026"):
    """Build the @misc arXiv entry the importer writes for a preprint."""
    return (
        f"@misc{{{key},\n"
        f"  title = {{{title}}},\n"
        f"  author = {{{author}}},\n"
        f"  year = {{{year}}},\n"
        "  journal = {arXiv},\n"
        f"  doi = {{10.48550/arXiv.{arxiv}}},\n"
        "}\n"
    )


def bib_article(
    key,
    title,
    doi,
    author="Attila Cangi",
    year="2024",
    journal="Phys. Rev. B",
):
    """Build a journal @article entry."""
    return (
        f"@article{{{key},\n"
        f"  title = {{{title}}},\n"
        f"  author = {{{author}}},\n"
        f"  year = {{{year}}},\n"
        f"  journal = {{{journal}}},\n"
        f"  doi = {{{doi}}},\n"
        "}\n"
    )


def snapshot(root):
    """Return every file below ``root`` with its exact bytes."""
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def assert_tree_unchanged(testcase, before, after):
    """Assert that no file was created, deleted or rewritten."""
    testcase.assertEqual(sorted(after), sorted(before))
    for name, content in before.items():
        testcase.assertEqual(after[name], content, name)


def assert_sentinels_untouched(testcase, root, paths):
    """Assert that no output file was created, removed or rewritten."""
    for attribute, path in paths.items():
        if attribute.endswith("DIR"):
            continue
        testcase.assertTrue(path.exists(), f"{path} disappeared")
        expected = OUTPUT_SEEDS.get(attribute, SENTINEL)
        testcase.assertEqual(path.read_text(encoding="utf-8"), expected, str(path))
    written = sorted(
        str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
    )
    expected = sorted(
        str(path.relative_to(root))
        for attribute, path in paths.items()
        if not attribute.endswith("DIR")
    )
    testcase.assertEqual(written, expected)


class ImportSafetyTestCase(unittest.TestCase):
    """Block real HTTP for every test in this module."""

    def setUp(self):
        patcher = mock.patch.object(
            IMPORTER.requests, "get", side_effect=NetworkForbidden("real HTTP call")
        )
        self.requests_get = patcher.start()
        self.addCleanup(patcher.stop)


class ArxivFetchFailureTests(ImportSafetyTestCase):
    def test_connection_error_raises_instead_of_returning_empty(self):
        self.requests_get.side_effect = requests.exceptions.ConnectionError("boom")
        with self.assertRaises(IMPORTER.ArxivFetchError):
            IMPORTER.fetch_from_arxiv("Attila Cangi")

    def test_http_error_status_raises_with_the_original_cause(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(status_code=503, reason="Unavailable")
        with self.assertRaises(IMPORTER.ArxivFetchError) as raised:
            IMPORTER.fetch_from_arxiv("Attila Cangi")
        self.assertIsInstance(raised.exception.__cause__, requests.exceptions.HTTPError)

    def test_malformed_xml_raises_instead_of_returning_empty(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(
            content=b"<html><body>service unavailable"
        )
        with self.assertRaises(IMPORTER.ArxivFetchError) as raised:
            IMPORTER.fetch_from_arxiv("Attila Cangi")
        self.assertIn("XML", str(raised.exception))

    def test_error_message_names_the_arxiv_endpoint(self):
        self.requests_get.side_effect = requests.exceptions.Timeout("slow")
        with self.assertRaises(IMPORTER.ArxivFetchError) as raised:
            IMPORTER.fetch_from_arxiv("Attila Cangi")
        self.assertIn("export.arxiv.org", str(raised.exception))

    def test_the_endpoint_is_requested_over_https(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(content=ARXIV_FEED)
        IMPORTER.fetch_from_arxiv("Attila Cangi")
        url = self.requests_get.call_args.args[0]
        self.assertTrue(url.startswith("https://"), url)
        self.assertEqual(url, "https://export.arxiv.org/api/query")

    def test_a_valid_feed_is_still_parsed(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(content=ARXIV_FEED)
        records = IMPORTER.fetch_from_arxiv("Attila Cangi")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["title"], "MANDALA: a test preprint")
        self.assertEqual(records[0]["kind"], "preprint")

    def test_an_entry_without_an_identifier_raises(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(
            content=arxiv_feed_with("<title>A preprint with no identifier</title>")
        )
        with self.assertRaises(IMPORTER.ArxivFetchError):
            IMPORTER.fetch_from_arxiv("Attila Cangi")

    def test_an_entry_without_a_title_raises(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(
            content=arxiv_feed_with("<id>https://arxiv.org/abs/2607.00001v1</id>")
        )
        with self.assertRaises(IMPORTER.ArxivFetchError) as raised:
            IMPORTER.fetch_from_arxiv("Attila Cangi")
        self.assertIn("incomplete record", str(raised.exception))

    def test_an_empty_feed_is_warned_about_but_not_fatal(self):
        """A valid feed with no entries is legal; it must be visible, not silent."""
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(content=EMPTY_ARXIV_FEED)
        errors = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
            records = IMPORTER.fetch_from_arxiv("Attila Cangi")
        self.assertEqual(records, [])
        self.assertIn("returned no entries", errors.getvalue())


class MissingAuthorConfigTests(ImportSafetyTestCase):
    def test_blank_author_name_raises_before_any_request(self):
        for value in ("", "   ", '""'):
            with self.subTest(value=value):
                with self.assertRaises(IMPORTER.MissingAuthorConfigError):
                    IMPORTER.fetch_from_arxiv(value)
        self.requests_get.assert_not_called()

    def test_configuration_errors_share_a_base_class(self):
        self.assertTrue(issubclass(IMPORTER.ArxivFetchError, IMPORTER.PublicationImportError))
        self.assertTrue(
            issubclass(IMPORTER.MissingAuthorConfigError, IMPORTER.PublicationImportError)
        )


@contextlib.contextmanager
def quiet():
    """Silence importer chatter and expose the captured stderr stream."""
    errors = io.StringIO()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
        yield errors


class MainFailClosedTests(ImportSafetyTestCase):
    def test_main_aborts_before_writing_when_the_arxiv_request_fails(self):
        self.requests_get.side_effect = requests.exceptions.ConnectionError("boom")
        with sandbox_outputs() as (root, paths):
            with mock.patch.object(
                IMPORTER, "fetch_publications", return_value=[openalex_work()]
            ):
                with quiet() as errors:
                    with self.assertRaises(SystemExit) as raised:
                        IMPORTER.main()
                self.assertEqual(raised.exception.code, 1)
                assert_sentinels_untouched(self, root, paths)
        self.assertIn("Aborting", errors.getvalue())
        self.assertIn("_bibliography/papers.bib", errors.getvalue())

    def test_main_aborts_before_writing_when_the_arxiv_feed_is_unparseable(self):
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(content=b"\x00not xml")
        with sandbox_outputs() as (root, paths):
            with mock.patch.object(
                IMPORTER, "fetch_publications", return_value=[openalex_work()]
            ):
                with quiet():
                    with self.assertRaises(SystemExit) as raised:
                        IMPORTER.main()
                self.assertEqual(raised.exception.code, 1)
                assert_sentinels_untouched(self, root, paths)

    def test_main_never_reaches_the_writers_when_the_arxiv_fetch_fails(self):
        writers = {name: mock.Mock() for name in WRITERS}
        with sandbox_outputs():
            with mock.patch.multiple(
                IMPORTER,
                fetch_publications=mock.Mock(return_value=[openalex_work()]),
                fetch_from_arxiv=mock.Mock(side_effect=IMPORTER.ArxivFetchError("boom")),
                **writers,
            ):
                with quiet():
                    with self.assertRaises(SystemExit):
                        IMPORTER.main()
        for name in WRITERS:
            writers[name].assert_not_called()

    def test_main_aborts_on_missing_author_config_before_any_network_call(self):
        with sandbox_outputs() as (root, paths):
            with mock.patch.object(IMPORTER, "ARXIV_AUTHOR_NAME", ""):
                with mock.patch.object(IMPORTER, "fetch_publications") as openalex_fetch:
                    with quiet():
                        with self.assertRaises(SystemExit) as raised:
                            IMPORTER.main()
                    self.assertEqual(raised.exception.code, 1)
                    openalex_fetch.assert_not_called()
                    assert_sentinels_untouched(self, root, paths)
        self.requests_get.assert_not_called()


class MainSuccessPathTests(ImportSafetyTestCase):
    def test_main_writes_once_when_both_fetches_succeed(self):
        def fake_get(url, **kwargs):
            if "export.arxiv.org" in url:
                return FakeResponse(content=ARXIV_FEED)
            raise NetworkForbidden(url)

        self.requests_get.side_effect = fake_get
        writers = {name: mock.Mock() for name in WRITERS}
        with sandbox_outputs():
            with mock.patch.multiple(
                IMPORTER,
                fetch_publications=mock.Mock(return_value=[openalex_work()]),
                **writers,
            ):
                with quiet():
                    IMPORTER.main()
        for name in WRITERS:
            writers[name].assert_called_once()
        records = writers["write_yaml_files"].call_args.args[0]
        titles = [record["title"] for record in records]
        self.assertIn("A journal article", titles)
        self.assertIn("MANDALA: a test preprint", titles)
        arxiv_calls = [
            call
            for call in self.requests_get.call_args_list
            if "export.arxiv.org" in call.args[0]
        ]
        self.assertEqual(len(arxiv_calls), 1)


class PreprintLossGuardTests(ImportSafetyTestCase):
    """Every bibliography preprint must come back as a preprint, or nothing is written."""

    PRIOR = bib_preprint("Cangi2026MANDALA", "MANDALA: a test preprint", "2607.24997")
    SECOND = bib_preprint("Cangi2026Second", "A second preprint", "2605.00001")

    def run_import(self, *, feed, prior_bib, works=None, overrides=None):
        """Run main() in a sandbox; return exit code, stderr, writers and files."""
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(content=feed)
        writers = {name: mock.Mock() for name in WRITERS}
        with sandbox_outputs() as (root, paths):
            if prior_bib is not None:
                paths["BIBLIOGRAPHY_FILE"].write_text(prior_bib, encoding="utf-8")
            if overrides is not None:
                paths["POLICY_OVERRIDES_FILE"].write_text(overrides, encoding="utf-8")
            before = snapshot(root)
            payload = [openalex_work()] if works is None else works
            exit_code = None
            errors = io.StringIO()
            with mock.patch.multiple(
                IMPORTER,
                fetch_publications=mock.Mock(return_value=payload),
                **writers,
            ):
                try:
                    with contextlib.redirect_stdout(io.StringIO()):
                        with contextlib.redirect_stderr(errors):
                            IMPORTER.main()
                except SystemExit as exc:
                    exit_code = exc.code
            after = snapshot(root)
        return {
            "exit": exit_code,
            "stderr": errors.getvalue(),
            "before": before,
            "after": after,
            "writers": writers,
        }

    def test_empty_feed_with_existing_preprints_blocks_the_update(self):
        """Zero entries is an upstream failure, even if OpenAlex shows a successor."""
        result = self.run_import(
            feed=EMPTY_ARXIV_FEED,
            prior_bib=self.PRIOR,
            works=[openalex_work(title="MANDALA: a test preprint")],
        )
        self.assertEqual(result["exit"], 1)
        assert_tree_unchanged(self, result["before"], result["after"])
        for name in WRITERS:
            result["writers"][name].assert_not_called()
        self.assertIn("arXiv returned no entries", result["stderr"])
        self.assertIn("MANDALA: a test preprint", result["stderr"])

    def test_partial_feed_that_drops_a_preprint_blocks_and_names_it(self):
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2607.24997", "MANDALA: a test preprint")),
            prior_bib=self.PRIOR + self.SECOND,
        )
        self.assertEqual(result["exit"], 1)
        assert_tree_unchanged(self, result["before"], result["after"])
        for name in WRITERS:
            result["writers"][name].assert_not_called()
        self.assertIn("A second preprint", result["stderr"])
        self.assertIn("Cangi2026Second", result["stderr"])
        self.assertNotIn("Cangi2026MANDALA", result["stderr"])
        self.assertIn("were not fetched again as preprints", result["stderr"])

    def test_journal_successor_by_title_does_not_allow_deletion(self):
        """A same-title journal article is not a substitute for the preprint record."""
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2605.00001", "A second preprint")),
            prior_bib=self.PRIOR,
            works=[openalex_work(title="MANDALA: a test preprint")],
        )
        self.assertEqual(result["exit"], 1)
        assert_tree_unchanged(self, result["before"], result["after"])
        for name in WRITERS:
            result["writers"][name].assert_not_called()
        self.assertIn("MANDALA: a test preprint", result["stderr"])
        self.assertIn("were not fetched again as preprints", result["stderr"])

    def test_arxiv_identifier_in_a_journal_article_is_not_enough(self):
        """A journal article carrying the preprint's arXiv id still cannot replace it."""
        work = openalex_work(title="Published under a new title")
        work["locations"] = [{"landing_page_url": "https://arxiv.org/abs/2607.24997"}]
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2605.00001", "A second preprint")),
            prior_bib=self.PRIOR,
            works=[work],
        )
        self.assertEqual(result["exit"], 1)
        assert_tree_unchanged(self, result["before"], result["after"])
        for name in WRITERS:
            result["writers"][name].assert_not_called()
        self.assertIn("Cangi2026MANDALA", result["stderr"])

    def test_curated_override_does_not_allow_deletion(self):
        """A superseded_preprints entry documents publication but keeps the record."""
        overrides = (
            "superseded_preprints:\n"
            "  - title: 'MANDALA: a test preprint'\n"
            "    arxiv: '2607.24997'\n"
            "    published_as: '10.1103/physrevb.109.014101'\n"
        )
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2605.00001", "A second preprint")),
            prior_bib=self.PRIOR,
            overrides=overrides,
        )
        self.assertEqual(result["exit"], 1)
        assert_tree_unchanged(self, result["before"], result["after"])
        for name in WRITERS:
            result["writers"][name].assert_not_called()
        self.assertIn("Cangi2026MANDALA", result["stderr"])
        self.assertIn("superseded_preprints entry is not accepted", result["stderr"])

    def test_curated_override_and_journal_successor_together_still_block(self):
        """Both kinds of 'successor' evidence at once must not delete the record."""
        overrides = (
            "superseded_preprints:\n"
            "  - title: 'MANDALA: a test preprint'\n"
            "    arxiv: '2607.24997'\n"
            "    published_as: '10.1103/physrevb.109.014101'\n"
        )
        work = openalex_work(title="MANDALA: a test preprint")
        work["locations"] = [{"landing_page_url": "https://arxiv.org/abs/2607.24997"}]
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2605.00001", "A second preprint")),
            prior_bib=self.PRIOR,
            works=[work],
            overrides=overrides,
        )
        self.assertEqual(result["exit"], 1)
        assert_tree_unchanged(self, result["before"], result["after"])
        for name in WRITERS:
            result["writers"][name].assert_not_called()
        self.assertIn("MANDALA: a test preprint", result["stderr"])

    def test_same_title_by_another_author_is_not_a_successor(self):
        """Title evidence must not hide a loss behind an unrelated paper."""
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2605.00001", "A second preprint")),
            prior_bib=self.PRIOR,
            works=[
                openalex_work(
                    title="MANDALA: a test preprint",
                    authors=("Someone Else",),
                )
            ],
        )
        self.assertEqual(result["exit"], 1)
        self.assertIn("MANDALA: a test preprint", result["stderr"])

    def test_empty_feed_without_prior_preprints_is_allowed(self):
        result = self.run_import(
            feed=EMPTY_ARXIV_FEED,
            prior_bib=bib_article("Cangi2024X", "A journal article", "10.1103/physrevb.109.014101"),
        )
        self.assertIsNone(result["exit"])
        for name in WRITERS:
            self.assertEqual(result["writers"][name].call_count, 1)

    def test_all_prior_preprints_still_fetched_allows_the_update(self):
        feed = arxiv_feed(
            atom_entry("2607.24997", "MANDALA: a test preprint"),
            atom_entry("2605.00001", "A second preprint"),
        )
        result = self.run_import(feed=feed, prior_bib=self.PRIOR + self.SECOND)
        self.assertIsNone(result["exit"])
        for name in WRITERS:
            self.assertEqual(result["writers"][name].call_count, 1)
        written = result["writers"]["write_yaml_files"].call_args.args[0]
        titles = [record["title"] for record in written]
        self.assertIn("A second preprint", titles)

    def test_existing_journal_articles_are_not_protected(self):
        """The guard is narrow: it protects preprints, not article records."""
        prior = bib_article(
            "Cangi2024Gone",
            "An article no longer fetched",
            "10.1103/physrevb.109.014101",
        )
        result = self.run_import(
            feed=arxiv_feed(atom_entry("2605.00001", "A second preprint")),
            prior_bib=prior,
            works=[openalex_work(title="Something else entirely")],
        )
        self.assertIsNone(result["exit"])

    def test_unreadable_bibliography_blocks_the_update(self):
        writers = {name: mock.Mock() for name in WRITERS}
        self.requests_get.side_effect = None
        self.requests_get.return_value = FakeResponse(content=ARXIV_FEED)
        with sandbox_outputs() as (root, paths):
            path = paths["BIBLIOGRAPHY_FILE"]
            path.unlink()
            path.mkdir()
            with mock.patch.multiple(
                IMPORTER,
                fetch_publications=mock.Mock(return_value=[openalex_work()]),
                **writers,
            ):
                with quiet() as errors:
                    with self.assertRaises(SystemExit) as raised:
                        IMPORTER.main()
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("cannot read", errors.getvalue())
        for name in WRITERS:
            writers[name].assert_not_called()


if __name__ == "__main__":
    unittest.main()
