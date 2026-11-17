"""Rendered-output checks for the publications page.

The Jekyll build is intentionally not run from unittest. Build first, then:

    JEKYLL_ENV=production bundle exec jekyll build --destination /tmp/publications-site
    PUBLICATIONS_HTML=/tmp/publications-site/publications/index.html \
      python -m unittest tests.test_rendered_publications -v

The expected set of entries is computed independently from the real
bibliography with the importer's policy functions. The page itself is filtered
by _plugins/publications-policy.rb, so agreement between the two is a real
cross-check of the Liquid/Ruby and Python implementations.
"""
import html as html_module
import os
import pathlib
import re
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from publication_policy_support import (  # noqa: E402
    REAL_BIB,
    load_importer,
    marked_records,
)

HTML_PATH = os.getenv("PUBLICATIONS_HTML", "")


def clean_text(value: str) -> str:
    """Strip tags/entities and collapse whitespace.

    The bibliography is rendered with the `latex` filter, which turns straight
    apostrophes and dashes into typographic ones. Normalise them so titles can
    be compared with the raw BibTeX field.
    """
    text = re.sub(r"<[^>]+>", "", value)
    text = re.sub(r"\s+", " ", html_module.unescape(text)).strip()
    for source, target in (("\u2019", "'"), ("\u2018", "'"), ("\u201c", '"'), ("\u201d", '"'), ("\u2013", "-"), ("\u2014", "-")):
        text = text.replace(source, target)
    return text


@unittest.skipUnless(HTML_PATH, "set PUBLICATIONS_HTML to a built publications/index.html")
class RenderedPublicationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = pathlib.Path(HTML_PATH).read_text(encoding="utf-8")
        cls.blocks = re.findall(
            r'<div id="([^"]+)" class="col-sm-8">(.*?)<div class="links">',
            cls.html,
            re.S,
        )
        importer = load_importer()
        entries = importer.parse_bibtex_entries(REAL_BIB.read_text(encoding="utf-8"))
        cls.records = marked_records(importer, entries)
        cls.visible = [record for record in cls.records if record["publication_page"]]
        cls.preprints = [
            record for record in cls.visible if record["kind"] == "preprint"
        ]

    def titles(self):
        titles = []
        for key, block in self.blocks:
            match = re.search(r'<div class="title">(.*?)</div>', block, re.S)
            self.assertIsNotNone(match, key)
            assert match is not None
            titles.append(clean_text(match.group(1)))
        return titles

    def periodicals(self, block):
        values = re.findall(r'<div class="periodical">(.*?)</div>', block, re.S)
        return [clean_text(value) for value in values]

    def test_page_lists_exactly_the_policy_entries(self):
        expected = sorted(clean_text(record["title"] or "") for record in self.visible)
        rendered = sorted(self.titles())
        self.assertEqual(len(rendered), len(expected))
        self.assertEqual(rendered, expected)
        self.assertEqual(len(rendered), 62)

    def test_page_renders_no_duplicate_titles(self):
        titles = self.titles()
        self.assertEqual(len(titles), len(set(titles)))

    def test_page_renders_no_talks_reports_datasets_or_conference_papers(self):
        text = clean_text(self.html)
        for label in (
            "Conference talk",
            "Conference abstract",
            "Conference paper",
            "Technical report",
            "Dissertation",
            "Book chapter",
            "Dataset",
            "Repository record",
            "Supplementary material",
        ):
            self.assertNotIn(label, text)
        # No "In <journal>" prefix: that markup only belongs to proceedings.
        self.assertNotIn("In <em>", self.html)

    def test_every_preprint_is_labelled_and_only_preprints_are(self):
        labelled = [key for key, block in self.blocks if "Preprint" in self.periodicals(block)]
        self.assertEqual(len(labelled), len(self.preprints))
        self.assertEqual(len(labelled), 8)

    def test_no_empty_periodical(self):
        empty = [
            key
            for key, block in self.blocks
            for value in self.periodicals(block)
            if not value
        ]
        self.assertEqual(empty, [])

    def test_every_entry_shows_venue_and_year(self):
        for key, block in self.blocks:
            values = self.periodicals(block)
            self.assertTrue(values, key)
            self.assertRegex(values[0], r"\d{4}$", key)

    def test_year_headings_match_visible_entries(self):
        headings = re.findall(r'<h2 class="bibliography">(\d{4})</h2>', self.html)
        years = {str(record["year"]) for record in self.visible}
        self.assertEqual(set(headings), years)
        self.assertEqual(len(headings), len(set(headings)))

    def test_doi_links_match_entries_with_a_doi(self):
        links = re.findall(r'href="https://doi\.org/([^"]+)"', self.html)
        expected = sorted(
            record["doi"].lower()
            for record in self.visible
            if record.get("doi")
        )
        self.assertEqual(len(links), len(expected))


if __name__ == "__main__":
    unittest.main()
