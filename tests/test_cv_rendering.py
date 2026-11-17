"""Rendered-output checks for the Markdown CV page.

The Jekyll build is intentionally not run from unittest. Build first, then:

    JEKYLL_ENV=production bundle exec jekyll build
    CV_HTML="$PWD/_site/cv/index.html" \
      python -m unittest tests.test_cv_rendering -v

`SITE_DIR` is accepted as an alternative (`$SITE_DIR/cv/index.html`).

The page is written as Kramdown/GFM pipe tables. Kramdown only recognises a
table when it is preceded by a blank line, so a table glued to the heading
above it degrades into a paragraph of literal `| ... |` text. These tests read
the source Markdown and the built HTML side by side so that a regression in
either the delimiters or the surrounding blank lines fails loudly, and so that
tables can never silently lose rows or cells.
"""
import html as html_module
import os
import pathlib
import re
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CV_SOURCE = REPO_ROOT / "_pages" / "cv.md"

HTML_PATH = os.getenv("CV_HTML", "")
if not HTML_PATH and os.getenv("SITE_DIR"):
    HTML_PATH = str(pathlib.Path(os.environ["SITE_DIR"]) / "cv" / "index.html")


def normalise(text):
    """Reduce Markdown or HTML cell content to comparable plain text."""
    text = re.sub(r"<[^>]+>", "", text)
    text = html_module.unescape(text)
    # Markdown links -> link text; emphasis markers dropped.
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.replace("**", "").replace("*", "")
    text = text.replace("\u00a0", " ")
    for source, target in (
        ("\u2019", "'"),
        ("\u2018", "'"),
        ("\u201c", '"'),
        ("\u201d", '"'),
        ("\u2013", "-"),
        ("\u2014", "-"),
    ):
        text = text.replace(source, target)
    return re.sub(r"\s+", " ", text).strip()


def split_row(line):
    return [cell for cell in line.strip().strip("|").split("|")]


def source_tables(markdown):
    """Return [(section, header_cells, [row_cells, ...]), ...] in order."""
    tables = []
    section = ""
    block = []
    for line in markdown.splitlines() + [""]:
        stripped = line.strip()
        if stripped.startswith("#"):
            section = normalise(stripped.lstrip("#"))
        if stripped.startswith("|"):
            block.append(stripped)
            continue
        if block:
            if len(block) >= 2 and re.fullmatch(r"\|[\s:|-]+\|?", block[1]):
                tables.append((section, split_row(block[0]), [split_row(row) for row in block[2:]]))
            block = []
    return tables


def rendered_tables(article_html):
    """Return [(header_cells, [row_cells, ...]), ...] for each <table>."""
    tables = []
    for table_html in re.findall(r"<table\b.*?</table>", article_html, re.S):
        head_match = re.search(r"<thead\b[^>]*>(.*?)</thead>", table_html, re.S)
        header = []
        if head_match:
            header_rows = re.findall(r"<tr\b[^>]*>(.*?)</tr>", head_match.group(1), re.S)
            if header_rows:
                header = [
                    normalise(cell)
                    for cell in re.findall(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", header_rows[0], re.S)
                ]
        body_match = re.search(r"<tbody\b[^>]*>(.*?)</tbody>", table_html, re.S)
        body_html = body_match.group(1) if body_match else table_html
        rows = re.findall(r"<tr\b[^>]*>(.*?)</tr>", body_html, re.S)
        parsed = [
            [normalise(cell) for cell in re.findall(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", row, re.S)]
            for row in rows
        ]
        tables.append((header, parsed))
    return tables


@unittest.skipUnless(HTML_PATH, "set CV_HTML (or SITE_DIR) to a built site")
class CvRenderingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = CV_SOURCE.read_text(encoding="utf-8")
        html_text = pathlib.Path(HTML_PATH).read_text(encoding="utf-8")
        match = re.search(r"<article>(.*?)</article>", html_text, re.S)
        if not match:
            raise AssertionError("no <article> element in built CV HTML")
        cls.article = match.group(1)
        cls.expected = source_tables(cls.source)
        cls.actual = rendered_tables(cls.article)

    def test_source_has_twenty_tables(self):
        # Guards the fixture itself: a silently dropped table block in cv.md
        # would make the comparison below trivially pass.
        self.assertEqual(len(self.expected), 20)

    def test_no_literal_pipe_text(self):
        """The original bug: tables rendered as paragraphs of `| a | b |`."""
        literal = re.findall(r"<p>\s*\|.*?\|.*?</p>", self.article, re.S)
        self.assertEqual(literal, [], "pipe table syntax leaked into a paragraph")
        self.assertNotIn("|:-", self.article)

    def test_blank_markdown_headers_are_dropped(self):
        """GFM needs a header row; the CV has none, so it must not render one.

        An unlabelled header row shows up as a blank band above the table and
        as empty column headers for assistive technology.
        """
        self.assertTrue(
            all(all(not cell.strip() for cell in header) for _, header, _ in self.expected),
            "fixture changed: the CV tables now carry header labels",
        )
        self.assertEqual(self.article.count("<thead"), 0, "empty header row still rendered")

    def test_every_markdown_table_renders_as_a_table(self):
        self.assertEqual(len(self.actual), len(self.expected))

    def test_headings_are_not_swallowed_by_tables(self):
        expected_headings = [
            normalise(line.lstrip("#"))
            for line in self.source.splitlines()
            if line.startswith("#")
        ]
        rendered_headings = [
            normalise(match)
            for match in re.findall(r"<h[1-6]\b[^>]*>(.*?)</h[1-6]>", self.article, re.S)
        ]
        self.assertEqual(rendered_headings, expected_headings)

    def test_table_rows_and_cells_are_preserved(self):
        for (section, expected_header, expected_rows), (actual_header, actual_rows) in zip(
            self.expected, self.actual
        ):
            with self.subTest(section=section):
                if all(not cell.strip() for cell in expected_header):
                    self.assertEqual(actual_header, [], "unlabelled header row should be dropped")
                else:
                    self.assertEqual(
                        actual_header, [normalise(cell) for cell in expected_header],
                        "header cell count",
                    )
                self.assertEqual(len(actual_rows), len(expected_rows), "data row count")
                for index, (expected_row, actual_row) in enumerate(
                    zip(expected_rows, actual_rows), start=1
                ):
                    self.assertEqual(len(actual_row), len(expected_row), "cell count")
                    self.assertEqual(
                        actual_row,
                        [normalise(cell) for cell in expected_row],
                        f"row {index} content",
                    )

    def test_only_pipe_tables_inside_article(self):
        # A CV table rendered as raw HTML would bypass the Markdown source of
        # truth above.
        self.assertEqual(self.article.count("<table"), len(self.expected))


class CvSourceTests(unittest.TestCase):
    """Source-level guards; these run without a Jekyll build.

    Kramdown only recognises a pipe table when it is preceded by a blank line
    and followed by one. With a heading glued to the table the whole block
    becomes a paragraph of literal pipes, which is how the CV page broke.
    """

    def setUp(self):
        self.lines = CV_SOURCE.read_text(encoding="utf-8").splitlines()

    def _blocks(self):
        blocks = []
        current = []
        for index, line in enumerate(self.lines):
            if line.lstrip().startswith("|"):
                current.append(index)
                continue
            if current:
                blocks.append(current)
                current = []
        if current:
            blocks.append(current)
        return blocks

    def test_tables_are_separated_by_blank_lines(self):
        for block in self._blocks():
            first, last = block[0], block[-1]
            with self.subTest(first_table_line=first + 1):
                self.assertTrue(
                    first > 0 and not self.lines[first - 1].strip(),
                    "table needs a blank line before it (Kramdown requirement)",
                )
                self.assertTrue(
                    last + 1 >= len(self.lines) or not self.lines[last + 1].strip(),
                    "table needs a blank line after it",
                )

    def test_tables_have_a_delimiter_row_and_consistent_columns(self):
        for block in self._blocks():
            with self.subTest(first_table_line=block[0] + 1):
                rows = [split_row(self.lines[index]) for index in block]
                self.assertGreaterEqual(len(rows), 3, "header, delimiter and at least one row")
                self.assertRegex(self.lines[block[1]].strip(), r"^\|[\s:|-]+\|?$")
                widths = {len(row) for row in rows}
                self.assertEqual(len(widths), 1, f"ragged table columns: {sorted(widths)}")


if __name__ == "__main__":
    unittest.main()
