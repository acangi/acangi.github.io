"""Authoring checks for research highlights in ``_highlights/``.

A highlight's ``research_area`` front-matter key must match an anchor on
``/research/``; otherwise the "Related research area" link and the
per-area highlight list silently point nowhere.  The valid keys are
therefore derived from ``_pages/research.md`` (never hard-coded in the
layouts) and every highlight is validated against them.

Run without a Jekyll build:

    python -m unittest tests.test_research_highlights -v

There is no build-time hook for this check: this suite is the enforcement
point, so run it before publishing and keep the documented key list in
``CONTENT.md`` in step with ``_pages/research.md``.
"""
import pathlib
import re
import tempfile
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
HIGHLIGHTS_DIR = REPO_ROOT / "_highlights"
RESEARCH_PAGE = REPO_ROOT / "_pages" / "research.md"
CONTENT_GUIDE = REPO_ROOT / "CONTENT.md"

# Anchors that existed when this check was introduced; a deliberate rename
# must update this list, _pages/research.md, the highlights and CONTENT.md.
EXPECTED_AREAS = {
    "electronic-structure",
    "representation-learning",
    "quantum-dynamics",
    "magnetic-materials",
    "thz-driven-systems",
}

SECTION_ID_RE = re.compile(
    r'<section class="research-area"[^>]*aria-labelledby="([^"]+)"'
)
# Kramdown heading attribute list, e.g. "{: #quantum-dynamics }".
HEADING_ANCHOR_RE = re.compile(r"^\s*\{:\s*#([A-Za-z][\w-]*)", re.MULTILINE)


class ResearchAreaError(ValueError):
    """A highlight's ``research_area`` cannot be used as a research anchor."""


def section_ids(page_text):
    """Return the ``aria-labelledby`` ids of the research-area sections."""
    return set(SECTION_ID_RE.findall(page_text))


def heading_anchors(page_text):
    """Return the Kramdown heading ids used as ``/research/#...`` targets."""
    return set(HEADING_ANCHOR_RE.findall(page_text))


def valid_research_areas(page_text):
    """Derive the valid ``research_area`` keys from ``_pages/research.md``."""
    ids = section_ids(page_text)
    anchors = heading_anchors(page_text)
    if not ids:
        raise ValueError("no research-area sections found in the research page")
    if ids != anchors:
        raise ValueError(
            "research-area section ids and heading anchors disagree: "
            f"only in sections={sorted(ids - anchors)}, "
            f"only in headings={sorted(anchors - ids)}"
        )
    return ids


def read_front_matter(path):
    """Parse the YAML front matter of a Markdown file into a dict."""
    lines = pathlib.Path(path).read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError(f"{path}: file does not start with YAML front matter")
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            data = yaml.safe_load("\n".join(lines[1:index]))
            return data if isinstance(data, dict) else {}
    raise ValueError(f"{path}: YAML front matter is not closed with '---'")


def validate_research_area(front_matter, valid_areas, *, source="<front matter>"):
    """Return the highlight's ``research_area`` or raise ``ResearchAreaError``.

    ``valid_areas`` must come from :func:`valid_research_areas` so the check
    cannot drift from the anchors rendered on ``/research/``.
    """
    area = front_matter.get("research_area")
    if not isinstance(area, str) or not area.strip():
        raise ResearchAreaError(
            f"{source}: 'research_area' must be a non-empty string, got {area!r}"
        )
    if area != area.strip():
        raise ResearchAreaError(f"{source}: 'research_area' must not contain surrounding whitespace")
    if area not in valid_areas:
        raise ResearchAreaError(
            f"{source}: unknown research_area {area!r}; "
            f"valid keys: {', '.join(sorted(valid_areas))}"
        )
    return area


def collect_research_area_problems(directory, valid_areas):
    """Return one message per highlight whose ``research_area`` is unusable."""
    problems = []
    for path in sorted(pathlib.Path(directory).glob("*.md")):
        try:
            meta = read_front_matter(path)
            validate_research_area(meta, valid_areas, source=path.name)
        except (ResearchAreaError, ValueError) as error:
            problems.append(str(error))
    return problems


class HighlightFixtures(unittest.TestCase):
    """Unit-level validation behaviour, independent of real content."""

    def setUp(self):
        self.valid = {"electronic-structure", "magnetic-materials"}

    def test_valid_area_is_returned(self):
        meta = {"research_area": "electronic-structure"}
        self.assertEqual(
            validate_research_area(meta, self.valid), "electronic-structure"
        )

    def test_unknown_area_is_rejected_with_valid_keys(self):
        meta = {"research_area": "quantum-cheese"}
        with self.assertRaises(ResearchAreaError) as ctx:
            validate_research_area(meta, self.valid, source="new-post.md")
        message = str(ctx.exception)
        self.assertIn("new-post.md", message)
        self.assertIn("quantum-cheese", message)
        # The error should tell the author what to use instead.
        self.assertIn("electronic-structure", message)
        self.assertIn("magnetic-materials", message)

    def test_missing_area_is_rejected(self):
        with self.assertRaises(ResearchAreaError) as ctx:
            validate_research_area({"title": "No area"}, self.valid)
        self.assertIn("research_area", str(ctx.exception))

    def test_empty_area_is_rejected(self):
        for value in ("", "   ", None):
            with self.subTest(value=value):
                with self.assertRaises(ResearchAreaError):
                    validate_research_area({"research_area": value}, self.valid)

    def test_non_string_area_is_rejected(self):
        with self.assertRaises(ResearchAreaError) as ctx:
            validate_research_area({"research_area": 2024}, self.valid)
        self.assertIn("research_area", str(ctx.exception))

    def test_surrounding_whitespace_is_rejected(self):
        meta = {"research_area": " magnetic-materials "}
        with self.assertRaises(ResearchAreaError):
            validate_research_area(meta, self.valid)

    def test_directory_check_flags_new_bad_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            good = pathlib.Path(tmp, "good.md")
            bad = pathlib.Path(tmp, "bad.md")
            good.write_text(
                "---\nresearch_area: electronic-structure\n---\nBody.\n",
                encoding="utf-8",
            )
            bad.write_text("---\nresearch_area: not-an-area\n---\nBody.\n",
                           encoding="utf-8")
            problems = collect_research_area_problems(pathlib.Path(tmp), self.valid)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("bad.md", problems[0])
        self.assertIn("not-an-area", problems[0])


class ResearchPageAreas(unittest.TestCase):
    """The research page is the single source of truth for valid keys."""

    @classmethod
    def setUpClass(cls):
        cls.page = RESEARCH_PAGE.read_text(encoding="utf-8")
        cls.areas = valid_research_areas(cls.page)

    def test_expected_areas_are_defined(self):
        self.assertEqual(self.areas, EXPECTED_AREAS)

    def test_heading_anchors_match_section_ids(self):
        # _pages/research.md carries each key twice: on the section's
        # aria-labelledby and on the heading's Kramdown id.  If only one is
        # edited, /research/#key links break.
        self.assertEqual(section_ids(self.page), heading_anchors(self.page))
        self.assertEqual(len(section_ids(self.page)), len(self.areas))

    def test_each_area_renders_its_highlight_include(self):
        self.assertEqual(
            set(re.findall(r'research_highlights\.liquid\s+area="([^"]+)"', self.page)),
            self.areas,
        )


class HighlightContent(unittest.TestCase):
    """Regression checks against the highlights actually in the tree."""

    @classmethod
    def setUpClass(cls):
        cls.valid = valid_research_areas(RESEARCH_PAGE.read_text(encoding="utf-8"))
        cls.files = sorted(HIGHLIGHTS_DIR.glob("*.md"))

    def test_highlights_exist(self):
        self.assertTrue(self.files, f"no highlights found in {HIGHLIGHTS_DIR}")

    def test_every_highlight_declares_a_valid_area(self):
        for path in self.files:
            with self.subTest(highlight=path.name):
                meta = read_front_matter(path)
                validate_research_area(meta, self.valid, source=path.name)

    def test_directory_check_is_clean(self):
        self.assertEqual(
            collect_research_area_problems(HIGHLIGHTS_DIR, self.valid), []
        )

    def test_valid_keys_are_documented(self):
        guide = CONTENT_GUIDE.read_text(encoding="utf-8")
        for area in sorted(self.valid):
            with self.subTest(area=area):
                self.assertIn(f"`{area}`", guide)


if __name__ == "__main__":
    unittest.main()
