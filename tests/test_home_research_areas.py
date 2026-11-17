"""Authoring checks for the homepage research-area grid.

``_pages/about.md`` shows every research area as an icon plus the area title, linking to ``/research/#<anchor>``.  ``_pages/research.md``
is the single source of truth: titles, anchors and their order are derived from
it here, so renaming an area cannot silently leave the homepage pointing at a
dead anchor or advertising a stale title.

The grid must stay a *single* presentation of the areas: no descriptions and no
second, redundant area menu.  The homepage also drops the visible section
numbers and the heading around the latent-dynamics visualization, whose
explanation lives in the figure tooltip.

Run without a Jekyll build:

    python -m unittest tests.test_home_research_areas -v
"""
import pathlib
import re
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ABOUT_PAGE = REPO_ROOT / "_pages" / "about.md"
RESEARCH_PAGE = REPO_ROOT / "_pages" / "research.md"
LATENT_DYNAMICS = REPO_ROOT / "_includes" / "latent_dynamics.liquid"
ICON_DIR = REPO_ROOT / "assets" / "img" / "research"

HEADING_RE = re.compile(r"^###\s+(?P<title>.+?)\s*\n\{:\s*#(?P<anchor>[A-Za-z][\w-]*)\s*\}", re.MULTILINE)
CARD_RE = re.compile(
    r'<a class="nh-area-card" href="\{\{ \'/research/\' \| relative_url \}\}#(?P<anchor>[a-z0-9-]+)">'
    r"(?P<body>.*?)</a>",
    re.DOTALL,
)
IMG_RE = re.compile(r'<img src="\{\{ \'(?P<path>/assets/img/research/[^\']+)\' \| relative_url \}\}"')
TITLE_RE = re.compile(r'<span class="nh-area-title">\s*(?P<title>.+?)\s*</span>', re.DOTALL)


def research_areas():
    """Return ``[(anchor, title), ...]`` in the order used by /research/."""
    text = RESEARCH_PAGE.read_text(encoding="utf-8")
    areas = [(m.group("anchor"), m.group("title").strip()) for m in HEADING_RE.finditer(text)]
    if not areas:
        raise ValueError(f"no research-area headings found in {RESEARCH_PAGE}")
    return areas


def homepage_section(text):
    """Return the markup of the homepage research section."""
    start = text.index('id="research"')
    end = text.index("</section>", text.index('class="nh-area-grid"', start))
    return text[start:end]


def parse_cards(section_text):
    return [
        {
            "anchor": m.group("anchor"),
            "body": m.group("body"),
        }
        for m in CARD_RE.finditer(section_text)
    ]


class HomepageResearchAreas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = ABOUT_PAGE.read_text(encoding="utf-8")
        cls.section = homepage_section(cls.text)
        cls.cards = parse_cards(cls.section)
        cls.expected = research_areas()

    def test_every_area_is_listed_once(self):
        anchors = [card["anchor"] for card in self.cards]
        self.assertEqual(len(anchors), len(set(anchors)), "duplicate research areas on the homepage")
        self.assertEqual(sorted(anchors), sorted(anchor for anchor, _ in self.expected))

    def test_areas_keep_the_research_page_order(self):
        self.assertEqual([card["anchor"] for card in self.cards], [anchor for anchor, _ in self.expected])

    def test_titles_match_the_research_page(self):
        expected = dict(self.expected)
        for card in self.cards:
            with self.subTest(area=card["anchor"]):
                match = TITLE_RE.search(card["body"])
                if match is None:
                    self.fail(f"{card['anchor']}: missing nh-area-title")
                self.assertEqual(" ".join(match.group("title").split()), expected[card["anchor"]])

    def test_each_area_carries_an_icon(self):
        for card in self.cards:
            with self.subTest(area=card["anchor"]):
                match = IMG_RE.search(card["body"])
                if match is None:
                    self.fail(f"{card['anchor']}: missing research icon")
                icon = REPO_ROOT / match.group("path").lstrip("/")
                self.assertTrue(icon.is_file(), f"{card['anchor']}: icon {icon} does not exist")
                self.assertIn('alt=""', card["body"], "decorative icons need an empty alt attribute")
                self.assertIn('loading="lazy"', card["body"])

    def test_cards_have_no_redundant_area_label(self):
        for card in self.cards:
            with self.subTest(area=card["anchor"]):
                self.assertNotIn('class="nh-area-label"', card["body"])

    def test_no_descriptions_in_the_grid(self):
        for card in self.cards:
            with self.subTest(area=card["anchor"]):
                self.assertNotRegex(card["body"], r"<p[\s>]", "research areas are icon + label + title only")
        # And the section itself has no prose paragraph outside the grid.
        without_grid = self.section.split('class="nh-area-grid"')[0]
        self.assertNotRegex(without_grid, r"<p[\s>]")

    def test_tiles_hold_only_icon_label_and_title(self):
        for card in self.cards:
            with self.subTest(area=card["anchor"]):
                classes = re.findall(r'class="(nh-area-[\w-]+)"', card["body"])
                self.assertEqual(classes, ["nh-area-icon", "nh-area-title"])
                self.assertNotIn("<a", card["body"], "the whole tile is one link, not a link inside it")
                self.assertNotRegex(card["body"], r"(?i)read more")

    def test_no_second_area_menu(self):
        self.assertNotIn("nh-area-index", self.text, "the redundant area index menu is gone")
        # The descriptive three-card teaser is superseded by the grid.
        self.assertNotIn('class="nh-cards"', self.text)
        self.assertNotIn('class="nh-card"', self.text)

    def test_no_visible_section_numbers(self):
        self.assertNotIn("nh-sec-num", self.text, "homepage section heads carry no visible numbers")
        self.assertNotRegex(self.text, r">\s*0\d\s*<", "stray section number left in the markup")


class HomepageSectionHeads(unittest.TestCase):
    """The visualization keeps its figure and tooltip, without a heading."""

    @classmethod
    def setUpClass(cls):
        cls.text = ABOUT_PAGE.read_text(encoding="utf-8")
        start = cls.text.index('id="dynamics"')
        cls.dynamics = cls.text[start : cls.text.index("</section>", start)]

    def test_visualization_has_no_heading_or_eyebrow(self):
        self.assertNotIn("nh-sec-head", self.dynamics)
        self.assertNotIn("nh-sec-note", self.dynamics)
        self.assertNotIn("Dynamics in a learned space", self.dynamics)
        self.assertNotIn("Interactive", self.dynamics)

    def test_visualization_is_still_rendered_and_described(self):
        self.assertIn("latent_dynamics.liquid", self.dynamics)
        figure = LATENT_DYNAMICS.read_text(encoding="utf-8")
        self.assertIn("latent-dynamics-tooltip", figure)
        self.assertIn("Dynamics in a learned space", figure)
        self.assertIn("aria-describedby=\"latent-dynamics-description\"", figure)

    def test_no_dangling_heading_reference(self):
        # Removing the <h2> must also remove aria-labelledby, or the section
        # would point at a heading that no longer exists.
        self.assertNotIn("aria-labelledby", self.dynamics)


if __name__ == "__main__":
    unittest.main()
