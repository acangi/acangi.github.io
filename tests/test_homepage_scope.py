"""Homepage scope checks for the simplified landing page.

The homepage is a single-purpose landing page: identity, the research-area
grid and the latent-dynamics visualization.  The user asked to remove the
hero research statement, its action links, the biography note, the
"Elsewhere" band and the monogram badge, the software spotlight section, and
the selected-publications list.

The hero's visible H1 is the department name, "Machine Learning for Materials
Design"; the head, Attila Cangi, is named once beside it as "Head of
Department".  The department name is stated once in the hero so the heading
and the role line cannot drift into repetition.

The /software/ page itself is kept and keeps its URL; only its navigation
entry is removed, so these checks assert the page content and permalink still
exist and that no page advertises it in the navbar.

Run without a Jekyll build:

    python -m unittest tests.test_homepage_scope -v
"""

import pathlib
import re
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ABOUT_PAGE = REPO_ROOT / "_pages" / "about.md"
SOFTWARE_PAGE = REPO_ROOT / "_pages" / "software.md"
PAGES_DIR = REPO_ROOT / "_pages"


def split_front_matter(text):
    """Return ``(front_matter_dict, body)`` for a Jekyll Markdown page.

    Parsed only as far as these tests need it: top-level ``key: value`` pairs
    with one level of nesting (``announcements:`` and friends), ``true``/
    ``false`` converted to booleans, and trailing ``# comments`` dropped.
    """
    match = re.match(r"\A---\s*\n(?P<fm>.*?)\n---\s*\n(?P<body>.*)\Z", text, re.DOTALL)
    if match is None:
        raise ValueError("page has no front matter block")
    front_matter = {}
    parent = None
    for raw in match.group("fm").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        key, _, value = raw.strip().partition(":")
        key = key.strip()
        value = re.split(r"\s+#", value.strip(), maxsplit=1)[0].strip()
        if raw[0] in " \t":
            if isinstance(parent, dict):
                parent[key] = coerce_scalar(value)
            continue
        if value == "":
            parent = front_matter[key] = {}
        else:
            parent = None
            front_matter[key] = coerce_scalar(value)
    return front_matter, match.group("body")


def coerce_scalar(value):
    if value == "true":
        return True
    if value == "false":
        return False
    return value


H1_RE = re.compile(r"<h1\b(?P<attrs>[^>]*)>(?P<text>.*?)</h1>", re.DOTALL)
ID_RE = re.compile(r'\bid="(?P<id>[^"]+)"')
DEPARTMENT = "Machine Learning for Materials Design"


class HomepageHero(unittest.TestCase):
    """The hero leads with the department name and names the head below it."""

    @classmethod
    def setUpClass(cls):
        cls.text = ABOUT_PAGE.read_text(encoding="utf-8")
        cls.front_matter, cls.body = split_front_matter(cls.text)
        start = cls.body.index('class="nh-hero"')
        cls.hero = cls.body[start : cls.body.index("</section>", start)]

    def test_single_visible_h1_is_the_department_name(self):
        headings = H1_RE.findall(self.body)
        self.assertEqual(len(headings), 1, "the homepage must expose exactly one H1")
        _, text = headings[0]
        self.assertEqual(" ".join(text.split()), DEPARTMENT)

    def test_h1_labels_the_hero_section(self):
        attrs, _ = H1_RE.findall(self.body)[0]
        heading_id = ID_RE.search(attrs)
        if heading_id is None:
            self.fail("the H1 needs an id for aria-labelledby")
        self.assertIn(f'aria-labelledby="{heading_id.group("id")}"', self.hero)

    def test_head_is_named_next_to_the_department(self):
        # The name follows the heading, and neither string is repeated in the
        # hero: the department is stated once, the person once.
        self.assertEqual(self.hero.count(DEPARTMENT), 1)
        self.assertEqual(self.hero.count("Attila Cangi"), 1)
        self.assertIn("<strong>Attila Cangi</strong>", self.hero)
        self.assertIn("Head of Department", self.hero)
        self.assertLess(self.hero.index(DEPARTMENT), self.hero.index("Attila Cangi"))

    def test_identity_and_affiliation_are_kept(self):
        self.assertIn('class="nh-hero"', self.body)
        self.assertIn("Center for Advanced Systems Understanding", self.body)
        self.assertIn("https://www.casus.science", self.body)
        self.assertIn("https://www.hzdr.de", self.body)
        self.assertIn("Görlitz, Germany", self.body)
        self.assertNotIn("Ph.D. · Görlitz, Germany", self.body)
        self.assertIn("CASUS — Helmholtz-Zentrum Dresden-Rossendorf", self.body)

    def test_research_statement_is_gone(self):
        for marker in (
            "nh-statement",
            "nh-statement-label",
            "nh-statement-body",
            "nh-statement-text",
        ):
            with self.subTest(marker=marker):
                self.assertNotIn(marker, self.body)
        self.assertNotIn("We develop physics-informed machine learning", self.body)
        # The hero is now only the eyebrow, the heading, the name and the
        # affiliation line; nothing of the statement band may survive it.
        self.assertNotRegex(self.hero, r"(?i)nh-(statement|actions|hero-open|hero-note|hero-links)")

    def test_hero_action_links_are_gone(self):
        for marker in ("nh-actions", "nh-link-arrow", "nh-hero-open"):
            with self.subTest(marker=marker):
                self.assertNotIn(marker, self.body)
        self.assertNotIn("Research areas", self.body)
        self.assertNotIn("Selected publications", self.body)
        self.assertNotIn("Join us", self.body)
        self.assertNotIn("HZDR job portal", self.body)

    def test_biography_note_is_gone(self):
        self.assertNotIn("He received his Ph.D.", self.body)
        self.assertNotIn("postdoctoral fellow", self.body)
        self.assertNotIn("Sandia", self.body)
        self.assertNotIn("nh-hero-note", self.body)

    def test_elsewhere_band_is_gone(self):
        self.assertNotIn("Elsewhere", self.body)
        self.assertNotIn("nh-hero-links", self.body)
        self.assertNotIn("CASUS department website", self.body)

    def test_monogram_badge_and_metadata_are_gone(self):
        for marker in ("nh-monogram", "nh-monogram-disc", "nh-monogram-meta", "ML4MD"):
            with self.subTest(marker=marker):
                self.assertNotIn(marker, self.body)

    def test_hero_grid_holds_the_lead_and_the_head(self):
        # The head's name and affiliation fill the grid's second column, where
        # the monogram used to sit; the lead holds the department heading only.
        grid = self.body.split('class="nh-hero-grid"', 1)[1].split("</section>", 1)[0]
        self.assertEqual(grid.count('class="nh-hero-lead"'), 1)
        self.assertEqual(grid.count('class="nh-role"'), 1)
        self.assertLess(grid.index('class="nh-hero-lead"'), grid.index('class="nh-role"'))
        self.assertNotRegex(grid, r"<div[^>]*>\s*</div>", "empty wrapper left behind")

    def test_kept_front_matter_flags(self):
        self.assertEqual(self.front_matter.get("permalink"), "/")
        self.assertEqual(self.front_matter.get("layout"), "about")
        self.assertTrue(self.front_matter.get("social"), "social icons stay at the page foot")
        announcements = self.front_matter.get("announcements")
        self.assertIsInstance(announcements, dict)
        self.assertTrue(announcements.get("enabled"))


class HomepageSections(unittest.TestCase):
    """Software spotlight and selected publications leave the homepage."""

    @classmethod
    def setUpClass(cls):
        cls.text = ABOUT_PAGE.read_text(encoding="utf-8")
        cls.front_matter, cls.body = split_front_matter(cls.text)

    def test_software_spotlight_section_is_gone(self):
        for marker in (
            'id="software"',
            "nh-software",
            "nh-feats",
            "nh-code",
            "Spotlight",
            "Materials Learning Algorithms",
            "Also maintained",
        ):
            with self.subTest(marker=marker):
                self.assertNotIn(marker, self.body)
        self.assertNotIn("mala-project.github.io", self.body)
        self.assertNotIn("github.com/mala-project/mala", self.body)
        self.assertNotIn("atoMEC", self.body)

    def test_selected_publications_are_disabled(self):
        self.assertFalse(
            self.front_matter.get("selected_papers"),
            "selected_papers must be absent or false so the layout omits the list",
        )
        self.assertNotIn("selected_papers: true", self.text)

    def test_research_grid_and_visualization_are_preserved(self):
        self.assertIn('id="research"', self.body)
        self.assertEqual(self.body.count('class="nh-area-card"'), 5)
        self.assertIn('id="dynamics"', self.body)
        self.assertIn("latent_dynamics.liquid", self.body)

    def test_section_styles_that_are_still_used_survive(self):
        # The scoped <style> block styles the research grid and the
        # visualization spacing; it belongs to the kept content.
        self.assertIn(".nh-area-grid", self.body)
        self.assertIn(".nh-viz-section", self.body)


class SoftwarePageKept(unittest.TestCase):
    """The software page and its URL survive; only its navbar entry is off."""

    @classmethod
    def setUpClass(cls):
        cls.text = SOFTWARE_PAGE.read_text(encoding="utf-8")
        cls.front_matter, cls.body = split_front_matter(cls.text)

    def test_page_and_url_are_retained(self):
        self.assertEqual(self.front_matter.get("permalink"), "/software/")
        self.assertEqual(self.front_matter.get("layout"), "page")
        self.assertEqual(self.front_matter.get("title"), "Software")

    def test_navigation_entry_is_disabled(self):
        self.assertIs(self.front_matter.get("nav"), False)
        self.assertNotIn("nav: true", self.text)

    def test_content_is_untouched(self):
        self.assertIn("## MALA — Materials Learning Algorithms", self.body)
        self.assertIn("## atoMEC — average-atom modelling for warm dense matter", self.body)
        self.assertIn("git clone https://github.com/mala-project/mala.git", self.body)
        self.assertIn("pip install atoMEC", self.body)
        self.assertIn("10.5281/zenodo.5557254", self.body)

    def test_no_page_advertises_software_in_the_navbar(self):
        offenders = []
        for page in sorted(PAGES_DIR.glob("*.md")):
            front_matter, _ = split_front_matter(page.read_text(encoding="utf-8"))
            if front_matter.get("permalink") == "/software/" and front_matter.get("nav"):
                offenders.append(page.name)
        self.assertEqual(offenders, [], "a page still asks the navbar to show /software/")


if __name__ == "__main__":
    unittest.main()
