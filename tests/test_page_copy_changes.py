"""Copy guards for the requested subtitle/subheading removals.

The user asked for the visible subtitles on the join, news, publications and
research-highlights pages to disappear, for two explanatory sentences (join and
news) and one trailing sentence (teaching) to be removed, for the highlights
count line to disappear, and for the research page's ``<h1>`` to read
``Research`` instead of the department name.

``_layouts/page.liquid`` renders ``page.description`` as a visible subtitle, so
the subtitle copy must move to ``seo_description`` (which
``_includes/metadata.liquid`` prefers for the meta description) rather than
being deleted outright.  These checks are build-independent; the rendered
checks at the bottom run when ``SITE_DIR`` points at a built site.

    python -m unittest tests.test_page_copy_changes -v
    SITE_DIR=/tmp/copy-site python -m unittest tests.test_page_copy_changes -v
"""

import html as html_lib
import os
import pathlib
import re
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
PAGES = ROOT / "_pages"
CONFIG = ROOT / "_config.yml"

TARGET_PAGES = (
    "join.md",
    "news.md",
    "teaching.md",
    "publications.md",
    "research.md",
    "highlights.md",
)

# Copy that must no longer appear anywhere in the page source (front matter or
# body).  Compared case-insensitively after whitespace normalisation because
# the theme renders ``.post-description`` and ``.highlight-count`` uppercase.
REMOVED_COPY = {
    "join.md": (
        "Opportunities at the Department of Machine Learning for Materials Design",
        "We work at the intersection of quantum theory, machine learning, "
        "and scientific computing at CASUS / HZDR in Görlitz.",
    ),
    "news.md": (
        "Recent announcements from the department",
        "Short updates on releases, talks, awards, and new team members.",
    ),
    "teaching.md": (
        "The teaching material can be accessed on my institutional website.",
    ),
    "publications.md": (
        "Peer-reviewed journal articles and preprints by the Department of "
        "Machine Learning for Materials Design.",
    ),
    "highlights.md": (
        "Research outputs from the department, each linked to its publication.",
        "highlights, newest first.",
    ),
}

# Subtitle copy that should survive as SEO metadata rather than disappear.
SEO_COPY = {
    "join.md": "Opportunities at the Department of Machine Learning for Materials Design",
    "news.md": "Recent announcements from the department",
    "highlights.md": "Research outputs from the department, each linked to its publication.",
}


def read_page(name):
    return (PAGES / name).read_text(encoding="utf-8")


def front_matter(name):
    """Return (metadata dict, body, full source) for a page."""
    source = read_page(name)
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", source, re.S)
    if not match:
        raise AssertionError(f"{name} has no YAML front matter")
    metadata = yaml.safe_load(match.group(1)) or {}
    return metadata, match.group(2), source


def normalize(value):
    return re.sub(r"\s+", " ", value).strip()


def page_defaults():
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    return {
        (entry.get("scope") or {}).get("path"): entry.get("values") or {}
        for entry in config.get("defaults") or []
    }


class RemovedCopyTests(unittest.TestCase):
    def test_requested_copy_is_gone_from_page_body(self):
        """Removed copy must not be in the page body; seo_description may keep it."""
        for name, phrases in REMOVED_COPY.items():
            with self.subTest(page=name):
                _, body, _ = front_matter(name)
                body = normalize(body).casefold()
                for phrase in phrases:
                    self.assertNotIn(
                        normalize(phrase).casefold(),
                        body,
                        f"{name} still contains removed copy: {phrase!r}",
                    )

    def test_no_page_renders_a_description_subtitle(self):
        """page.liquid prints page.description; no target page may set it."""
        for name in TARGET_PAGES:
            with self.subTest(page=name):
                metadata, _, _ = front_matter(name)
                self.assertNotIn(
                    "description",
                    metadata,
                    f"{name} sets description, which page.liquid renders as a subtitle",
                )

    def test_research_title_is_research(self):
        metadata, _, _ = front_matter("research.md")
        self.assertEqual(metadata.get("title"), "Research")
        self.assertNotIn(
            "heading",
            metadata,
            "page.liquid prefers `heading` over `title`, so it must be absent",
        )


class SeoPreservationTests(unittest.TestCase):
    def test_removed_subtitles_survive_as_seo_descriptions(self):
        for name, expected in SEO_COPY.items():
            with self.subTest(page=name):
                metadata, _, _ = front_matter(name)
                self.assertEqual(
                    normalize(str(metadata.get("seo_description", ""))),
                    normalize(expected),
                    f"{name} should keep the subtitle copy for search engines",
                )

    def test_every_target_page_still_has_a_description_for_search(self):
        defaults = page_defaults()
        for name in TARGET_PAGES:
            with self.subTest(page=name):
                metadata, _, _ = front_matter(name)
                own = metadata.get("seo_description") or metadata.get("description")
                scoped = defaults.get(f"_pages/{name}") or {}
                self.assertTrue(
                    own or scoped.get("seo_description") or scoped.get("description"),
                    f"{name} needs an seo_description (page or _config default)",
                )


class PreservedContentTests(unittest.TestCase):
    def test_join_keeps_opportunities_and_contact(self):
        _, body, _ = front_matter("join.md")
        self.assertIn("## Opportunities", body)
        self.assertIn("HZDR vacancies page", body)
        self.assertIn("a.cangi@hzdr.de", body)

    def test_news_keeps_the_listing(self):
        _, body, _ = front_matter("news.md")
        self.assertIn("{% include news.liquid %}", body)

    def test_teaching_keeps_the_course_table(self):
        _, body, _ = front_matter("teaching.md")
        self.assertIn('<table class="table table-sm">', body)
        self.assertIn("Festkörperphysikalische Grundlagen", body)
        self.assertIn("Atomistic Simulation Methods", body)
        self.assertEqual(body.count("<tr>"), 6, "header row plus five course rows")

    def test_publications_keeps_the_bibliography(self):
        _, body, _ = front_matter("publications.md")
        self.assertIn("{% publications_bibliography --query @* %}", body)

    def test_research_keeps_intro_and_all_areas(self):
        _, body, _ = front_matter("research.md")
        self.assertIn(
            "Our department develops theoretical and computational methods",
            body,
        )
        self.assertIn("research highlights", body)
        for anchor in (
            "electronic-structure",
            "representation-learning",
            "quantum-dynamics",
            "magnetic-materials",
            "thz-driven-systems",
        ):
            with self.subTest(anchor=anchor):
                self.assertIn(f"{{: #{anchor} }}", body)
                self.assertIn(f'aria-labelledby="{anchor}"', body)

    def test_highlights_keeps_the_card_list(self):
        _, body, _ = front_matter("highlights.md")
        self.assertIn("{% for h in highlights %}", body)
        self.assertIn("highlight-card", body)
        self.assertIn("doi.org/", body)


SITE_DIR = os.getenv("SITE_DIR", "")


@unittest.skipUnless(SITE_DIR, "set SITE_DIR to a built site to run rendered checks")
class RenderedCopyTests(unittest.TestCase):
    PAGES = ("join", "news", "teaching", "publications", "research", "highlights")

    @classmethod
    def setUpClass(cls):
        cls.site = pathlib.Path(SITE_DIR)

    def rendered(self, slug):
        path = self.site / slug / "index.html"
        self.assertTrue(path.is_file(), f"missing built page {path}")
        return path.read_text(encoding="utf-8")

    def visible_text(self, markup):
        markup = re.sub(
            r"<(script|style)\b.*?</\1>", " ", markup, flags=re.S | re.I
        )
        text = html_lib.unescape(re.sub(r"<[^>]+>", " ", markup))
        return normalize(text)

    def test_description_subtitles_are_visually_absent(self):
        for slug in self.PAGES:
            with self.subTest(page=slug):
                markup = self.rendered(slug)
                descriptions = re.findall(
                    r'<p class="post-description">(.*?)</p>', markup, re.S
                )
                self.assertTrue(
                    all(not normalize(value) for value in descriptions),
                    f"/{slug}/ still renders a visible subtitle: {descriptions}",
                )

    def test_research_h1_says_research(self):
        markup = self.rendered("research")
        match = re.search(r"<h1 class=\"post-title\">(.*?)</h1>", markup, re.S)
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(normalize(re.sub(r"<[^>]+>", "", match.group(1))), "Research")

    def test_removed_copy_is_not_in_rendered_pages(self):
        for slug in (name.removesuffix(".md") for name in REMOVED_COPY):
            with self.subTest(page=slug):
                text = self.visible_text(self.rendered(slug)).casefold()
                for phrase in REMOVED_COPY[f"{slug}.md"]:
                    self.assertNotIn(normalize(phrase).casefold(), text)


if __name__ == "__main__":
    unittest.main()
