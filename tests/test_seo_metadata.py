"""SEO metadata guardrails: one real title, accurate descriptions, canonicals.

The site is a research-group site, not a keyword farm: the title must name the
department, the description must say who the researcher is and where they work,
and every indexable page must carry its own description instead of repeating
the site-wide one.

Build-independent checks read ``_config.yml`` and ``_includes/head.liquid`` and
run on a plain checkout:

    python -m unittest tests.test_seo_metadata -v

Rendered checks need a built site (they skip otherwise):

    SITE_DIR=/tmp/seo-site python -m unittest tests.test_seo_metadata -v

The template checks render ``_includes/metadata.liquid`` and
``_includes/head.liquid`` with python-liquid when it is installed; they skip
otherwise so a plain checkout still runs the build-independent checks.
"""

import functools
import html as html_lib
import json
import os
import pathlib
import re
import unittest
import xml.etree.ElementTree as ET

import yaml

try:  # Optional: lets the template checks render the real Liquid includes.
    from liquid import Environment, FileSystemLoader
except ImportError:  # pragma: no cover - depends on the developer environment
    Environment = FileSystemLoader = None

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "_config.yml"
INCLUDES_DIR = ROOT / "_includes"
HEAD_PATH = INCLUDES_DIR / "head.liquid"
METADATA_PATH = INCLUDES_DIR / "metadata.liquid"
PAGES_DIR = ROOT / "_pages"
DATA_DIR = ROOT / "_data"

SITE_TITLE = "Machine Learning for Materials Design"
SITE_URL = "https://acangi.github.io"  # asserted once; rendered checks use the configured url

# Search terms the site should be findable for. They must appear in the
# metadata, but as distinct phrases -- no repetition to game a ranking.
REQUIRED_KEYWORDS = (
    "attila cangi",
    "machine learning",
    "electronic structure",
    "representation learning",
    "chemical space",
    "thz",
)

# Claims a research site cannot substantiate in metadata.
RANKING_CLAIMS = re.compile(
    r"\b(best|top[- ]?rated|#1|number one|world[- ]leading|leading expert|"
    r"award[- ]winning|ranked|unrivalled|unrivaled)\b",
    re.IGNORECASE,
)


def load_config():
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def front_matter(path):
    text = path.read_text(encoding="utf-8")
    match = re.match(r"---\s*\n(.*?)\n---", text, re.S)
    return yaml.safe_load(match.group(1)) or {} if match else {}


def nav_pages():
    """{url: front matter} for the homepage and the pages linked from the navbar."""
    pages = {}
    for path in sorted(PAGES_DIR.glob("*.md")):
        data = front_matter(path)
        permalink = data.get("permalink") or "/" + path.stem + "/"
        if not data.get("nav") and permalink != "/":
            continue
        pages[permalink] = dict(data, _source=path)
    return pages


def defaults_by_scope(config):
    """{scope path: values} for every front-matter default in ``_config.yml``."""
    defaults = {}
    for entry in config.get("defaults") or []:
        scope = (entry.get("scope") or {}).get("path")
        if scope:
            defaults[scope] = entry.get("values") or {}
    return defaults


def site_config():
    """The Jekyll config as the templates see it, including ``site.data``."""
    config = load_config()
    data = {}
    for path in sorted(DATA_DIR.glob("*.yml")):
        data[path.stem] = yaml.safe_load(path.read_text(encoding="utf-8"))
    config["data"] = data
    return config


# --- Liquid rendering of the real includes -----------------------------------
# Jekyll's Liquid is Ruby; these checks use python-liquid when it is installed
# so the template logic is exercised, not just grepped. Jekyll-only filters
# are stubbed with the same semantics.


def _filter_context(fn):
    """Adapt a (value, *args) filter to python-liquid's (context, value, *args)."""
    def wrapped(context, value, *args):
        return fn(value, *args)

    return wrapped


def _jsonify(value):
    return json.dumps(value, ensure_ascii=False)


@functools.lru_cache(maxsize=1)
def liquid_environment():
    env = Environment(loader=FileSystemLoader(str(INCLUDES_DIR)))
    env.filters["push"] = _filter_context(lambda values, item: list(values or []) + [item])
    env.filters["remove_first"] = _filter_context(
        lambda value, needle: value.replace(needle, "", 1) if value else value
    )
    env.filters["jsonify"] = _filter_context(_jsonify)
    env.filters["normalize_whitespace"] = _filter_context(
        lambda value: re.sub(r"\s+", " ", str(value or "")).strip()
    )
    env.filters["relative_url"] = _filter_context(lambda value, *args: value)
    env.filters["absolute_url"] = _filter_context(
        lambda value, *args: (load_config().get("url") or "") + (value or "")
    )
    env.filters["bust_file_cache"] = _filter_context(lambda value, *args: value)
    env.filters["bust_css_cache"] = _filter_context(lambda value, *args: value)
    return env


def render_metadata(page, site=None):
    return liquid_environment().get_template("metadata.liquid").render(
        {"site": site or site_config(), "page": page}
    )


HEAD_INCLUDE_RE = re.compile(r"{%\s*include\s+metadata\.liquid\s*%}")


def render_head(page, site=None):
    """Render head.liquid with the metadata include inlined separately."""
    source = HEAD_INCLUDE_RE.sub("", HEAD_PATH.read_text(encoding="utf-8"))
    template = liquid_environment().parse(source)
    return template.render({"site": site or site_config(), "page": page})


class HtmlMetadata:
    """Minimal reader for the head metadata the tests care about."""

    def __init__(self, html):
        self.html = html
        self.titles = [
            re.sub(r"\s+", " ", match).strip()
            for match in re.findall(r"<title[^>]*>(.*?)</title>", html, re.S)
        ]
        self.meta = {}
        for attrs in re.findall(r"<meta\b([^>]*)>", html):
            name = re.search(r'(?:name|property)\s*=\s*"([^"]+)"', attrs)
            content = re.search(r'content\s*=\s*"([^"]*)"', attrs)
            if name and content:
                self.meta.setdefault(name.group(1).lower(), []).append(content.group(1))
        self.canonicals = re.findall(r'<link\b[^>]*rel="canonical"[^>]*>', html)
        self.json_ld = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S
        )

    def contents(self, key):
        return self.meta.get(key.lower(), [])

    def content(self, key, default=None):
        values = self.contents(key)
        return values[0] if values else default

    def canonical_url(self):
        if len(self.canonicals) != 1:
            return None
        match = re.search(r'href="([^"]+)"', self.canonicals[0])
        return match.group(1) if match else None


class SiteConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config()
        cls.description = (cls.config.get("description") or "").strip()
        cls.keywords = (cls.config.get("keywords") or "").strip()

    def test_site_title_names_the_department(self):
        self.assertEqual(self.config.get("title"), SITE_TITLE)

    def test_researcher_identity_is_preserved(self):
        self.assertEqual(self.config.get("first_name"), "Attila")
        self.assertEqual(self.config.get("last_name"), "Cangi")
        self.assertIn("Attila Cangi", self.description)
        for affiliation in ("CASUS", "Helmholtz-Zentrum Dresden-Rossendorf"):
            with self.subTest(affiliation=affiliation):
                self.assertIn(affiliation, self.description)

    def test_description_fits_a_search_snippet(self):
        self.assertGreaterEqual(len(self.description), 110)
        self.assertLessEqual(len(self.description), 160, self.description)
        self.assertNotIn("\n", self.description)
        self.assertNotRegex(self.description, RANKING_CLAIMS)

    def test_keywords_are_distinct_relevant_terms(self):
        terms = [term.strip() for term in self.keywords.split(",") if term.strip()]
        self.assertGreaterEqual(len(terms), 6)
        self.assertLessEqual(len(terms), 12, "keyword lists this long are stuffing")
        lowered = [term.lower() for term in terms]
        self.assertEqual(len(lowered), len(set(lowered)), "duplicate keyword")
        joined = " | ".join(lowered)
        for required in REQUIRED_KEYWORDS:
            with self.subTest(term=required):
                self.assertIn(required, joined)

    def test_site_url_and_sitemap_are_absolute(self):
        self.assertEqual(self.config["url"], SITE_URL)
        self.assertFalse(self.config["url"].endswith("/"))
        self.assertIn("jekyll-sitemap", self.config["plugins"])

    def test_every_nav_page_gets_its_own_description(self):
        """Pages without their own description must get one via defaults."""
        defaults = defaults_by_scope(self.config)
        description_keys = ("seo_description", "description")
        for url, data in nav_pages().items():
            if url == "/":
                continue
            with self.subTest(page=data["_source"].name):
                relative = data["_source"].relative_to(ROOT).as_posix()
                values = defaults.get(relative, {})
                own = any(data.get(key) for key in description_keys + ("summary",))
                self.assertTrue(
                    own or any(values.get(key) for key in description_keys),
                    f"{relative} needs its own description or a description default",
                )

    def test_seo_defaults_do_not_render_as_page_text(self):
        """A default must not fill page.description, or page.liquid prints it."""
        defaults = defaults_by_scope(self.config)
        scoped_pages = (
            "_pages/research.md",
            "_pages/publications.md",
            "_pages/teaching.md",
            "_pages/cv.md",
            "_pages/impressum.md",
        )
        for scope in scoped_pages:
            with self.subTest(scope=scope):
                values = defaults.get(scope, {})
                self.assertTrue(
                    values.get("seo_description"),
                    f"{scope} needs an seo_description default",
                )
                self.assertNotIn(
                    "description",
                    values,
                    "a description default would be rendered as the page subtitle",
                )

    def test_publication_page_keeps_its_own_description(self):
        """The page keeps its own search copy, but as a scoped default.

        `page.liquid` renders `page.description` above the article, and the
        page's subtitle was removed on request.  The scoped `seo_description`
        default in `_config.yml` still gives the page unique metadata.
        """
        data = front_matter(PAGES_DIR / "publications.md")
        self.assertNotIn("description", data)
        self.assertNotIn("seo_description", data)
        values = defaults_by_scope(load_config()).get("_pages/publications.md", {})
        self.assertTrue(values.get("seo_description"))

    def test_no_blanket_description_default_that_would_hide_page_text(self):
        """A global default would make every page look identical to crawlers."""
        for entry in self.config.get("defaults") or []:
            scope = (entry.get("scope") or {}).get("path") or ""
            values = entry.get("values") or {}
            with self.subTest(scope=scope):
                for key in ("description", "seo_description"):
                    self.assertFalse(
                        scope == "" and values.get(key),
                        "a site-wide description default duplicates the site description",
                    )


class HeadTemplateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.head = HEAD_PATH.read_text(encoding="utf-8")

    def test_head_emits_exactly_one_canonical(self):
        self.assertEqual(self.head.count('rel="canonical"'), 1)

    def test_canonical_is_absolute(self):
        self.assertIn("absolute_url", self.head)

    def test_non_indexable_pages_are_marked(self):
        self.assertIn("noindex", self.head)
        self.assertIn("404", self.head)

    def test_head_does_not_duplicate_description_metadata(self):
        """metadata.liquid owns title/description; head must not emit a second."""
        self.assertNotIn('name="description"', self.head)
        self.assertNotIn('name="keywords"', self.head)

    def test_person_structured_data_is_configurable(self):
        self.assertIn("person_schema", self.head)


class MetadataRenderTests(unittest.TestCase):
    """Render the real metadata include and check what crawlers receive."""

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_homepage_social_titles_match_the_document_title(self):
        meta = HtmlMetadata(
            render_metadata({"url": "/", "title": "About", "permalink": "/"})
        )
        self.assertEqual(meta.titles, [SITE_TITLE])
        for key in ("og:title", "twitter:title", "og:site_name"):
            with self.subTest(key=key):
                self.assertEqual(meta.content(key), SITE_TITLE)

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_page_social_titles_match_the_document_title(self):
        page = {"url": "/research/", "title": "Research", "permalink": "/research/"}
        meta = HtmlMetadata(render_metadata(page))
        expected = f"Research | {SITE_TITLE}"
        self.assertEqual(meta.titles, [expected])
        self.assertEqual(meta.content("og:title"), expected)
        self.assertEqual(meta.content("twitter:title"), expected)

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_special_characters_in_titles_are_escaped(self):
        page = {"url": "/r/", "title": "Research & Development", "permalink": "/r/"}
        meta = HtmlMetadata(render_metadata(page))
        expected = f"Research & Development | {SITE_TITLE}"
        self.assertEqual(html_lib.unescape(meta.titles[0]), expected)
        self.assertEqual(html_lib.unescape(meta.content("og:title")), expected)
        self.assertEqual(html_lib.unescape(meta.content("twitter:title")), expected)

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_seo_description_takes_precedence(self):
        page = {
            "url": "/research/",
            "title": "Research",
            "description": "Subtitle shown on the page.",
            "seo_description": "Description written for search results.",
        }
        meta = HtmlMetadata(render_metadata(page))
        self.assertEqual(meta.content("description"), "Description written for search results.")
        self.assertEqual(meta.content("og:description"), meta.content("description"))
        self.assertEqual(meta.content("twitter:description"), meta.content("description"))

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_page_description_is_used_without_seo_description(self):
        page = {
            "url": "/publications/",
            "title": "Publications",
            "description": "Peer-reviewed articles and preprints.",
        }
        meta = HtmlMetadata(render_metadata(page))
        self.assertEqual(meta.content("description"), "Peer-reviewed articles and preprints.")
        self.assertEqual(meta.content("og:description"), meta.content("description"))

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_highlight_summary_describes_the_page(self):
        page = {
            "url": "/highlights/example/",
            "title": "Example highlight",
            "summary": "  A machine-learned\ninteratomic potential   for iron\n" + "at extreme conditions " * 12,
        }
        meta = HtmlMetadata(render_metadata(page))
        description = meta.content("description")
        self.assertTrue(description.startswith("A machine-learned interatomic potential"))
        self.assertNotIn("\n", description)
        self.assertLessEqual(len(description), 170)
        self.assertNotEqual(description, load_config()["description"])
        self.assertEqual(meta.content("og:description"), description)
        self.assertEqual(meta.content("twitter:description"), description)

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_highlight_summary_is_escaped(self):
        summary = 'Spin "waves" & <lattice> dynamics in iron'
        page = {"url": "/highlights/example/", "title": "Example", "summary": summary}
        raw = render_metadata(page)
        match = re.search(r'<meta name="description" content="([^"]*)"', raw)
        self.assertIsNotNone(match, "a raw quote in the summary broke the meta tag")
        self.assertEqual(html_lib.unescape(match.group(1)), summary)
        self.assertNotIn("<lattice>", raw)

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_site_description_is_the_last_resort(self):
        meta = HtmlMetadata(render_metadata({"url": "/plain/", "title": "Plain"}))
        self.assertEqual(meta.content("description"), load_config()["description"])

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_one_description_tag_per_render(self):
        meta = HtmlMetadata(render_metadata({"url": "/plain/", "title": "Plain"}))
        self.assertEqual(len(meta.contents("description")), 1)


class ImageAltRenderTests(unittest.TestCase):
    """og:image:alt must not depend on a site-wide alt being configured."""

    def head(self, page, site=None):
        return HtmlMetadata(render_head(page, site if site is not None else site_config()))

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_page_specific_alt_is_emitted_without_site_alt(self):
        site = site_config()
        self.assertFalse(site.get("og_image_alt"))
        page = {
            "url": "/research/",
            "title": "Research",
            "og_image": "/assets/img/research.png",
            "og_image_alt": 'Iron core under "extreme" conditions & pressure',
        }
        meta = self.head(page, site)
        expected = 'Iron core under "extreme" conditions & pressure'
        self.assertEqual(html_lib.unescape(meta.content("og:image:alt")), expected)
        self.assertEqual(html_lib.unescape(meta.content("twitter:image:alt")), expected)

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_site_alt_is_the_fallback(self):
        site = site_config()
        site["og_image_alt"] = "Department preview image"
        meta = self.head(
            {"url": "/research/", "title": "Research", "og_image": "/assets/img/research.png"},
            site,
        )
        self.assertEqual(meta.content("og:image:alt"), "Department preview image")

    @unittest.skipUnless(Environment is not None, "python-liquid is not installed")
    def test_no_alt_without_an_image(self):
        meta = self.head(
            {"url": "/research/", "title": "Research", "og_image_alt": "Orphan alt text"}
        )
        self.assertIsNone(meta.content("og:image:alt"))
        self.assertIsNone(meta.content("twitter:image:alt"))


def build_site_available():
    return bool(os.environ.get("SITE_DIR"))


@unittest.skipUnless(build_site_available(), "set SITE_DIR to a built site directory")
class RenderedMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.site = pathlib.Path(os.environ["SITE_DIR"])
        assert cls.site.is_dir(), f"Built site directory not found: {cls.site}"
        cls.config = load_config()
        cls.base_url = cls.config["url"].rstrip("/")
        cls.pages = nav_pages()
        cls.html = {}
        for url in cls.pages:
            target = cls.site / url.strip("/") / "index.html" if url != "/" else cls.site / "index.html"
            if target.is_file():
                cls.html[url] = HtmlMetadata(target.read_text(encoding="utf-8"))

    def test_every_nav_page_was_built(self):
        self.assertEqual(sorted(self.html), sorted(self.pages))

    def test_homepage_title_is_the_site_title(self):
        home = self.html["/"]
        self.assertEqual(home.titles, [SITE_TITLE])
        self.assertEqual(home.content("og:site_name"), SITE_TITLE)

    def test_page_titles_are_page_then_site(self):
        for url, data in self.pages.items():
            if url == "/":
                continue
            with self.subTest(page=url):
                expected = f"{data['title']} | {SITE_TITLE}"
                self.assertEqual(self.html[url].titles, [expected])

    def test_social_titles_match_the_document_title(self):
        for url, meta in self.html.items():
            with self.subTest(page=url):
                self.assertTrue(meta.titles)
                self.assertEqual(meta.content("og:title"), meta.titles[0])
                self.assertEqual(meta.content("twitter:title"), meta.titles[0])

    def test_exactly_one_description_per_page(self):
        for url, meta in self.html.items():
            with self.subTest(page=url):
                self.assertEqual(len(meta.contents("description")), 1)

    def test_description_is_consistent_across_social_tags(self):
        for url, meta in self.html.items():
            description = meta.content("description")
            with self.subTest(page=url):
                self.assertIsNotNone(description)
                self.assertEqual(meta.content("og:description"), description)
                self.assertEqual(meta.content("twitter:description"), description)

    def test_each_page_has_a_real_description(self):
        site_description = self.config["description"].strip()
        for url, meta in self.html.items():
            description = (meta.content("description") or "").strip()
            with self.subTest(page=url):
                self.assertGreaterEqual(len(description), 40, description)
                self.assertLessEqual(len(description), 170, description)
                if url != "/":
                    self.assertNotEqual(
                        description, site_description, f"{url} reuses the site description"
                    )

    def test_descriptions_are_distinct_between_pages(self):
        seen = {}
        for url, meta in self.html.items():
            description = meta.content("description")
            with self.subTest(page=url):
                self.assertNotIn(description, seen, f"same description as {seen.get(description)}")
            seen[description] = url

    def test_no_ranking_claims_in_metadata(self):
        for url, meta in self.html.items():
            text = " ".join(meta.contents("description") + meta.contents("keywords"))
            with self.subTest(page=url):
                self.assertNotRegex(text, RANKING_CLAIMS)

    def test_canonicals_are_absolute_and_unique(self):
        urls = set()
        for url, meta in self.html.items():
            canonical = meta.canonical_url()
            with self.subTest(page=url):
                self.assertIsNotNone(canonical)
                self.assertTrue(canonical.startswith(self.base_url + "/"), canonical)
                self.assertNotIn("index.html", canonical)
                self.assertNotIn(canonical, urls)
            urls.add(canonical)

    def test_homepage_canonical_is_the_root(self):
        self.assertEqual(self.html["/"].canonical_url(), self.base_url + "/")

    def test_person_structured_data(self):
        meta = self.html["/"]
        self.assertTrue(meta.json_ld, "no JSON-LD emitted")
        blocks = []
        for raw in meta.json_ld:
            with self.subTest(block=raw[:40]):
                blocks.append(json.loads(raw))
        people = [block for block in blocks if block.get("@type") == "Person"]
        self.assertEqual(len(people), 1, "expected exactly one Person block")
        person = people[0]
        self.assertEqual(person["name"], "Attila Cangi")
        self.assertIn("CASUS", person["affiliation"]["name"])
        same_as = " ".join(person.get("sameAs", []))
        self.assertIn("orcid.org/0000-0001-9162-262X", same_as)

    def test_sitemap_lists_pages_and_skips_assets(self):
        sitemap = self.site / "sitemap.xml"
        self.assertTrue(sitemap.is_file(), "jekyll-sitemap did not run")
        root = ET.parse(sitemap).getroot()
        namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locations = {node.text for node in root.findall("sm:url/sm:loc", namespace)}
        for url in self.pages:
            with self.subTest(page=url):
                self.assertIn(self.base_url + url, locations)
        self.assertFalse([loc for loc in locations if "/assets/" in (loc or "")])
        self.assertNotIn(self.base_url + "/404.html", locations)

    def test_robots_points_at_the_sitemap(self):
        robots = (self.site / "robots.txt").read_text(encoding="utf-8")
        self.assertIn(f"Sitemap: {self.base_url}/sitemap.xml", robots)

    def test_404_is_not_indexable(self):
        page = self.site / "404.html"
        self.assertTrue(page.is_file())
        meta = HtmlMetadata(page.read_text(encoding="utf-8"))
        self.assertEqual(meta.content("robots"), "noindex, follow")
        self.assertIsNone(meta.canonical_url())


if __name__ == "__main__":
    unittest.main()
