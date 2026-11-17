"""Rendering and style checks for the team page.

The tests do not need a Jekyll/Ruby install.  The page body is rendered with
``python-liquid`` against synthetic ``site.data.team`` fixtures, so every rule
below is exercised through the real Liquid template rather than a copy of its
logic.  A small source-level class still runs when ``python-liquid`` is absent.

Run with::

    python -m unittest tests.test_team_rendering -v

The data contract is deliberately narrow: ``name``, ``role``, ``email``,
``start_year``, ``end_year``, ``last_position`` and optional ``photo``.
Current members never show years; alumni show only the years that exist and a
single year when start and end are equal.
"""

from __future__ import annotations

import html
import pathlib
import re
import unittest
import xml.etree.ElementTree as ET

import yaml

try:  # pragma: no cover - exercised implicitly by the environment
    from liquid import Environment
except ImportError:  # pragma: no cover - CI without python-liquid
    Environment = None  # type: ignore[assignment]

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEAM_PAGE = REPO_ROOT / "_pages" / "team.md"
TEAM_SASS = REPO_ROOT / "_sass" / "_team.scss"
MAIN_SCSS = REPO_ROOT / "assets" / "css" / "main.scss"

BASE = "/BASE"
# Decorative, gender-neutral fallback shown when a person has no photo.
PLACEHOLDER_URL = "/assets/img/team/avatar-neutral.svg"
PLACEHOLDER_PATH = REPO_ROOT / PLACEHOLDER_URL.lstrip("/")

HEAD = {
    "name": "Attila Cangi, Ph.D.",
    "email": "a.cangi@hzdr.de",
    "photo": "/assets/img/team/Attila_Cangi.jpg",
    "cv": "/cv/",
    "orcid": "https://orcid.org/0000-0001-9162-262X",
    "aliases": ["Attila Cangi", "Prof. Dr. Attila Cangi"],
}

TEAM = [
    {
        "title": "Current members",
        "members": [
            {
                "name": "Dr. Zoë Ångström",
                "role": "Postdoctoral researcher",
                "email": "zoe@example.org",
                "start_year": 2021,
                "end_year": None,
                "photo": "/assets/img/team/zoe.jpg",
            },
            {
                "name": "Sam <Test> & Co",
                "role": "Doctoral researcher",
                "email": "sam@example.org",
                "start_year": 2023,
                "end_year": None,
            },
        ],
    },
    {
        "title": "Alumni",
        "members": [
            {
                "name": "Dr. Former One",
                "role": "Postdoctoral researcher",
                "email": "former@example.org",
                "start_year": 2018,
                "end_year": 2021,
                "last_position": "Research scientist, TU Dresden",
            },
            {
                "name": "Dr. Same Year",
                "role": "Doctoral researcher",
                "start_year": 2022,
                "end_year": 2022,
            },
            {
                "name": "Dr. Start Only",
                "role": "Doctoral researcher",
                "start_year": 2020,
            },
            {
                "name": "Dr. End Only",
                "role": "Doctoral researcher",
                "end_year": 2024,
            },
        ],
    },
]

CARD_RE = re.compile(r'<article class="team-card[^"]*">(.*?)</article>', re.S)
NAME_RE = re.compile(r'<h3 class="team-card-name">(.*?)</h3>', re.S)


def read_page():
    """Return the page's front matter and body."""
    text = TEAM_PAGE.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise AssertionError("team.md must start with YAML front matter")
    end = text.index("\n---", 3)
    front_matter = yaml.safe_load(text[4:end])
    body = text[end + 4 :]
    return front_matter, body


def render(team_data, head=None):
    """Render the page body with ``team_data`` as ``site.data.team``."""
    if Environment is None:  # pragma: no cover
        raise unittest.SkipTest("python-liquid is not installed")
    env = Environment(autoescape=False, strict_filters=True)
    env.filters["relative_url"] = (
        lambda _context, value: BASE + value
        if isinstance(value, str) and value.startswith("/")
        else value
    )
    _, body = read_page()
    return env.render(
        body,
        {
            "site": {"data": {"team": team_data}},
            "page": {"head": HEAD if head is None else head},
        },
    )


def section(rendered, heading):
    """Return the HTML between one member section heading and the next h2."""
    start = rendered.find(f">{heading}</h2>")
    if start < 0:
        raise AssertionError(f"missing section heading {heading!r}")
    next_heading = rendered.find("<h2", start + 1)
    return rendered[start:] if next_heading < 0 else rendered[start:next_heading]


def card_blocks(rendered):
    return CARD_RE.findall(rendered)


def card_names(rendered):
    names = []
    for block in card_blocks(rendered):
        match = NAME_RE.search(block)
        assert match is not None
        names.append(html.unescape(match.group(1)))
    return names


def card_for(rendered, name):
    for block in card_blocks(rendered):
        match = NAME_RE.search(block)
        if match and html.unescape(match.group(1)) == name:
            return block
    raise AssertionError(f"no card for {name!r}; rendered {card_names(rendered)!r}")


class TeamSourceTests(unittest.TestCase):
    """Cheap checks that need neither rendering nor a Ruby toolchain."""

    @classmethod
    def setUpClass(cls):
        cls.front_matter, cls.body = read_page()
        cls.sass = TEAM_SASS.read_text(encoding="utf-8")
        cls.main_scss = MAIN_SCSS.read_text(encoding="utf-8")

    def test_page_keeps_its_permalink_and_navigation(self):
        self.assertEqual(self.front_matter["permalink"], "/team/")
        self.assertEqual(self.front_matter["layout"], "page")
        self.assertTrue(self.front_matter.get("nav"))

    def test_head_metadata_keeps_cv_and_orcid_links(self):
        head = self.front_matter["head"]
        self.assertEqual(head["name"], "Attila Cangi, Ph.D.")
        self.assertEqual(head["cv"], "/cv/")
        self.assertEqual(head["orcid"], "https://orcid.org/0000-0001-9162-262X")

    def test_page_renders_from_data_and_contains_no_invented_people(self):
        self.assertIn("site.data.team", self.body)
        for invented in ("Alex Doe", "Robin Roe", "PLACEHOLDER", "20XX"):
            self.assertNotIn(invented, self.body)

    def test_template_does_not_sort_member_data(self):
        # The data feed is already ordered; sorting again can reorder alumni.
        self.assertNotRegex(self.body, r"\|\s*sort\b")

    def test_referenced_portraits_exist(self):
        head_photo = self.front_matter["head"]["photo"].lstrip("/")
        self.assertTrue((REPO_ROOT / head_photo).is_file(), head_photo)
        self.assertTrue(PLACEHOLDER_PATH.is_file(), PLACEHOLDER_URL)

    def test_team_styles_are_scoped_and_responsive(self):
        self.assertIn(".team-page {", self.sass)
        self.assertRegex(self.sass, r"grid-template-columns:\s*repeat\(auto-fill, minmax\(")
        self.assertRegex(self.sass, r"@media\s*\(max-width:\s*57[0-9](?:\.98)?px\)")
        self.assertIn("object-fit: cover", self.sass)
        self.assertIn("var(--global-card-bg-color)", self.sass)
        # No unscoped .team-* selector at the start of a line.
        self.assertNotRegex(self.sass, r"^\.team-(?!page)", )

    def test_team_stylesheet_is_imported_once(self):
        imports = re.findall(r'^\s*"team",?\s*$', self.main_scss, re.MULTILINE)
        self.assertEqual(len(imports), 1)


class PlaceholderAssetTests(unittest.TestCase):
    """The fallback avatar must be local, decorative and neutral."""

    @classmethod
    def setUpClass(cls):
        cls.text = PLACEHOLDER_PATH.read_text(encoding="utf-8")
        cls.root = ET.fromstring(cls.text)
        _, cls.body = read_page()

    def test_svg_is_a_valid_square_vector_sized_for_the_card(self):
        self.assertEqual(self.root.tag, "{http://www.w3.org/2000/svg}svg")
        self.assertEqual(self.root.get("viewBox"), "0 0 96 96")
        self.assertEqual(self.root.get("width"), "96")
        self.assertEqual(self.root.get("height"), "96")

    def test_svg_is_exposed_as_decorative(self):
        # The <img> carries alt=""; the document itself must not add a name.
        self.assertEqual(self.root.get("aria-hidden"), "true")
        self.assertEqual(self.root.get("focusable"), "false")
        self.assertIsNone(self.root.get("aria-label"))
        self.assertIsNone(self.root.get("role"))
        tags = {element.tag.rsplit("}", 1)[-1] for element in self.root.iter()}
        self.assertFalse(tags & {"text", "title", "desc"})

    def test_svg_has_no_opaque_background_so_the_card_tone_shows_through(self):
        # Dark mode swaps --nordic-stone; a baked-in light rect would glare.
        for element in self.root.iter():
            if element.tag.rsplit("}", 1)[-1] != "rect":
                continue
            width = element.get("width", "")
            height = element.get("height", "")
            self.assertNotEqual((width, height), ("96", "96"))
            self.assertNotIn(width, ("100%", "96"))

    def test_svg_uses_only_low_saturation_shape_fills(self):
        fills = set()
        for element in self.root.iter():
            fill = element.get("fill", "").strip()
            if fill.startswith("#") and len(fill) == 7:
                fills.add(fill)
        self.assertTrue(fills)
        for fill in fills:
            channels = [int(fill[index : index + 2], 16) for index in (1, 3, 5)]
            self.assertLessEqual(max(channels) - min(channels), 16, fill)

    def test_template_uses_the_neutral_placeholder_for_both_fallbacks(self):
        self.assertEqual(self.body.count(PLACEHOLDER_URL), 2)
        self.assertNotIn("avatar.jpg", self.body)


@unittest.skipUnless(Environment is not None, "python-liquid is not installed")
class TeamRenderingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rendered = render(TEAM)

    def test_head_is_rendered_first_with_cv_orcid_and_one_email(self):
        head_position = self.rendered.find("Attila Cangi, Ph.D.")
        self.assertGreaterEqual(head_position, 0)
        self.assertLess(head_position, self.rendered.find("Current members"))
        self.assertEqual(self.rendered.count('href="/BASE/cv/"'), 1)
        self.assertEqual(
            self.rendered.count('href="https://orcid.org/0000-0001-9162-262X"'), 1
        )
        self.assertEqual(self.rendered.count("mailto:a.cangi@hzdr.de"), 1)
        self.assertIn('<article class="team-card team-card--head">', self.rendered)

    def test_head_photo_is_used_with_descriptive_alt(self):
        head_block = card_blocks(self.rendered)[0]
        self.assertIn('src="/BASE/assets/img/team/Attila_Cangi.jpg"', head_block)
        self.assertIn('alt="Portrait of Attila Cangi, Ph.D."', head_block)
        self.assertIn('loading="lazy"', head_block)
        self.assertIn('width="96"', head_block)
        self.assertIn('height="96"', head_block)

    def test_head_is_not_duplicated_when_present_in_team_data(self):
        team = [
            {
                "title": "Current members",
                "members": [
                    {
                        "name": "Attila Cangi",
                        "role": "Head of Department",
                        "email": "a.cangi@hzdr.de",
                        "start_year": 2018,
                    }
                ],
            }
        ]
        rendered = render(team)
        self.assertEqual(rendered.count("mailto:a.cangi@hzdr.de"), 1)
        self.assertEqual(card_names(rendered).count("Attila Cangi"), 0)
        self.assertEqual(len(card_blocks(rendered)), 1)
        self.assertNotIn("Current members", rendered)

    def test_current_member_cards_show_role_email_and_no_dates(self):
        current = section(self.rendered, "Current members")
        self.assertIn("Postdoctoral researcher", current)
        self.assertIn("Doctoral researcher", current)
        self.assertIn('href="mailto:zoe@example.org"', current)
        self.assertIn('href="mailto:sam@example.org"', current)
        self.assertNotRegex(current, r"\b20\d{2}\b")
        self.assertNotIn("team-card-period", current)

    def test_alumni_periods_are_clear_and_never_invent_missing_years(self):
        alumni = section(self.rendered, "Alumni")
        former = card_for(self.rendered, "Dr. Former One")
        self.assertIn('<p class="team-card-period">2018\u20132021</p>', former)
        self.assertEqual(former.count("2018"), 1)
        self.assertEqual(former.count("2021"), 1)

        same = card_for(self.rendered, "Dr. Same Year")
        self.assertIn('<p class="team-card-period">2022</p>', same)
        self.assertEqual(same.count("2022"), 1)
        self.assertNotIn("2022\u20132022", same)

        start_only = card_for(self.rendered, "Dr. Start Only")
        self.assertIn('<p class="team-card-period">2020</p>', start_only)
        self.assertNotIn("\u2013", start_only)

        end_only = card_for(self.rendered, "Dr. End Only")
        self.assertIn('<p class="team-card-period">2024</p>', end_only)
        self.assertNotIn("\u2013", end_only)

        self.assertNotIn("None", alumni)
        self.assertNotIn("nan", alumni)

    def test_alumni_last_known_position_is_labelled_and_escaped(self):
        team = [
            {
                "title": "Alumni",
                "members": [
                    {
                        "name": "Dr. Former Two",
                        "role": "Doctoral researcher",
                        "start_year": 2017,
                        "end_year": 2020,
                        "last_position": "R&D Lead <Acme>",
                    }
                ],
            }
        ]
        rendered = render(team)
        block = card_for(rendered, "Dr. Former Two")
        self.assertIn("Last known position:", block)
        self.assertIn("R&amp;D Lead &lt;Acme&gt;", block)
        self.assertNotIn("R&D Lead <Acme>", block)

    def test_current_members_do_not_show_last_position_even_if_present(self):
        team = [
            {
                "title": "Current members",
                "members": [
                    {
                        "name": "Current Person",
                        "role": "Scientist",
                        "last_position": "Previous employer",
                    }
                ],
            }
        ]
        rendered = render(team)
        self.assertNotIn("Last known position", rendered)
        self.assertNotIn("Previous employer", rendered)

    def test_placeholder_is_used_for_missing_photos_with_empty_alt(self):
        block = card_for(self.rendered, "Sam <Test> & Co")
        self.assertIn(f'src="{BASE}{PLACEHOLDER_URL}"', block)
        self.assertIn('class="team-card-photo team-card-photo--placeholder"', block)
        self.assertIn('alt=""', block)

    def test_actual_photo_is_kept_for_members_that_have_one(self):
        block = card_for(self.rendered, "Dr. Zoë Ångström")
        self.assertIn('src="/BASE/assets/img/team/zoe.jpg"', block)
        self.assertIn('alt="Portrait of Dr. Zoë Ångström"', block)
        self.assertNotIn(PLACEHOLDER_URL, block)

    def test_names_are_escaped_and_spelled_exactly_as_provided(self):
        names = card_names(self.rendered)
        self.assertIn("Sam <Test> & Co", names)
        self.assertIn("Dr. Zoë Ångström", names)
        self.assertIn("Sam &lt;Test&gt; &amp; Co", self.rendered)

    def test_member_and_category_order_follow_the_data(self):
        expected = [
            "Dr. Zoë Ångström",
            "Sam <Test> & Co",
            "Dr. Former One",
            "Dr. Same Year",
            "Dr. Start Only",
            "Dr. End Only",
        ]
        self.assertEqual(card_names(self.rendered)[1:], expected)
        self.assertLess(
            self.rendered.find("Current members"), self.rendered.find("Alumni")
        )

    def test_every_person_is_rendered_exactly_once(self):
        names = card_names(self.rendered)
        self.assertEqual(len(names), 1 + sum(len(c["members"]) for c in TEAM))
        self.assertEqual(len(names), len(set(names)))

    def test_empty_category_is_omitted(self):
        team = [
            {"title": "Current members", "members": []},
            {"title": "Alumni", "members": [{"name": "Only Alum", "role": "Alum"}]},
        ]
        rendered = render(team)
        self.assertNotIn("Current members", rendered)
        self.assertIn("Alumni", rendered)
        self.assertEqual(card_names(rendered), ["Attila Cangi, Ph.D.", "Only Alum"])

    def test_empty_data_shows_a_neutral_message_and_still_shows_the_head(self):
        rendered = render([])
        self.assertIn("No team members are listed at present.", rendered)
        self.assertIn("Attila Cangi, Ph.D.", rendered)
        self.assertEqual(len(card_blocks(rendered)), 1)

    def test_missing_head_photo_falls_back_to_the_placeholder(self):
        head = dict(HEAD, photo=None)
        rendered = render([], head=head)
        head_block = card_blocks(rendered)[0]
        self.assertIn(f'src="{BASE}{PLACEHOLDER_URL}"', head_block)
        self.assertIn('alt=""', head_block)
        self.assertNotRegex(head_block, r'src=""')

    def test_generated_html_is_not_indented_as_markdown_code(self):
        # Jekyll runs Liquid before Kramdown; four-space-indented tags would be
        # rendered as a code block instead of markup.
        indented = [
            line
            for line in self.rendered.splitlines()
            if line.startswith("    ") and line.lstrip().startswith("<")
        ]
        self.assertEqual(indented, [])

    def test_sections_are_labelled_by_existing_headings(self):
        ids = re.findall(r'<section class="team-section[^"]*" aria-labelledby="([^"]+)"', self.rendered)
        headings = re.findall(r'<h2 class="team-section-title" id="([^"]+)"', self.rendered)
        self.assertEqual(ids, headings)
        self.assertEqual(len(ids), 3)  # head, current members, alumni
        self.assertEqual(len(ids), len(set(ids)))


if __name__ == "__main__":
    unittest.main()
