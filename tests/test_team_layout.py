"""Layout contract for the team cards: names line up across a grid row.

The team grid puts a portrait beside a name/role stack inside every card.  If
that stack is centred against the portrait, its height - and therefore the
name's y offset - depends on how many lines the role needs, so names in the
same row drift apart (measured 13.5px at the desktop grid before this fix).
The cards must top-align the portrait and the stack instead: the name always
starts at the card's content edge, whatever the role length is.

These checks read ``_sass/_team.scss`` through a small SCSS rule parser, so
they assert the declarations that produce that geometry rather than grepping
for a formatted snippet.  A rendering check (when ``python-liquid`` is
installed) confirms the template itself puts the name first in the stack, so
no markup depends on the role's height.

Run with::

    python -m unittest tests.test_team_layout -v
"""

from __future__ import annotations

import pathlib
import re
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEAM_SASS = REPO_ROOT / "_sass" / "_team.scss"
TEAM_PAGE = REPO_ROOT / "_pages" / "team.md"

try:  # pragma: no cover - environment dependent
    from liquid import Environment
except ImportError:  # pragma: no cover - CI without python-liquid
    Environment = None  # type: ignore[assignment]


# --------------------------------------------------------------------------
# Minimal SCSS reader: flatten nesting into (media, selector, declarations).
# --------------------------------------------------------------------------


def strip_line_comments(text: str) -> str:
    """Drop ``//`` comments, leaving strings (and ``https://``) intact."""
    return re.sub(r'(?<![\w"\'/:])//[^\n]*', "", text)


def split_top_level(text: str, separator: str = ",") -> list[str]:
    """Split on ``separator`` while ignoring separators inside parentheses."""
    parts, depth, current = [], 0, []
    for char in text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == separator and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


def normalise_selector(selector: str) -> str:
    return re.sub(r"\s+", " ", selector).strip()


def parse_scss(text: str) -> list[tuple[str | None, str, dict[str, str]]]:
    """Return ``(media, selector, declarations)`` for every rule in ``text``.

    ``media`` is the enclosing at-rule prelude (e.g. ``"media (max-width: …)"``)
    or ``None`` for top level.  Nested selectors are joined with a space;
    ``&`` is expanded against the parent like Sass does.
    """
    text = strip_line_comments(text)
    rules: list[tuple[str | None, str, dict[str, str]]] = []
    stack: list[tuple[str | None, list[str]]] = []  # (at-rule prelude, selectors)
    buffer: list[str] = []
    selectors: list[str] = []
    media: str | None = None

    index = 0
    while index < len(text):
        char = text[index]
        if char == "{":
            selectors = [normalise_selector(part) for part in split_top_level("".join(buffer))]
            buffer = []
            if selectors and selectors[0].startswith("@"):
                stack.append((selectors[0], []))
            else:
                parents = stack[-1][1] if stack and stack[-1][1] else [""]
                resolved = []
                for parent in parents:
                    for selector in selectors:
                        if "&" in selector:
                            resolved.append(selector.replace("&", parent).strip())
                        else:
                            resolved.append(f"{parent} {selector}".strip())
                stack.append((None, resolved))
        elif char == "}":
            stack.pop()
            buffer = []
        elif char == ";":
            declaration = "".join(buffer).strip()
            buffer = []
            if declaration and stack:
                prop, _, value = declaration.partition(":")
                prop = prop.strip()
                value = re.sub(r"\s+", " ", value.strip())
                if not prop or prop.startswith("@"):
                    continue
                media_prelude = None
                for frame in stack:
                    if frame[0] and frame[0].startswith("@media"):
                        media_prelude = frame[0]
                media = re.sub(r"\s+", " ", media_prelude[1:]) if media_prelude else None
                for selector in stack[-1][1]:
                    rules.append((media, selector, {prop: value}))
        else:
            buffer.append(char)
        index += 1
    return rules


def declarations(rules, selector: str, media: str | None = None) -> dict[str, str]:
    """Merge every declaration that applies to ``selector`` in ``media``."""
    merged: dict[str, str] = {}
    for rule_media, rule_selector, decls in rules:
        if rule_selector == selector and rule_media == media:
            merged.update(decls)
    return merged


def selectors(rules) -> set[str]:
    return {rule_selector for _, rule_selector, _ in rules}


class ScssParserTests(unittest.TestCase):
    """The parser must be trustworthy before its findings mean anything."""

    def test_nesting_and_at_rules_are_flattened(self):
        rules = parse_scss(
            """
            .team-page {
              --team-avatar: 72px;
              .team-card { display: flex; }
            }
            @media (max-width: 575.98px) {
              .team-page { .team-grid { grid-template-columns: 1fr; } }
            }
            """
        )
        self.assertEqual(declarations(rules, ".team-page"), {"--team-avatar": "72px"})
        self.assertEqual(declarations(rules, ".team-page .team-card"), {"display": "flex"})
        self.assertEqual(
            declarations(rules, ".team-page .team-grid", "media (max-width: 575.98px)"),
            {"grid-template-columns": "1fr"},
        )

    def test_line_comments_do_not_swallow_urls(self):
        rules = parse_scss('.a { background: url("https://example.org/x.png"); } // note\n')
        self.assertEqual(
            declarations(rules, ".a"),
            {"background": 'url("https://example.org/x.png")'},
        )


class TeamCardAlignmentTests(unittest.TestCase):
    """Names must start at the same offset in every card of a grid row."""

    @classmethod
    def setUpClass(cls):
        cls.rules = parse_scss(TEAM_SASS.read_text(encoding="utf-8"))

    def test_portrait_and_identity_are_top_aligned(self):
        # Centring (the old value) makes the name's offset depend on role
        # height; flex-start pins it to the card's content edge.
        head = declarations(self.rules, ".team-page .team-card-head")
        self.assertEqual(head.get("align-items"), "flex-start")
        self.assertEqual(head.get("display"), "flex")

    def test_head_card_keeps_its_centred_portrait(self):
        # The head spans the whole row alone, so its one name stays optically
        # centred; this override must survive any change to the shared rule.
        head = declarations(self.rules, ".team-page .team-card--head .team-card-head")
        self.assertEqual(head.get("align-items"), "center")

    def test_no_rule_recentres_the_shared_card_head(self):
        for media, selector, decls in self.rules:
            if selector.endswith(".team-card-head") and "team-card--head" not in selector:
                with self.subTest(media=media, selector=selector):
                    self.assertNotEqual(decls.get("align-items"), "center")

    def test_name_and_role_stack_in_one_column(self):
        identity = declarations(self.rules, ".team-page .team-card-identity")
        self.assertEqual(identity.get("display"), "flex")
        self.assertEqual(identity.get("flex-direction"), "column")
        self.assertEqual(identity.get("min-width"), "0")
        self.assertIn("gap", identity)

    def test_role_carries_no_vertical_offset_of_its_own(self):
        # A margin on the role is fine visually but must not be the only
        # spacing: the stack's gap owns the rhythm, so one card cannot drift.
        role = declarations(self.rules, ".team-page .team-card-role")
        self.assertEqual(role.get("margin"), "0")
        name = declarations(self.rules, ".team-page .team-card-name")
        self.assertEqual(name.get("margin"), "0")

    def test_nothing_offsets_the_name_by_role_height(self):
        offenders = []
        for media, selector, decls in self.rules:
            if not selector.startswith(".team-page"):
                continue
            if not re.search(r"team-card-(name|role|identity)\b", selector):
                continue
            for prop, value in decls.items():
                shifted = prop in {"top", "inset-block-start", "translate", "transform", "align-self"}
                negative_margin = prop.startswith("margin") and value.strip().startswith("-")
                if shifted or negative_margin:
                    offenders.append((media, selector, prop, value))
        self.assertEqual(offenders, [])

    def test_portrait_size_and_shape_are_unchanged(self):
        photo = declarations(self.rules, ".team-page .team-card-photo")
        self.assertEqual(photo.get("width"), "var(--team-avatar)")
        self.assertEqual(photo.get("height"), "var(--team-avatar)")
        self.assertEqual(photo.get("border-radius"), "4px")  # neutral square, not a disc
        self.assertEqual(photo.get("object-fit"), "cover")
        self.assertEqual(declarations(self.rules, ".team-page").get("--team-avatar"), "72px")


class TeamGridResponsiveTests(unittest.TestCase):
    """The grid keeps its breakpoints and its print behaviour."""

    @classmethod
    def setUpClass(cls):
        cls.rules = parse_scss(TEAM_SASS.read_text(encoding="utf-8"))
        cls.css = TEAM_SASS.read_text(encoding="utf-8")

    def test_grid_remains_an_auto_filling_minmax_grid(self):
        grid = declarations(self.rules, ".team-page .team-grid")
        self.assertEqual(grid.get("display"), "grid")
        self.assertRegex(
            grid.get("grid-template-columns", ""),
            r"^repeat\(auto-fill, minmax\(\d+px, 1fr\)\)$",
        )

    def test_single_column_below_the_existing_mobile_breakpoint(self):
        mobile = [
            media for media, _, _ in self.rules if media and "575.98px" in media
        ]
        self.assertTrue(mobile, "the 575.98px breakpoint must stay")
        self.assertEqual(
            declarations(
                self.rules,
                ".team-page .team-grid",
                "media (max-width: 575.98px)",
            ).get("grid-template-columns"),
            "1fr",
        )

    def test_head_card_stacks_below_the_tablet_breakpoint(self):
        self.assertEqual(
            declarations(
                self.rules,
                ".team-page .team-card--head",
                "media (max-width: 767.98px)",
            ).get("flex-direction"),
            "column",
        )

    def test_mobile_avatar_shrinks_but_stays_the_same_shape(self):
        self.assertEqual(
            declarations(self.rules, ".team-page", "media (max-width: 575.98px)").get(
                "--team-avatar"
            ),
            "64px",
        )

    def test_print_keeps_cards_whole(self):
        card = declarations(self.rules, ".team-page .team-card", "media print")
        self.assertEqual(card.get("break-inside"), "avoid")
        self.assertEqual(card.get("page-break-inside"), "avoid")
        self.assertEqual(
            declarations(self.rules, ".team-page .team-grid", "media print").get("display"),
            "block",
        )

    def test_styles_stay_scoped_to_the_team_page(self):
        for _, selector, _ in self.rules:
            first = selector.split()[0]
            with self.subTest(selector=selector):
                self.assertTrue(
                    first.startswith(".team-page")
                    or first.startswith("body:has(.team-page)"),
                    f"unscoped selector: {selector}",
                )


@unittest.skipUnless(Environment is not None, "python-liquid is not installed")
class TeamMarkupStructureTests(unittest.TestCase):
    """The template must not make the name's position depend on the role."""

    HEAD = {
        "name": "Attila Cangi, Ph.D.",
        "email": "a.cangi@hzdr.de",
        "photo": "/assets/img/team/Attila_Cangi.jpg",
        "cv": "/cv/",
        "orcid": "https://orcid.org/0000-0001-9162-262X",
        "aliases": ["Attila Cangi"],
    }

    @classmethod
    def setUpClass(cls):
        raw = TEAM_PAGE.read_text(encoding="utf-8")
        end = raw.index("\n---", 3)
        cls.body = raw[end + 4 :]

    def render(self, team):
        env = Environment(autoescape=False, strict_filters=True)
        env.filters["relative_url"] = lambda _ctx, value: value
        return env.render(
            self.body,
            {"site": {"data": {"team": team}}, "page": {"head": self.HEAD}},
        )

    def cards(self, rendered):
        return re.findall(r'<article class="team-card">(.*?)</article>', rendered, re.S)

    def test_role_length_does_not_change_the_card_structure(self):
        team = [
            {
                "title": "Current members",
                "members": [
                    {"name": "Short Role", "role": "PhD Candidate", "email": "a@example.org"},
                    {
                        "name": "Long Role",
                        "role": (
                            "Student Research Assistant (Scultetus Early Career Fellow, "
                            "HZDR Summer Student Program)"
                        ),
                        "email": "b@example.org",
                    },
                    {"name": "No Role", "email": "c@example.org"},
                ],
            }
        ]
        cards = self.cards(self.render(team))
        self.assertEqual(len(cards), 3)
        shapes = []
        for block in cards:
            # Name first, optional role second: the stack starts with the name
            # in every card, so top-alignment pins every name to the same y.
            name_at = block.index('class="team-card-name"')
            role_at = block.find('class="team-card-role"')
            shapes.append(
                (
                    block.count('class="team-card-head"'),
                    block.count('class="team-card-identity"'),
                    role_at == -1 or role_at > name_at,
                )
            )
        self.assertEqual(shapes, [(1, 1, True)] * 3)

    def test_cards_carry_no_inline_layout(self):
        team = [
            {
                "title": "Current members",
                "members": [{"name": "Person", "role": "Scientist", "email": "p@example.org"}],
            }
        ]
        for block in self.cards(self.render(team)):
            self.assertNotIn("style=", block)

    def test_name_and_role_are_siblings_in_the_identity_stack(self):
        team = [
            {
                "title": "Alumni",
                "members": [
                    {
                        "name": "Dr. Former",
                        "role": "Doctoral researcher",
                        "start_year": 2018,
                        "end_year": 2021,
                        "last_position": "Scientist",
                    }
                ],
            }
        ]
        block = self.cards(self.render(team))[0]
        identity = block.split('class="team-card-identity"', 1)[1]
        self.assertIn('class="team-card-name"', identity)
        self.assertIn('class="team-card-role"', identity)
        self.assertLess(
            identity.index('class="team-card-name"'),
            identity.index('class="team-card-role"'),
        )
        # Alumni material sits after the head, so it never enters the stack
        # whose height decides where the name starts.
        self.assertGreater(identity.index("team-card-period"), identity.index("team-card-role"))


if __name__ == "__main__":
    unittest.main()
