"""Typography guardrails: the site ships exactly two text families.

The Nordic Academe theme used to download three text faces (Inter, DM Serif
Display, JetBrains Mono). Metadata labels were moved to Inter, so the page now
carries two fonts and no mono webfont is fetched at all.

Build-independent, so it runs on a plain checkout:

    python -m unittest tests.test_typography_fonts -v

Optionally point CSS_PATH at a compiled main.css to check the shipped output.
"""
import os
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
FONTS_SCSS = ROOT / "_sass" / "_fonts.scss"
THEME_SCSS = ROOT / "_sass" / "_custom-theme.scss"
FONT_DIR = ROOT / "assets" / "fonts"

ALLOWED_FAMILIES = {"Inter", "DM Serif Display"}
RETIRED_FAMILIES = {"JetBrains Mono"}


def strip_comments(source):
    """Drop CSS/SCSS comments so explanatory text is never mistaken for code."""
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.S)
    return re.sub(r"(?m)^\s*//.*$", "", source)


def font_faces(source):
    """[(family, url)] for every @font-face block in the stylesheet."""
    faces = []
    for block in re.findall(r"@font-face\s*\{(.*?)\}", strip_comments(source), re.S):
        family = re.search(r'font-family:\s*"([^"]+)"', block)
        url = re.search(r'url\("([^"]+)"\)', block)
        if family and url:
            faces.append((family.group(1), url.group(1)))
    return faces


class FontDeclarationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = FONTS_SCSS.read_text(encoding="utf-8")
        cls.faces = font_faces(cls.source)

    def test_only_two_text_families_are_declared(self):
        families = {family for family, _ in self.faces}
        self.assertEqual(families, ALLOWED_FAMILIES)

    def test_retired_mono_face_is_gone(self):
        self.assertFalse(RETIRED_FAMILIES & {family for family, _ in self.faces})
        self.assertNotIn("jetbrains-mono-latin.woff2", strip_comments(self.source))

    def test_every_face_points_at_a_local_file(self):
        self.assertTrue(self.faces)
        for family, url in self.faces:
            with self.subTest(family=family, url=url):
                self.assertTrue(url.startswith("../fonts/"), "webfont must be same-origin")
                target = FONT_DIR / pathlib.PurePosixPath(url).name
                self.assertTrue(target.is_file(), f"missing font file {target}")

    def test_no_google_fonts_request(self):
        code = strip_comments(self.source)
        self.assertNotIn("fonts.googleapis.com", code)
        self.assertNotIn("fonts.gstatic.com", code)


class StackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = THEME_SCSS.read_text(encoding="utf-8")
        cls.source = strip_comments(cls.raw)

    def stack(self, name):
        match = re.search(rf"\${name}:\s*([^;]+);", self.source)
        assert match is not None, f"${name} is not declared"
        return [part.strip().strip('"') for part in match.group(1).split(",")]

    def test_sans_starts_with_local_inter(self):
        self.assertEqual(self.stack("font-sans")[0], "Inter")

    def test_serif_starts_with_local_dm_serif(self):
        self.assertEqual(self.stack("font-serif")[0], "DM Serif Display")

    def test_mono_is_a_system_stack_not_a_webfont(self):
        mono = self.stack("font-mono")
        self.assertNotIn("JetBrains Mono", mono)
        self.assertIn("ui-monospace", mono)
        # A webfont here would silently re-introduce a third download.
        self.assertFalse(any(name in self.source for name in RETIRED_FAMILIES))

    def test_labels_use_the_inter_label_mixin(self):
        match = re.search(r"\.post-description,([^{}]*)\{([^{}]*)\}", self.source)
        assert match is not None, "label rule not found"
        self.assertIn("@include label-type", match.group(2))

    def test_only_code_falls_back_to_mono(self):
        for match in re.finditer(r"font-family:\s*\$font-mono", self.source):
            selector_start = max(
                self.source.rfind("}", 0, match.start()),
                self.source.rfind(";", 0, match.start()),
            )
            selector = self.source[selector_start + 1 : match.start()].strip()
            with self.subTest(selector=selector):
                self.assertRegex(selector, r"\b(code|kbd|samp|pre)\b")


@unittest.skipUnless(os.environ.get("CSS_PATH"), "set CSS_PATH to a compiled main.css")
class CompiledCssTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.css = pathlib.Path(os.environ["CSS_PATH"]).read_text(encoding="utf-8")

    def test_no_retired_or_remote_fonts_in_output(self):
        code = strip_comments(self.css)
        for retired in RETIRED_FAMILIES:
            self.assertNotIn(f'"{retired}"', code, f"{retired} still referenced")
        self.assertNotIn("fonts.googleapis.com", code)

    def test_output_declares_two_text_faces(self):
        families = {family for family, _ in font_faces(self.css)}
        self.assertEqual(families & (ALLOWED_FAMILIES | RETIRED_FAMILIES), ALLOWED_FAMILIES)

    def test_output_never_requests_a_mono_webfont(self):
        urls = [url for _, url in font_faces(self.css)]
        self.assertFalse([url for url in urls if "mono" in url.lower()])


if __name__ == "__main__":
    unittest.main()
