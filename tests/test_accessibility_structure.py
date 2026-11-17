"""Structural accessibility checks on a freshly built site."""

from html.parser import HTMLParser
import os
from pathlib import Path
import unittest


class LandmarkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.skip_links = []
        self.main_ids = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and "skip-link" in (attrs.get("class") or "").split():
            self.skip_links.append(attrs.get("href"))
        if tag == "main" or attrs.get("role") == "main":
            self.main_ids.add(attrs.get("id"))


@unittest.skipUnless(os.environ.get("SITE_DIR"), "set SITE_DIR to a built site directory")
class AccessibilityStructureTests(unittest.TestCase):
    def test_pages_have_skip_link_to_main_landmark(self):
        site = Path(os.environ["SITE_DIR"])
        for name in ("index.html", "research/index.html", "publications/index.html"):
            with self.subTest(page=name):
                parser = LandmarkParser()
                parser.feed((site / name).read_text(encoding="utf-8"))
                self.assertTrue(parser.skip_links, "missing skip link")
                self.assertTrue(any(link == "#" + main_id for link in parser.skip_links
                                    for main_id in parser.main_ids if main_id),
                                "skip link does not target a main landmark")
