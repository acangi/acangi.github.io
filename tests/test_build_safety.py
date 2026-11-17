"""Checks for files that must never be included in a built website."""

import os
from pathlib import Path
import unittest


@unittest.skipUnless(os.environ.get("SITE_DIR"), "set SITE_DIR to a built site directory")
class BuildSafetyTests(unittest.TestCase):
    def test_internal_handoff_is_not_published(self):
        site = Path(os.environ["SITE_DIR"])
        self.assertTrue(site.is_dir(), f"Built site directory not found: {site}")
        self.assertFalse((site / "HERMES_HANDOFF.md").exists())
        self.assertFalse((site / "HERMES_HANDOFF.html").exists())
