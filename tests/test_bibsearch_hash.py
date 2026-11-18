"""Run the bibsearch hash behavior suite with Node's built-in test runner."""

import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class BibsearchHashTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "node is required for JS tests")
    def test_node_suite(self) -> None:
        result = subprocess.run(
            ["node", "--test", "tests/bibsearch_hash.test.mjs"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            "bibsearch hash tests failed:\n"
            f"{result.stdout}\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
