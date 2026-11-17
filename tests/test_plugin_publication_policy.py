"""Run the Ruby publications-policy checks from the Python test suite.

The Jekyll filter is Ruby, so its checks live in
``tests/publication_policy_plugin_test.rb``. This module runs that file with a
local Ruby installation, the repository's Bundler environment, or - when Ruby
is not installed on the host, as documented in CONTENT.md - the project's
al-folio container image.

Run with:
    python -m unittest tests.test_plugin_publication_policy -v

The checks never build the site and never write to the repository.
"""
import os
import pathlib
import re
import shutil
import subprocess
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
RUBY_TEST = "tests/publication_policy_plugin_test.rb"
CONTAINER_IMAGE = os.getenv(
    "PUBLICATION_POLICY_RUBY_IMAGE", "docker.io/amirpourmand/al-folio:v0.16.2"
)
CONTAINER_TIMEOUT = 240


def image_present(engine):
    """Return True when the configured container image is already local."""
    try:
        completed = subprocess.run(
            [engine, "image", "inspect", CONTAINER_IMAGE],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


def candidate_commands():
    """Return (label, command, timeout) candidates, most native first."""
    candidates = []
    if shutil.which("ruby"):
        candidates.append(
            ("local ruby", ["ruby", RUBY_TEST], 120)
        )
    if shutil.which("bundle"):
        candidates.append(
            ("bundler", ["bundle", "exec", "ruby", RUBY_TEST], 120)
        )
    for engine in ("podman", "docker"):
        if not shutil.which(engine) or not image_present(engine):
            continue
        mount = f"{REPO_ROOT}:/srv/jekyll:Z" if engine == "podman" else f"{REPO_ROOT}:/srv/jekyll"
        candidates.append(
            (
                f"{engine} {CONTAINER_IMAGE}",
                [
                    engine,
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "-v",
                    mount,
                    "-w",
                    "/srv/jekyll",
                    CONTAINER_IMAGE,
                    "bundle",
                    "exec",
                    "ruby",
                    RUBY_TEST,
                ],
                CONTAINER_TIMEOUT,
            )
        )
    return candidates


def run_ruby_checks():
    """Run the Ruby checks, returning (returncode, output) of the first real run.

    A candidate that cannot even start Ruby (missing gems, no image) is skipped
    and the next one is tried. The first candidate that reports check results is
    authoritative, so a genuine policy failure is never hidden by a fallback.
    """
    errors = []
    for label, command, timeout in candidate_commands():
        try:
            completed = subprocess.run(
                command,
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            errors.append(f"{label}: {exc}")
            continue
        output = (completed.stdout or "") + (completed.stderr or "")
        if "checks," in output and "failures" in output:
            return completed.returncode, f"[{label}]\n{output}"
        errors.append(f"{label}: exit {completed.returncode}\n{output.strip()}")
    raise unittest.SkipTest(
        "no Ruby runtime available for the Jekyll filter checks:\n"
        + "\n\n".join(errors)
    )


class RubyPublicationsPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.returncode, cls.output = run_ruby_checks()

    def test_every_ruby_policy_check_passes(self):
        failed = [
            line for line in self.output.splitlines() if line.startswith("not ok -")
        ]
        self.assertEqual(failed, [], self.output)
        self.assertEqual(self.returncode, 0, self.output)

    def test_ruby_checks_cover_the_policy_ground(self):
        match = re.search(r"(\d+) checks, (\d+) failures", self.output)
        self.assertIsNotNone(match, self.output)
        assert match is not None
        self.assertGreaterEqual(int(match.group(1)), 20, self.output)


if __name__ == "__main__":
    unittest.main()
