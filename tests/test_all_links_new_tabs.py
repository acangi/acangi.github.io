"""Tests for the context-aware site link policy.

The policy is implemented by ``_plugins/open-links-in-new-tabs.rb``:

* internal destinations (relative paths, fragments, queries, absolute URLs on
  the configured site origin) navigate in the same browsing context, so they
  must not carry ``target="_blank"``;
* external HTTP(S) destinations, including protocol-relative URLs, open in a
  new tab with both ``noopener`` and ``noreferrer`` while keeping any existing
  ``rel`` tokens;
* non-navigation destinations (``javascript:``, ``mailto:``, ``tel:``,
  ``sms:``, ``data:``, ``blob:`` and links with ``download``) keep the browser
  default: no target is added and ``mailto:`` is not forced into a new tab.

Run the integration half against a built site::

    SITE_DIR=/tmp/site python -m pytest tests/test_all_links_new_tabs.py -v

There are several groups of tests:

* :class:`RenderedLinkTests` inspects the actual built HTML: no internal link
  may still carry ``target="_blank"``, every external link must be secured,
  and the skip link must navigate within the page again.
* :class:`PluginTransformTests` unit-tests the Ruby transformer on fixtures,
  including deceptive look-alike hosts, credentials, ports, configured site
  origins, escaping, protected regions and idempotency.  The static transform
  never trusts ``localhost`` unless it is the configured site host.
* :class:`DynamicAnchorRuntimeTests` runs the runtime generated from the
  current plugin source under Node with a minimal DOM to prove that
  client-inserted anchors and late ``href`` changes follow the same policy,
  including WHATWG backslash-authority resolution.
* :class:`BrowserMutationObserverTests` runs that same source runtime in real
  Chromium with a real ``MutationObserver``, an actual preview HTTP origin and
  a timer, to prove that observer writes are idempotent (no mutation feedback
  loop), that backslash-deceptive hosts match the browser in both directions,
  and that the runtime matches the browser's current origin rather than
  trusting ``localhost`` by name.
* ``RenderedLinkTests.test_injected_runtime_matches_current_plugin_source``
  compares the runtime in the built pages with the source, so a stale build is
  reported instead of silently passing the runtime tests.
"""

from __future__ import annotations

import html as html_module
import http.server
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import threading
import unittest
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_PATH = REPO_ROOT / "_plugins" / "open-links-in-new-tabs.rb"
SITE_DIR = Path(os.environ.get("SITE_DIR", REPO_ROOT / "_site"))

SITE_URL = "https://acangi.github.io"
SITE_HOST = "acangi.github.io"
SECURITY_TOKENS = ("noopener", "noreferrer")
NON_NAVIGATION_SCHEMES = ("javascript", "mailto", "tel", "sms", "data", "blob")


class AnchorCollector(HTMLParser):
    """Collect start tags and their attributes from an HTML document."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[dict[str, str | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("a", "area"):
            self.anchors.append(dict(attrs))


def collect_anchors(markup: str) -> list[dict[str, str | None]]:
    parser = AnchorCollector()
    parser.feed(markup)
    return parser.anchors


def rel_tokens(anchor: dict[str, str | None]) -> list[str]:
    return (anchor.get("rel") or "").lower().split()


def normalize_href(href: str | None) -> str:
    """Drop ASCII controls and surrounding whitespace like a URL parser."""

    return re.sub(r"[\t\n\r]", "", href or "").strip()


def link_kind(anchor: dict[str, str | None]) -> str:
    """Classify an anchor as ``internal``, ``external`` or ``other``.

    ``other`` covers missing/empty hrefs and browser-default destinations
    (pseudo schemes, ``download`` links), which the policy leaves alone.
    """

    href = normalize_href(anchor.get("href"))
    if not href or "download" in anchor:
        return "other"

    try:
        parsed = urlsplit(href)
    except ValueError:
        return "external"

    scheme = parsed.scheme.lower()
    if scheme and scheme not in ("http", "https"):
        return "other"

    try:
        absolute = urlsplit(urljoin(SITE_URL + "/", href))
    except ValueError:
        return "external"

    if absolute.scheme not in ("http", "https") or not absolute.hostname:
        return "other"

    host = absolute.hostname.lower().rstrip(".")
    if host != SITE_HOST:
        return "external"

    port = absolute.port
    if port is None or (absolute.scheme == "https" and port == 443) or (
        absolute.scheme == "http" and port == 80
    ):
        return "internal"
    return "external"


def rendered_pages(site: Path) -> list[Path]:
    """HTML documents produced by Jekyll, excluding copied static assets."""

    return [
        path
        for path in sorted(site.rglob("*.html"))
        if "assets" not in path.relative_to(site).parts
    ]


@unittest.skipUnless(SITE_DIR.is_dir(), "build the site or set SITE_DIR")
class RenderedLinkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pages = rendered_pages(SITE_DIR)
        if not cls.pages:
            raise unittest.SkipTest(f"no rendered HTML under {SITE_DIR}")

    def test_external_links_open_in_a_new_tab_with_security_tokens(self) -> None:
        checked = 0
        failures: list[str] = []
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if link_kind(anchor) != "external":
                    continue
                checked += 1
                if anchor.get("target") != "_blank":
                    failures.append(f"{page}: missing target=_blank: {anchor}")
                    continue
                missing = [t for t in SECURITY_TOKENS if t not in rel_tokens(anchor)]
                if missing:
                    failures.append(f"{page}: missing {missing}: {anchor}")
        self.assertGreater(checked, 20, "expected many external links")
        self.assertEqual([], failures, "external links must open in a new tab")

    def test_internal_links_navigate_in_the_same_tab(self) -> None:
        checked = 0
        failures: list[str] = []
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if link_kind(anchor) != "internal":
                    continue
                checked += 1
                if anchor.get("target") is not None:
                    failures.append(f"{page}: internal link has target: {anchor}")
                leftover = [t for t in SECURITY_TOKENS if t in rel_tokens(anchor)]
                if leftover:
                    failures.append(f"{page}: internal link has {leftover}: {anchor}")
        self.assertGreater(checked, 50, "expected many internal links")
        self.assertEqual([], failures, "internal links must stay in the same tab")

    def test_absolute_canonical_site_links_stay_in_the_same_tab(self) -> None:
        canonical = []
        for path in self.pages:
            canonical.extend(
                anchor
                for anchor in collect_anchors(path.read_text(encoding="utf-8"))
                if normalize_href(anchor.get("href")).startswith(SITE_URL)
            )
        self.assertTrue(canonical, "expected absolute canonical site links")
        for anchor in canonical:
            with self.subTest(href=anchor.get("href")):
                self.assertIsNone(anchor.get("target"))
                self.assertFalse(set(SECURITY_TOKENS) & set(rel_tokens(anchor)))

    def test_no_rendered_link_has_an_unwanted_blank_target(self) -> None:
        failures: list[str] = []
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if anchor.get("target") != "_blank":
                    continue
                kind = link_kind(anchor)
                if kind != "external":
                    failures.append(f"{page}: unwanted target=_blank ({kind}): {anchor}")
        self.assertEqual([], failures)

    def test_every_blank_target_is_secure(self) -> None:
        failures: list[str] = []
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if anchor.get("target") != "_blank":
                    continue
                missing = [t for t in SECURITY_TOKENS if t not in rel_tokens(anchor)]
                if missing:
                    failures.append(f"{page}: target=_blank missing {missing}: {anchor}")
        self.assertEqual([], failures)

    def test_skip_link_restores_same_page_navigation(self) -> None:
        index = (SITE_DIR / "index.html").read_text(encoding="utf-8")
        skip_links = [
            anchor
            for anchor in collect_anchors(index)
            if "skip-link" in (anchor.get("class") or "").split()
        ]
        self.assertTrue(skip_links, "missing skip link")
        for anchor in skip_links:
            self.assertTrue((anchor.get("href") or "").startswith("#"))
            self.assertIsNone(anchor.get("target"))
            self.assertFalse(set(SECURITY_TOKENS) & set(rel_tokens(anchor)))

    def test_email_and_telephone_links_are_not_forced_into_a_new_tab(self) -> None:
        contacts: list[dict[str, str | None]] = []
        for path in self.pages:
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                href = normalize_href(anchor.get("href")).lower()
                if href.startswith(("mailto:", "tel:")):
                    contacts.append(anchor)
        self.assertTrue(contacts, "expected at least one email link")
        for anchor in contacts:
            with self.subTest(href=anchor.get("href")):
                self.assertIsNone(anchor.get("target"))
                self.assertFalse(set(SECURITY_TOKENS) & set(rel_tokens(anchor)))

    def test_javascript_pseudo_links_are_left_unchanged(self) -> None:
        for path in self.pages:
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if not normalize_href(anchor.get("href")).lower().startswith("javascript:"):
                    continue
                self.assertIsNone(anchor.get("target"))
                self.assertIsNone(anchor.get("rel"))

    def test_no_link_forces_the_opener_relationship(self) -> None:
        for path in self.pages:
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if link_kind(anchor) == "other" and normalize_href(
                    anchor.get("href")
                ).lower().startswith("javascript:"):
                    continue
                self.assertNotIn(
                    "opener",
                    rel_tokens(anchor),
                    f"{path}: rel=opener is unsafe",
                )

    def test_dynamic_runtime_is_injected_once_per_page(self) -> None:
        for path in self.pages:
            with self.subTest(page=path.relative_to(SITE_DIR)):
                markup = path.read_text(encoding="utf-8")
                runtimes = re.findall(
                    r"<script\b[^>]*\bdata-all-links-new-tab\b[^>]*>(.*?)</script>",
                    markup,
                    re.S | re.I,
                )
                self.assertEqual(1, len(runtimes), "expected exactly one runtime")
                self.assertIn("MutationObserver", runtimes[0])
                self.assertIn("querySelectorAll", runtimes[0])
                self.assertIn(SITE_HOST, runtimes[0])

    def test_injected_runtime_matches_current_plugin_source(self) -> None:
        # Runtime behaviour tests must not silently exercise an old build.
        source = source_runtime()
        if source is None:
            self.skipTest("ruby is not available to derive the runtime from source")
        for path in self.pages:
            with self.subTest(page=path.relative_to(SITE_DIR)):
                built = extract_runtime(path.read_text(encoding="utf-8"))
                self.assertIsNotNone(built, "runtime missing from built page")
                self.assertEqual(
                    source,
                    built,
                    "built runtime differs from the current plugin source; "
                    "rebuild the site before trusting rendered results",
                )


def _ruby_command() -> list[str] | None:
    """Return a command that can run the plugin's Ruby module, if available."""

    if shutil.which("ruby"):
        return ["ruby"]

    podman = shutil.which("podman")
    docker = shutil.which("docker")
    container = podman or docker
    if container is None:
        return None

    image = os.environ.get("JEKYLL_IMAGE", "docker.io/amirpourmand/al-folio:v0.16.2")
    if podman:
        probe = [podman, "image", "exists", image]
        run_options = ["--userns=keep-id"]
        volume = f"{REPO_ROOT}:/work:Z"
    else:
        probe = [docker, "image", "inspect", image]
        run_options = []
        volume = f"{REPO_ROOT}:/work"

    if subprocess.run(
        probe,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        return None

    return [
        container,
        "run",
        "--rm",
        "-i",
        *run_options,
        "-v",
        volume,
        "-w",
        "/work",
        "--entrypoint",
        "ruby",
        image,
    ]


def _chrome_command() -> list[str] | None:
    """Return a real Chromium/Chrome binary for browser-level tests."""

    candidates = []
    configured = os.environ.get("CHROME_BIN")
    if configured:
        candidates.append(configured)
    for name in (
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
        "chrome",
    ):
        found = shutil.which(name)
        if found:
            candidates.append(found)
    candidates.extend(
        sorted(str(path) for path in Path.home().glob(".hermes/tools/chromium-*/chrome-linux64/chrome"))
    )

    for candidate in candidates:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return [str(candidate)]
    return None


RUNTIME_PATTERN = re.compile(
    r"<script\b[^>]*\bdata-all-links-new-tab\b[^>]*>(.*?)</script>",
    re.S | re.I,
)


def extract_runtime(markup: str) -> str | None:
    """Return the injected runtime body, or ``None`` when it is absent."""

    match = RUNTIME_PATTERN.search(markup)
    return html_module.unescape(match.group(1)) if match else None


def plugin_argument(ruby: list[str]) -> str:
    """Path to the plugin as seen by the Ruby process (or container)."""

    return str(PLUGIN_PATH) if ruby[0] == "ruby" else (
        f"/work/{PLUGIN_PATH.relative_to(REPO_ROOT)}"
    )


def source_runtime() -> str | None:
    """Generate the runtime from the *current* plugin source.

    The built site may lag behind the plugin (or a previous build may still be
    on disk).  Runtime tests must exercise the source, and a separate test
    compares the built page against this value so a stale build is reported
    instead of silently passing.
    """

    ruby = _ruby_command()
    if ruby is None:
        return None
    markup = "<!doctype html><html><body></body></html>"
    code = (
        "STDOUT.write(Jekyll::AllLinksInNewTabs.transform("
        f"{json.dumps(markup)}))"
    )
    result = subprocess.run(
        ruby + ["-r", plugin_argument(ruby), "-e", code],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    runtime = extract_runtime(result.stdout)
    if runtime is None:
        raise RuntimeError("plugin did not inject a runtime")
    return runtime


@unittest.skipUnless(PLUGIN_PATH.is_file(), "plugin not implemented yet")
class PluginTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ruby = _ruby_command()
        if cls.ruby is None:
            raise unittest.SkipTest("ruby is not available to run the plugin unit tests")
        # Inside the container the repository is mounted at /work.
        cls.plugin_arg = (
            str(PLUGIN_PATH)
            if cls.ruby[0] == "ruby"
            else f"/work/{PLUGIN_PATH.relative_to(REPO_ROOT)}"
        )

    def transform(self, markup: str, site_url: str | None = None) -> str:
        call = "Jekyll::AllLinksInNewTabs.transform(STDIN.read"
        if site_url is not None:
            call += f", site_url: {json.dumps(site_url)}"
        call += ")"
        command = self.ruby + [
            "-r",
            self.plugin_arg,
            "-e",
            f"STDOUT.write({call})",
        ]
        result = subprocess.run(
            command, input=markup, text=True, capture_output=True, check=False
        )
        self.assertEqual(0, result.returncode, result.stderr)
        return result.stdout

    def anchor(self, markup: str, site_url: str | None = None) -> dict[str, str | None]:
        return collect_anchors(self.transform(markup, site_url))[0]

    def test_external_destinations_open_in_a_new_tab(self) -> None:
        cases = (
            "https://example.org",
            "http://example.org/path",
            "//cdn.example.org/lib.js",
            "https://example.org/?next=https://acangi.github.io/",
            "https://example.org/acangi.github.io/",
        )
        for href in cases:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertEqual("_blank", anchor["target"])
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_internal_destinations_stay_in_the_same_tab(self) -> None:
        cases = (
            "/team/",
            "team/",
            "./team/",
            "../team/",
            "/research/#quantum",
            "#main-content",
            "?page=2",
            "https://acangi.github.io/",
            "https://acangi.github.io/team/",
            "https://acangi.github.io./team/",
            "https://acangi.github.io:443/team/",
            "http://acangi.github.io/team/",
            "//acangi.github.io/team/",
        )
        for href in cases:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertIsNone(anchor.get("target"))
                self.assertIsNone(anchor.get("rel"))

    def test_look_alike_hosts_are_external(self) -> None:
        cases = (
            "https://acangi.github.io.evil.example/team/",
            "https://www.acangi.github.io/team/",
            "https://acangi.github.io:8443/team/",
            "https://acangi.github.io.evil.example:443/",
            "https://user:secret@evil.example/acangi.github.io/",
            "https://user@attacker.example/team/",
            "http://localhost.evil.example/team/",
            "https://evil.example/?host=acangi.github.io",
        )
        for href in cases:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertEqual("_blank", anchor["target"])
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_backslash_in_special_url_authority_matches_browser(self) -> None:
        # WHATWG treats "\" as "/" for special schemes before the authority
        # is parsed, so the host the browser visits can differ from the text
        # after the last "@".  A substring search or a split on "/?#" alone
        # would invert both of these cases.
        external = (
            r"https://attacker.example\@acangi.github.io/team/",
            r"https://evil.example\/@acangi.github.io/",
            r"https:///evil.example/team/",
            r"https:\\evil.example\path",
            r"\\evil.example\team/",
            r"/\evil.example/team/",
        )
        for href in external:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertEqual("_blank", anchor["target"], href)
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

        internal = (
            r"https://acangi.github.io\@evil.example/team/",
            r"https://acangi.github.io\/@evil.example/team/",
            r"\\acangi.github.io\team\/",
        )
        for href in internal:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertIsNone(anchor.get("target"), href)
                self.assertIsNone(anchor.get("rel"), href)

    def test_same_scheme_relative_urls_match_browser_resolution(self) -> None:
        # A special URL whose scheme equals the base scheme and that does not
        # start with "//" (or "\\") is resolved relative to the current page,
        # not as an absolute URL with that host.
        internal = (
            "https:acangi.github.io/team",
            "https:evil.example/team",
            "https:/evil.example/team",
            "https:\\evil.example/team",
        )
        for href in internal:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertIsNone(anchor.get("target"), href)

        external = (
            "https://evil.example/team",
            r"https:\\evil.example\team",
            r"\\evil.example\team",
        )
        for href in external:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertEqual("_blank", anchor["target"], href)

    def test_invalid_configured_site_url_falls_back_to_production_origin(self) -> None:
        # A malformed site_url must not become a host (or scheme) that no
        # browser can visit; fall back to the production origin instead.
        markup = (
            '<!doctype html><html><body>'
            '<a href="https://acangi.github.io/team/">internal</a>'
            '<a href="https://example.org/team/">external</a>'
            "</body></html>"
        )
        for bad in (
            "not a url",
            "https://",
            "https://exa mple.com",
            "https://-bad-.example",
            "https://acangi.github.io:99999",
            "https://acangi.github.io:notaport",
            "https:acangi.github.io",
            "ftp://example.test",
        ):
            with self.subTest(site_url=bad):
                output = self.transform(markup, site_url=bad)
                anchors = collect_anchors(output)
                self.assertIsNone(anchors[0].get("target"), "fallback host")
                self.assertEqual("_blank", anchors[1]["target"])
                runtime = extract_runtime(output) or ""
                self.assertIn('"acangi.github.io"', runtime)
                self.assertIn('"https"', runtime)
                self.assertNotIn('"not a url"', runtime)
                self.assertNotIn('"exa mple.com"', runtime)
                self.assertNotIn('"ftp"', runtime)

    def test_credentials_on_the_canonical_host_still_reach_the_canonical_host(self) -> None:
        anchor = self.anchor('<a href="https://user:secret@acangi.github.io/team/">x</a>')
        self.assertIsNone(anchor.get("target"))

    def test_configured_site_url_defines_the_internal_host(self) -> None:
        internal = self.anchor(
            '<a href="https://example.test/team/">x</a>',
            site_url="https://example.test",
        )
        self.assertIsNone(internal.get("target"))

        external = self.anchor(
            '<a href="https://acangi.github.io/team/">x</a>',
            site_url="https://example.test",
        )
        self.assertEqual("_blank", external["target"])

    def test_configured_site_url_port_is_part_of_the_origin(self) -> None:
        same_origin = self.anchor(
            '<a href="https://example.test:8443/team/">x</a>',
            site_url="https://example.test:8443",
        )
        self.assertIsNone(same_origin.get("target"))

        default_port = self.anchor(
            '<a href="https://example.test/team/">x</a>',
            site_url="https://example.test:8443",
        )
        self.assertEqual("_blank", default_port["target"])

        different_port = self.anchor(
            '<a href="https://example.test:8443/team/">x</a>',
            site_url="https://example.test",
        )
        self.assertEqual("_blank", different_port["target"])

        scheme_less = self.anchor(
            '<a href="https://example.test:8443/team/">x</a>',
            site_url="example.test:8443",
        )
        self.assertIsNone(scheme_less.get("target"))

    def test_local_preview_hosts_are_external_on_the_production_origin(self) -> None:
        # The static build runs on acangi.github.io; localhost is a different
        # host there and must not be trusted just because it is a preview name.
        cases = (
            "http://localhost:4000/team/",
            "http://127.0.0.1:4000/team/",
            "//localhost:4000/team/",
            "http://localhost.evil.example/team/",
        )
        for href in cases:
            with self.subTest(href=href):
                anchor = self.anchor(f'<a href="{href}">x</a>')
                self.assertEqual("_blank", anchor["target"])
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_configured_local_preview_origin_is_internal(self) -> None:
        same_origin = self.anchor(
            '<a href="http://localhost:4000/team/">x</a>',
            site_url="http://localhost:4000",
        )
        self.assertIsNone(same_origin.get("target"))

        different_host = self.anchor(
            '<a href="http://127.0.0.1:4000/team/">x</a>',
            site_url="http://localhost:4000",
        )
        self.assertEqual("_blank", different_host["target"])

        different_port = self.anchor(
            '<a href="http://localhost:5000/team/">x</a>',
            site_url="http://localhost:4000",
        )
        self.assertEqual("_blank", different_port["target"])

    def test_non_navigation_schemes_keep_the_browser_default(self) -> None:
        cases = (
            '<a href="mailto:a@example.org">m</a>',
            '<a href="tel:+123">t</a>',
            '<a href="sms:+123">s</a>',
            '<a href="data:text/plain,hello">d</a>',
            '<a href="blob:https://acangi.github.io/id">b</a>',
            '<a href="/paper.pdf" download>pdf</a>',
            '<a href="https://example.org/paper.pdf" download>pdf</a>',
        )
        for markup in cases:
            with self.subTest(markup=markup):
                self.assertEqual(markup, self.transform(markup))

    def test_existing_rel_is_merged_and_opener_is_removed(self) -> None:
        anchor = self.anchor(
            '<a href="https://example.org" rel="external nofollow opener">x</a>'
        )
        self.assertEqual("external nofollow", " ".join(rel_tokens(anchor)[:2]))
        self.assertNotIn("opener", rel_tokens(anchor))
        self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_unwanted_internal_blank_is_removed_even_from_earlier_output(self) -> None:
        cases = (
            '<a href="/team/" target="_blank" rel="noopener noreferrer">x</a>',
            "<a href='#main-content' target='_blank' rel='noopener noreferrer'>x</a>",
            '<a href="https://acangi.github.io/team/" target="_blank" '
            'rel="noopener noreferrer external">x</a>',
            '<a href="/team/" target="_blank">x</a>',
        )
        for markup in cases:
            with self.subTest(markup=markup):
                anchor = self.anchor(markup)
                self.assertIsNone(anchor.get("target"))
                if "external" in markup:
                    self.assertEqual(["external"], rel_tokens(anchor))
                else:
                    self.assertIsNone(anchor.get("rel"))

    def test_internal_links_keep_unrelated_rel_tokens(self) -> None:
        anchor = self.anchor('<a href="/team/" rel="me nofollow">x</a>')
        self.assertEqual(["me", "nofollow"], rel_tokens(anchor))
        self.assertIsNone(anchor.get("target"))

    def test_explicit_self_target_on_internal_link_is_preserved(self) -> None:
        markup = '<a href="/team/" target="_self">x</a>'
        self.assertEqual(markup, self.transform(markup))

    def test_body_text_inside_a_script_does_not_receive_the_runtime(self) -> None:
        markup = (
            "<!doctype html><html><body>"
            "<script>var closing = '</body>';</script>"
            "<p>content</p></body></html>"
        )
        output = self.transform(markup)
        self.assertIn("<script>var closing = '</body>';</script>", output)
        self.assertEqual(1, output.count("data-all-links-new-tab"))
        self.assertLess(output.index("data-all-links-new-tab"), output.rindex("</body>"))

    def test_javascript_pseudo_links_are_left_alone(self) -> None:
        links = (
            '<a href="javascript:void(0)">control</a>',
            '<a href="JavaScript:void(0)" target="_self">control</a>',
            '<a href="javascript:void(0)" target="_blank" rel="opener">control</a>',
        )
        for link in links:
            with self.subTest(link=link):
                self.assertEqual(link, self.transform(link))

    def test_links_without_href_or_with_empty_href_are_left_alone(self) -> None:
        links = (
            '<a name="anchor">not a link</a>',
            '<a href="">empty</a>',
            '<a href="   ">blank</a>',
            '<area alt="no href">',
        )
        for link in links:
            with self.subTest(link=link):
                self.assertEqual(link, self.transform(link))

    def test_non_link_markup_is_preserved_exactly(self) -> None:
        markup = (
            "<!doctype html>\n<html lang=\"en\"><body data-x=\"a > b\">\n"
            "<!-- <a href=\"https://comment.example\">comment</a> -->\n"
            "<script>var tpl = '<a href=\"https://script.example\">s</a>';</script>\n"
            "<style>.x::after { content: '<a href=\"https://style.example\">'; }</style>\n"
            "<p title=\"a > b\">A &amp; B</p>\n"
            "<a class=\"x\" href='https://example.com/a?x=1&amp;y=2' title=\"a > b\">A &amp; B</a>\n"
            "</body></html>"
        )
        output = self.transform(markup)
        self.assertIn(
            "<!-- <a href=\"https://comment.example\">comment</a> -->", output
        )
        self.assertIn(
            "<script>var tpl = '<a href=\"https://script.example\">s</a>';</script>",
            output,
        )
        self.assertIn(
            "<style>.x::after { content: '<a href=\"https://style.example\">'; }</style>",
            output,
        )
        self.assertIn("<body data-x=\"a > b\">", output)
        self.assertIn("<p title=\"a > b\">A &amp; B</p>", output)
        self.assertIn(
            "<a class=\"x\" href='https://example.com/a?x=1&amp;y=2' "
            "title=\"a > b\" target=\"_blank\" rel=\"noopener noreferrer\">",
            output,
        )
        self.assertEqual(
            output.count("<a "), markup.count("<a "),
            "only the real anchor should gain attributes, not new anchors",
        )
        self.assertEqual(1, output.count("data-all-links-new-tab"))

    def test_anchor_markup_inside_a_quoted_attribute_is_preserved(self) -> None:
        markup = (
            '<div title="a > b" data-html="<a href=\'/x\'>n</a>">'
            "<a href=\"/real\">real</a></div>"
            '<div data-html=\'<a href="/y">m</a>\'>later</div>'
        )
        output = self.transform(markup)
        self.assertIn(
            '<div title="a > b" data-html="<a href=\'/x\'>n</a>">', output
        )
        self.assertIn('<div data-html=\'<a href="/y">m</a>\'>', output)
        self.assertEqual(0, output.count("target="))
        anchors = collect_anchors(output)
        self.assertEqual(["/real"], [anchor["href"] for anchor in anchors])
        self.assertIsNone(anchors[0].get("target"))

    def test_entity_escaped_attribute_text_is_preserved(self) -> None:
        markup = (
            '<div data-html="&lt;a href=&#39;/x&#39;&gt;n&lt;/a&gt;"'
            ' data-note="<area href=\'/y\'>">ok</div>'
        )
        self.assertEqual(markup, self.transform(markup))

    def test_title_and_xmp_text_is_not_treated_as_markup(self) -> None:
        markup = (
            '<html><head><title><a href="/t">t</a></title></head>'
            '<body><xmp><a href="/x">x</a></xmp>'
            '<a href="/real">r</a></body></html>'
        )
        output = self.transform(markup)
        self.assertIn('<title><a href="/t">t</a></title>', output)
        self.assertIn('<xmp><a href="/x">x</a></xmp>', output)
        self.assertEqual(0, output.count("target="))
        self.assertEqual(1, output.count('href="/real"'))

    def test_entity_encoded_javascript_pseudo_links_are_left_alone(self) -> None:
        links = (
            '<a href="javascript&#58;void(0)">c</a>',
            '<a href="&#106;avascript:void(0)">c</a>',
            '<a href="&#x6A;avascript:void(0)">c</a>',
            '<a href="java&#115;cript:void(0)">c</a>',
            '<a href="javascript&colon;void(0)">c</a>',
            '<a href="&#0000106;avascript:void(0)">c</a>',
            '<a href="java\tscript:void(0)">c</a>',
            '<a href="  javascript&#58;void(0)">c</a>',
        )
        for link in links:
            with self.subTest(link=link):
                self.assertEqual(link, self.transform(link))

        encoded_url = '<a href="https://example.org/?a=1&amp;b=2">x</a>'
        anchor = self.anchor(encoded_url)
        # HTMLParser decodes character references in attribute values.
        self.assertEqual("https://example.org/?a=1&b=2", anchor["href"])
        self.assertEqual("_blank", anchor["target"])

    def test_end_tags_and_declarations_are_copied_as_a_unit(self) -> None:
        markup = (
            "<!doctype html><html><body>"
            '<div data-x="<a href=\'/bad\'>">x</div>'
            '</div data-y="<a href=\'/bad2\'>">'
            '<?xml-stylesheet href="<a href=\'/bad\'>"?>'
            '<a href="/real">r</a></body></html>'
        )
        output = self.transform(markup)
        self.assertIn('<div data-x="<a href=\'/bad\'>">x</div>', output)
        self.assertIn('</div data-y="<a href=\'/bad2\'>">', output)
        self.assertIn('<?xml-stylesheet href="<a href=\'/bad\'>"?>', output)
        self.assertEqual(0, output.count("target="))

    def test_transform_is_idempotent(self) -> None:
        markup = (
            "<!doctype html><html><body>"
            '<a href="https://example.org" rel="noopener">x</a>'
            '<a href="/team/">in</a>'
            '<a href="/team/" target="_blank" rel="noopener noreferrer">old</a>'
            "</body></html>"
        )
        once = self.transform(markup)
        twice = self.transform(once)
        self.assertEqual(once, twice)
        self.assertEqual(1, twice.count("data-all-links-new-tab"))

    def test_area_elements_are_links_too(self) -> None:
        internal = self.anchor('<map><area href="/team/" alt="team"></map>')
        self.assertIsNone(internal.get("target"))

        external = self.anchor('<map><area href="https://example.org/" alt="x"></map>')
        self.assertEqual("_blank", external["target"])
        self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(external)))

    def test_self_closing_and_unquoted_values_keep_their_urls(self) -> None:
        output = self.transform(
            '<map><area href="/team/"/>'
            '<area href="https://example.org/" /></map><a href=/team/>x</a>'
        )
        anchors = collect_anchors(output)
        self.assertEqual(
            ["/team/", "https://example.org/", "/team/"],
            [a["href"] for a in anchors],
        )
        self.assertIsNone(anchors[0].get("target"))
        self.assertEqual("_blank", anchors[1]["target"])
        self.assertIsNone(anchors[2].get("target"))


class DynamicAnchorRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not shutil.which("node"):
            raise unittest.SkipTest("node is not available")
        # Always exercise the runtime generated from the current plugin source;
        # test_injected_runtime_matches_current_plugin_source() reports when the
        # built site has not caught up with it.
        runtime = source_runtime()
        if runtime is None:
            raise unittest.SkipTest("ruby is not available to derive the runtime")
        cls.runtime = runtime

    def run_runtime(self, anchors: list[dict[str, object]]) -> dict[str, Any]:
        """Evaluate the runtime against fake anchors and return their state."""

        harness = r"""
const vm = require("vm");

function element(spec, attrs) {
  return {
    nodeType: 1,
    tagName: (spec.tag || "A").toUpperCase(),
    attrs,
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
    setAttribute(name, value) { this.attrs[name] = String(value); },
    removeAttribute(name) { delete this.attrs[name]; },
    hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name); },
  };
}

const anchors = JSON.parse(process.argv[1]).map((spec) => element(spec, Object.assign({}, spec.attrs)));
let observerCallback = null;
class FakeMutationObserver {
  constructor(callback) { observerCallback = callback; }
  observe() {}
}
const document = {
  readyState: "complete",
  baseURI: "https://acangi.github.io/",
  documentElement: {},
  addEventListener() {},
  querySelectorAll(selector) {
    return anchors.filter((anchor) =>
      selector.includes("a[href]") ? anchor.tagName === "A" : anchor.tagName === "AREA"
    );
  },
};
const window = {
  MutationObserver: FakeMutationObserver,
  location: { href: "https://acangi.github.io/", hostname: "acangi.github.io", protocol: "https:", port: "" },
};
const sandbox = { document, window, console, URL };
vm.runInNewContext(process.argv[2], sandbox);

const dynamicExternal = element({}, { href: "https://dynamic.example/path" });
observerCallback([{ type: "childList", addedNodes: [dynamicExternal] }]);

const dynamicInternal = element({}, { href: "/team/" });
observerCallback([{ type: "childList", addedNodes: [dynamicInternal] }]);

const late = element({}, {});
observerCallback([{ type: "childList", addedNodes: [late] }]);
late.attrs.href = "https://late.example/";
observerCallback([{ type: "attributes", target: late }]);
late.attrs.href = "/team/";
observerCallback([{ type: "attributes", target: late }]);

process.stdout.write(JSON.stringify({
  static: anchors.map((anchor) => anchor.attrs),
  dynamicExternal: dynamicExternal.attrs,
  dynamicInternal: dynamicInternal.attrs,
  late: late.attrs,
}));
"""
        payload = json.dumps(anchors)
        result = subprocess.run(
            ["node", "-e", harness, payload, self.runtime],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        return json.loads(result.stdout)

    def test_static_links_follow_the_context_aware_policy(self) -> None:
        result = self.run_runtime(
            [
                {"attrs": {"href": "https://static.example"}},
                {"attrs": {"href": "//cdn.example/lib.js"}},
                {"attrs": {"href": "https://acangi.github.io.evil.example/x"}},
                {"attrs": {"href": "/internal/"}},
                {"attrs": {"href": "#main-content", "class": "skip-link"}},
                {
                    "attrs": {
                        "href": "/stale/",
                        "target": "_blank",
                        "rel": "noopener noreferrer",
                    }
                },
                {"attrs": {"href": "https://acangi.github.io/team/"}},
                {"attrs": {"href": "mailto:a@example.org"}},
                {"attrs": {"href": "/paper.pdf", "download": ""}},
                {"attrs": {"href": "tel:+123"}},
                {"attrs": {"href": "javascript:void(0)"}},
                {"attrs": {"href": "java\tscript:void(0)"}},
                {"attrs": {"href": "https://acangi.github.io:8443/x"}},
            ]
        )
        static = result["static"]
        for index in range(3):
            with self.subTest(anchor=static[index]):
                self.assertEqual("_blank", static[index]["target"])
                self.assertIn("noopener", static[index]["rel"])
                self.assertIn("noreferrer", static[index]["rel"])
        for index in range(3, 7):
            with self.subTest(anchor=static[index]):
                self.assertNotIn("target", static[index])
                self.assertNotIn("rel", static[index])
        self.assertEqual("mailto:a@example.org", static[7]["href"])
        self.assertNotIn("target", static[7])
        self.assertEqual("/paper.pdf", static[8]["href"])
        self.assertIn("download", static[8])
        self.assertNotIn("target", static[8])
        self.assertEqual("tel:+123", static[9]["href"])
        self.assertNotIn("target", static[9])
        self.assertNotIn("target", static[10])
        self.assertNotIn("rel", static[10])
        # URL parsing drops ASCII tab, so this is a javascript: pseudo-link too.
        self.assertNotIn("target", static[11])
        self.assertNotIn("rel", static[11])
        # A non-default port on the canonical host is a different origin.
        self.assertEqual("_blank", static[12]["target"])
        self.assertIn("noopener", static[12]["rel"])
        self.assertIn("noreferrer", static[12]["rel"])

    def test_backslash_authorities_match_the_browser(self) -> None:
        # Native URL parsing (not a /?# split) must decide the host, because
        # WHATWG turns "\" into "/" for special schemes before parsing the
        # authority.  Both directions matter: a backslash can hide an external
        # host behind canonical-looking text, or hide the canonical host
        # behind external-looking text.
        result = self.run_runtime(
            [
                {"attrs": {"href": r"https://attacker.example\@acangi.github.io/team/"}},
                {"attrs": {"href": r"https://evil.example\/@acangi.github.io/"}},
                {"attrs": {"href": r"\\evil.example\team"}},
                {"attrs": {"href": r"https://acangi.github.io\@attacker.example/team/"}},
                {"attrs": {"href": r"https://acangi.github.io\/@evil.example/team/"}},
                {"attrs": {"href": "\\\\acangi.github.io\\team\\"}},
            ]
        )
        static = result["static"]
        for index in (0, 1, 2):
            with self.subTest(anchor=static[index]):
                self.assertEqual("_blank", static[index]["target"])
                self.assertIn("noopener", static[index]["rel"])
                self.assertIn("noreferrer", static[index]["rel"])
        for index in (3, 4, 5):
            with self.subTest(anchor=static[index]):
                self.assertNotIn("target", static[index])
                self.assertNotIn("rel", static[index])

    def test_trailing_dot_host_matches_the_static_policy(self) -> None:
        # A root dot is not a different DNS owner; native URL parsing keeps it
        # in hostname/origin, so the runtime normalizes it like the static
        # transform does.
        result = self.run_runtime(
            [
                {"attrs": {"href": "https://acangi.github.io./team/"}},
                {"attrs": {"href": "https://attacker.example./team/"}},
            ]
        )
        static = result["static"]
        self.assertNotIn("target", static[0])
        self.assertEqual("_blank", static[1]["target"])

    def test_dynamic_and_late_links_follow_the_same_policy(self) -> None:
        result = self.run_runtime([])
        self.assertEqual("_blank", result["dynamicExternal"]["target"])
        self.assertIn("noopener", result["dynamicExternal"]["rel"])
        self.assertIn("noreferrer", result["dynamicExternal"]["rel"])
        self.assertNotIn("target", result["dynamicInternal"])
        self.assertNotIn("rel", result["dynamicInternal"])
        self.assertNotIn("target", result["late"])
        self.assertNotIn("rel", result["late"])


class _PageHandler(http.server.BaseHTTPRequestHandler):
    """Serve one already-built HTML page for the browser-level test."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        body = self.server.page.encode("utf-8")  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class BrowserMutationObserverTests(unittest.TestCase):
    """Run the injected runtime in real Chromium with a real MutationObserver.

    The Node tests use fake anchors, so they cannot show that the observer's
    own attribute writes feed back into the observer.  This test inserts
    anchors after load and proves both that they are rewritten and that a
    macrotask timer still fires, which fails if secure() keeps re-queueing
    mutation records.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.chrome = _chrome_command()
        if cls.chrome is None:
            raise unittest.SkipTest("Chromium/Chrome is not available")
        runtime = source_runtime()
        if runtime is None:
            raise unittest.SkipTest("ruby is not available to derive the runtime")
        cls.runtime = runtime

    def page(self, port: int) -> str:
        return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>observer feedback</title></head>
<body>
<a id="same-origin" href="http://127.0.0.1:{port}/team/" target="_blank"
   rel="noopener noreferrer">same origin</a>
<a id="other-host" href="http://localhost:{port}/team/">other host</a>
<a id="other-port" href="http://127.0.0.1:1/team/">other port</a>
<a id="bs-canonical-external" href="https://attacker.example\\@acangi.github.io/team/">deceptive external</a>
<a id="bs-canonical-internal" href="https://acangi.github.io\\@evil.example/team/">deceptive internal</a>
<a id="bs-preview-external" href="http://attacker.example\\@127.0.0.1:{port}/team/">preview external</a>
<a id="bs-preview-internal" href="http://127.0.0.1:{port}\\@evil.example/team/">preview internal</a>
<div id="mount"></div>
<script>
{self.runtime}
</script>
<script>
(function () {{
  var mount = document.getElementById("mount");
  var external = document.createElement("a");
  external.id = "dynamic-external";
  external.setAttribute("href", "https://dynamic.example/path");
  mount.appendChild(external);
  var internal = document.createElement("a");
  internal.id = "dynamic-internal";
  internal.setAttribute("href", "/team/");
  mount.appendChild(internal);
  setTimeout(function () {{
    function value(id, name) {{
      var found = document.getElementById(id).getAttribute(name);
      return found === null ? "none" : found;
    }}
    document.body.setAttribute("data-timer", "fired");
    document.body.setAttribute("data-external-target", value("dynamic-external", "target"));
    document.body.setAttribute("data-external-rel", value("dynamic-external", "rel"));
    document.body.setAttribute("data-internal-target", value("dynamic-internal", "target"));
    document.body.setAttribute("data-internal-rel", value("dynamic-internal", "rel"));
    document.body.setAttribute("data-same-origin-target", value("same-origin", "target"));
    document.body.setAttribute("data-same-origin-rel", value("same-origin", "rel"));
    document.body.setAttribute("data-other-host-target", value("other-host", "target"));
    document.body.setAttribute("data-other-port-target", value("other-port", "target"));
    document.body.setAttribute("data-bs-canonical-external-target", value("bs-canonical-external", "target"));
    document.body.setAttribute("data-bs-canonical-internal-target", value("bs-canonical-internal", "target"));
    document.body.setAttribute("data-bs-preview-external-target", value("bs-preview-external", "target"));
    document.body.setAttribute("data-bs-preview-internal-target", value("bs-preview-internal", "target"));
  }}, 100);
}})();
</script>
</body></html>"""

    def test_observer_writes_are_idempotent_and_timers_still_fire(self) -> None:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _PageHandler)
        port = server.server_address[1]
        server.page = self.page(port)  # type: ignore[attr-defined]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        result: subprocess.CompletedProcess[str] | None = None
        chrome = self.chrome
        assert chrome is not None
        try:
            with tempfile.TemporaryDirectory() as profile:
                command = chrome + [
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    f"--user-data-dir={profile}",
                    "--virtual-time-budget=5000",
                    "--dump-dom",
                    f"http://127.0.0.1:{port}/",
                ]
                try:
                    result = subprocess.run(
                        command, capture_output=True, text=True, timeout=30
                    )
                except subprocess.TimeoutExpired:
                    self.fail(
                        "browser never yielded to the timer: the MutationObserver "
                        "is re-triggering itself with identical attribute writes"
                    )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        assert result is not None
        self.assertEqual(0, result.returncode, result.stderr[-3000:])
        dom = result.stdout
        # The inserted external anchor was secured...
        self.assertIn('data-external-target="_blank"', dom)
        self.assertIn('data-external-rel="noopener noreferrer"', dom)
        # ...the inserted internal anchor stayed in the same tab...
        self.assertIn('data-internal-target="none"', dom)
        self.assertIn('data-internal-rel="none"', dom)
        # ...and the timer fired, so mutation callbacks do not starve tasks.
        self.assertIn('data-timer="fired"', dom)
        # The runtime adapts to the actual preview origin (127.0.0.1:PORT):
        # its own origin is internal even though the configured site is
        # acangi.github.io, while localhost is a different host and stays
        # external.
        self.assertIn('data-same-origin-target="none"', dom)
        self.assertIn('data-same-origin-rel="none"', dom)
        self.assertIn('data-other-host-target="_blank"', dom)
        self.assertIn('data-other-port-target="_blank"', dom)
        # The real browser resolves "\" as "/" before the authority.  The
        # runtime must agree in both directions, on the configured origin and
        # on the preview origin actually served here.
        self.assertIn('data-bs-canonical-external-target="_blank"', dom)
        self.assertIn('data-bs-canonical-internal-target="none"', dom)
        self.assertIn('data-bs-preview-external-target="_blank"', dom)
        self.assertIn('data-bs-preview-internal-target="none"', dom)


if __name__ == "__main__":
    unittest.main()
