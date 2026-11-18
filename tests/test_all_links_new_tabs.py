"""Tests for the all-links-open-in-new-tabs policy.

The site-wide policy is implemented by
``_plugins/open-links-in-new-tabs.rb``: a post-render hook rewrites every
``<a>``/``<area>`` element with a non-empty ``href`` in rendered pages and
documents, and injects one small runtime that applies the same rule to anchors
inserted by client-side JavaScript.

Run the integration half against a built site::

    SITE_DIR=/tmp/site python -m pytest tests/test_all_links_new_tabs.py -v

There are three groups of tests:

* :class:`RenderedLinkTests` inspects the actual built HTML and fails if any
  rendered link with a non-empty ``href`` is missing ``target="_blank"`` or
  either ``noopener``/``noreferrer``.
* :class:`PluginTransformTests` unit-tests the Ruby transformer on fixtures,
  including markup that must be preserved byte-for-byte (scripts, comments,
  raw-text elements, quoted ``>`` characters, entities, and anchor-like text
  inside another tag's quoted attribute value).
* :class:`DynamicAnchorRuntimeTests` runs the injected runtime under Node with
  a minimal DOM to prove that client-inserted anchors and late ``href``
  changes are secured too.

The policy is literal: same-document fragments (the skip link, table-of-contents
links), ``mailto:``/``tel:``/``sms:``, ``data:``/``blob:`` URLs, and links with
the ``download`` attribute all get ``target="_blank"`` plus both security
tokens.  ``mailto:``/``tel:`` still hand off to the browser's protocol handler
and ``download`` still hands off to the browser's download manager; the plugin
keeps those schemes/attributes intact rather than replacing them.  The only
exception is ``javascript:`` pseudo-links, which are UI controls, not
navigations, and are left byte-for-byte unchanged.  A ``javascript:`` scheme
spelled with character references (``href="&#106;avascript&#58;..."``) or split
by ASCII tab/newline is recognised as a pseudo-link too, because browsers
decode and normalize attribute URLs before parsing the scheme.
"""

from __future__ import annotations

import html as html_module
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import unittest
from html.parser import HTMLParser
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_PATH = REPO_ROOT / "_plugins" / "open-links-in-new-tabs.rb"
SITE_DIR = Path(os.environ.get("SITE_DIR", REPO_ROOT / "_site"))

PSEUDO_SCHEMES = ("javascript:",)
SECURITY_TOKENS = ("noopener", "noreferrer")


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


def is_pseudo_link(anchor: dict[str, str | None]) -> bool:
    """UI controls that are not navigations and stay untouched."""

    href = (anchor.get("href") or "").strip()
    return href.lower().startswith(PSEUDO_SCHEMES)


def is_targeted(anchor: dict[str, str | None]) -> bool:
    """An anchor/area with a non-empty, non-javascript href."""

    href = (anchor.get("href") or "").strip()
    return bool(href) and not is_pseudo_link(anchor)


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

    def test_every_rendered_link_opens_in_a_new_tab(self) -> None:
        checked = 0
        failures: list[str] = []
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if not is_targeted(anchor):
                    continue
                checked += 1
                if anchor.get("target") != "_blank":
                    failures.append(f"{page}: {anchor}")
        self.assertGreater(checked, 400, "the rendered site should contain many links")
        self.assertEqual([], failures, "links missing target=_blank")

    def test_every_link_has_both_security_tokens(self) -> None:
        failures: list[str] = []
        checked = 0
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if not is_targeted(anchor):
                    continue
                checked += 1
                tokens = rel_tokens(anchor)
                missing = [token for token in SECURITY_TOKENS if token not in tokens]
                if missing:
                    failures.append(f"{page}: missing {missing}: {anchor}")
        self.assertGreater(checked, 400)
        self.assertEqual([], failures, "links missing noopener/noreferrer")

    def test_every_blank_target_is_secure(self) -> None:
        failures: list[str] = []
        for path in self.pages:
            page = path.relative_to(SITE_DIR)
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if is_pseudo_link(anchor) or anchor.get("target") != "_blank":
                    continue
                tokens = rel_tokens(anchor)
                missing = [token for token in SECURITY_TOKENS if token not in tokens]
                if missing:
                    failures.append(f"{page}: target=_blank missing {missing}: {anchor}")
        self.assertEqual([], failures)

    def test_internal_navigation_links_are_included(self) -> None:
        index = (SITE_DIR / "index.html").read_text(encoding="utf-8")
        internal = [
            anchor
            for anchor in collect_anchors(index)
            if (anchor.get("href") or "").startswith(("/", "./", "../"))
            and not (anchor.get("href") or "").startswith("//")
        ]
        self.assertTrue(internal, "expected internal links on the homepage")
        for anchor in internal:
            with self.subTest(anchor=anchor.get("href")):
                self.assertEqual("_blank", anchor.get("target"))
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_skip_link_also_opens_in_a_new_tab(self) -> None:
        index = (SITE_DIR / "index.html").read_text(encoding="utf-8")
        skip_links = [
            anchor
            for anchor in collect_anchors(index)
            if "skip-link" in (anchor.get("class") or "").split()
        ]
        self.assertTrue(skip_links, "missing skip link")
        for anchor in skip_links:
            self.assertTrue((anchor.get("href") or "").startswith("#"))
            self.assertEqual("_blank", anchor.get("target"))
            self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_email_and_telephone_links_keep_their_scheme(self) -> None:
        mailtos: list[dict[str, str | None]] = []
        for path in self.pages:
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                href = (anchor.get("href") or "").lower()
                if href.startswith(("mailto:", "tel:")):
                    mailtos.append(anchor)
        self.assertTrue(mailtos, "expected at least one email link")
        for anchor in mailtos:
            with self.subTest(href=anchor.get("href")):
                self.assertTrue((anchor.get("href") or "").lower().startswith(("mailto:", "tel:")))
                self.assertEqual("_blank", anchor.get("target"))
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_no_link_forces_the_opener_relationship(self) -> None:
        for path in self.pages:
            for anchor in collect_anchors(path.read_text(encoding="utf-8")):
                if is_pseudo_link(anchor):
                    continue
                self.assertNotIn("opener", rel_tokens(anchor),
                                 f"{path}: rel=opener is unsafe")

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

    def transform(self, markup: str) -> str:
        command = self.ruby + [
            "-r",
            self.plugin_arg,
            "-e",
            "STDOUT.write(Jekyll::AllLinksInNewTabs.transform(STDIN.read))",
        ]
        result = subprocess.run(
            command, input=markup, text=True, capture_output=True, check=False
        )
        self.assertEqual(0, result.returncode, result.stderr)
        return result.stdout

    def test_external_and_internal_links_are_rewritten(self) -> None:
        output = self.transform(
            '<p>Hi</p><a href="https://example.org">out</a>'
            '<a href="/team/">in</a>'
        )
        self.assertIn(
            '<a href="https://example.org" target="_blank" rel="noopener noreferrer">out</a>',
            output,
        )
        self.assertIn(
            '<a href="/team/" target="_blank" rel="noopener noreferrer">in</a>',
            output,
        )

    def test_existing_rel_is_merged_and_opener_is_removed(self) -> None:
        output = self.transform(
            '<a href="https://example.org" rel="external nofollow opener">x</a>'
        )
        self.assertIn(
            'rel="external nofollow noopener noreferrer"', output
        )
        self.assertNotIn("opener", rel_tokens(collect_anchors(output)[0]))

    def test_existing_target_is_normalized(self) -> None:
        output = self.transform('<a href="/team/" target="_self">x</a>')
        anchor = collect_anchors(output)[0]
        self.assertEqual("_blank", anchor["target"])
        self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

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

    def test_fragments_email_telephone_download_and_data_links_are_targeted(self) -> None:
        cases = (
            ('<a class="skip-link" href="#main-content">Skip</a>', "#main-content"),
            ('<a href="mailto:a@example.org">mail</a>', "mailto:a@example.org"),
            ('<a href="tel:+123">phone</a>', "tel:+123"),
            ('<a href="sms:+123">sms</a>', "sms:+123"),
            ('<a href="data:text/plain,hello">data</a>', "data:text/plain,hello"),
            ('<a href="blob:https://example.org/id">blob</a>', "blob:https://example.org/id"),
            ('<a href="/paper.pdf" download>pdf</a>', "/paper.pdf"),
        )
        for markup, href in cases:
            with self.subTest(href=href):
                output = self.transform(markup)
                anchor = collect_anchors(output)[0]
                self.assertEqual(href, anchor["href"])
                self.assertEqual("_blank", anchor["target"])
                self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))
                if "download" in markup:
                    self.assertIn("download", anchor)

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
        """Literal ``<a>`` text inside another tag's attribute is not markup.

        Only real anchor start tags may be rewritten; a quoted attribute value
        must stay byte-for-byte intact even when it contains anchor-like text.
        """

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
        self.assertEqual(1, output.count("target="))
        anchors = collect_anchors(output)
        self.assertEqual(["/real"], [anchor["href"] for anchor in anchors])
        self.assertEqual("_blank", anchors[0]["target"])
        self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchors[0])))

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
        self.assertEqual(1, output.count("target="))
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
        anchor = collect_anchors(self.transform(encoded_url))[0]
        # HTMLParser decodes character references in attribute values.
        self.assertEqual("https://example.org/?a=1&b=2", anchor["href"])
        self.assertEqual("_blank", anchor["target"])

    def test_end_tags_and_declarations_are_copied_as_a_unit(self) -> None:
        """Only real anchor start tags count; tag syntax is never re-entered."""

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
        self.assertEqual(1, output.count("target="))

    def test_transform_is_idempotent(self) -> None:
        markup = (
            "<!doctype html><html><body>"
            '<a href="https://example.org" rel="noopener">x</a>'
            '<a href="/team/">in</a>'
            "</body></html>"
        )
        once = self.transform(markup)
        twice = self.transform(once)
        self.assertEqual(once, twice)
        self.assertEqual(1, twice.count("data-all-links-new-tab"))

    def test_area_elements_are_links_too(self) -> None:
        output = self.transform('<map><area href="/team/" alt="team"></map>')
        anchor = collect_anchors(output)[0]
        self.assertEqual("_blank", anchor["target"])
        self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))

    def test_self_closing_and_unquoted_values_keep_their_urls(self) -> None:
        output = self.transform(
            '<map><area href="/team/"/>'
            '<area href="/team/" /></map><a href=/team/>x</a>'
        )
        anchors = collect_anchors(output)
        self.assertEqual(["/team/", "/team/", "/team/"], [a["href"] for a in anchors])
        for anchor in anchors:
            self.assertEqual("_blank", anchor["target"])
            self.assertTrue(set(SECURITY_TOKENS).issubset(rel_tokens(anchor)))


@unittest.skipUnless(SITE_DIR.is_dir(), "build the site or set SITE_DIR")
class DynamicAnchorRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not shutil.which("node"):
            raise unittest.SkipTest("node is not available")
        index = SITE_DIR / "index.html"
        markup = index.read_text(encoding="utf-8")
        match = re.search(
            r"<script\b[^>]*\bdata-all-links-new-tab\b[^>]*>(.*?)</script>",
            markup,
            re.S | re.I,
        )
        if not match:
            raise unittest.SkipTest("runtime not injected into the built site")
        cls.runtime = html_module.unescape(match.group(1))

    def run_runtime(self, anchors: list[dict[str, object]]) -> dict[str, Any]:
        """Evaluate the runtime against fake anchors and return their state."""

        harness = r"""
const vm = require("vm");

const anchors = JSON.parse(process.argv[1]).map((spec) => {
  const attrs = Object.assign({}, spec.attrs);
  return {
    tagName: (spec.tag || "A").toUpperCase(),
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(attrs, name) ? attrs[name] : null; },
    setAttribute(name, value) { attrs[name] = String(value); },
    hasAttribute(name) { return Object.prototype.hasOwnProperty.call(attrs, name); },
    attrs,
  };
});
let observerCallback = null;
class FakeMutationObserver {
  constructor(callback) { observerCallback = callback; }
  observe() {}
}
const document = {
  readyState: "complete",
  documentElement: {},
  addEventListener() {},
  querySelectorAll(selector) {
    return anchors.filter((anchor) =>
      selector.includes("a[href]") ? anchor.tagName === "A" : anchor.tagName === "AREA"
    );
  },
};
const sandbox = { document, window: { MutationObserver: FakeMutationObserver }, console };
vm.runInNewContext(process.argv[2], sandbox);

const dynamic = {
  nodeType: 1,
  tagName: "A",
  getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
  setAttribute(name, value) { this.attrs[name] = String(value); },
  hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name); },
  attrs: { href: "https://dynamic.example/path" },
};
observerCallback([{ type: "childList", addedNodes: [dynamic] }]);

const late = {
  nodeType: 1,
  tagName: "A",
  getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
  setAttribute(name, value) { this.attrs[name] = String(value); },
  hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name); },
  attrs: {},
};
observerCallback([{ type: "childList", addedNodes: [late] }]);
late.attrs.href = "https://late.example/";
observerCallback([{ type: "attributes", target: late }]);

process.stdout.write(JSON.stringify({
  static: anchors.map((anchor) => anchor.attrs),
  dynamic: dynamic.attrs,
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

    def test_static_dynamic_and_late_links_are_all_secured(self) -> None:
        result = self.run_runtime(
            [
                {"attrs": {"href": "https://static.example"}},
                {"attrs": {"href": "/internal/"}},
                {"attrs": {"href": "#main-content", "class": "skip-link"}},
                {"attrs": {"href": "mailto:a@example.org"}},
                {"attrs": {"href": "/paper.pdf", "download": ""}},
                {"attrs": {"href": "tel:+123"}},
                {"attrs": {"href": "javascript:void(0)"}},
                {"attrs": {"href": "java\tscript:void(0)"}},
            ]
        )
        static = result["static"]
        for index in range(6):
            with self.subTest(anchor=static[index]):
                self.assertEqual("_blank", static[index]["target"])
                self.assertIn("noopener", static[index]["rel"])
                self.assertIn("noreferrer", static[index]["rel"])
        self.assertEqual("mailto:a@example.org", static[3]["href"])
        self.assertEqual("/paper.pdf", static[4]["href"])
        self.assertIn("download", static[4])
        self.assertEqual("tel:+123", static[5]["href"])
        self.assertNotIn("target", static[6])
        self.assertNotIn("rel", static[6])
        # URL parsing drops ASCII tab, so this is a javascript: pseudo-link too.
        self.assertNotIn("target", static[7])
        self.assertNotIn("rel", static[7])
        self.assertEqual("_blank", result["dynamic"]["target"])
        self.assertIn("noopener", result["dynamic"]["rel"])
        self.assertIn("noreferrer", result["dynamic"]["rel"])
        self.assertEqual("_blank", result["late"]["target"])
        self.assertIn("noreferrer", result["late"]["rel"])


if __name__ == "__main__":
    unittest.main()
