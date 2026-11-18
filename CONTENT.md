# Editing and maintaining the website

This guide explains routine edits to the Jekyll/al-folio website. Edit the Markdown and YAML files in a text editor on the VM. **All names, dates, URLs and publication details in the examples below are fictional placeholders**; replace them with verified and approved information.

> Work in `/home/cangi21/projects/personal-website` on `redesign`. Substantial redesign work is uncommitted. Do not reset, clean, restore or switch branches without protecting and reviewing it. Do not push, merge or deploy without explicit approval. This guide is excluded from the generated website.

## Where content lives

| Content | Edit | Display |
|---|---|---|
| Announcements | A file in `_news/` | `/news/` and the latest four on `/` |
| Team and alumni | `_data/team.yml` | `/team/` |
| Head of Department and CV link | `_pages/team.md` | `/team/` |
| Research highlights | A file in `_highlights/` | `/highlights/`, detail page and research-area links |
| Research descriptions | `_pages/research.md` | `/research/` |
| Homepage introduction and tiles | `_pages/about.md` | `/` |
| CV | `_pages/cv.md` | `/cv/`, linked from the team page |
| Software | `_pages/software.md` | `/software/` |
| Publications | `_bibliography/papers.bib` | Filtered journal articles and unpublished preprints on `/publications/` |

Do not edit `_site/`: it is generated output and may be stale. The publication importer can overwrite the bibliography and several `_data/` files; see [Publications](#publications) below.

## Markdown and YAML basics

Files typically begin with YAML **front matter**, followed by Markdown:

```markdown
---
layout: post
title: "Example: a title with a colon"
date: 2026-11-16
inline: false
---

A paragraph with **bold text**, *emphasis* and a [link](https://example.org/).

## A heading

- First point
- Second point
```

The first `---` must be on line 1; the second closes the metadata. Use spaces, not tabs, for YAML indentation. Quote text values, especially titles with colons. Leave booleans unquoted: `false`, not `"false"`. Check links and use real dates and factual content before publication.

## Add news

Create `_news/YYYY-MM-DD-short-description.md`, using the actual date in both the filename and front matter. `/news/` shows published items newest first; the homepage currently shows four.

### Short item shown directly in the news list

For example, create `_news/2026-11-16-example-announcement.md`:

```markdown
---
layout: post
title: "Example: an announcement"
date: 2026-11-16
inline: true
related_posts: false
---

A brief, verified update. [Read more](https://example.org/).
```

With `inline: true`, the **body**, rather than the title, appears in the listing. Keep it to one short paragraph. The listing does not automatically link to the item's own page, so include a link in the text if needed.

### Longer item with its own page

```markdown
---
layout: post
title: "Example: a longer announcement"
date: 2026-11-16
inline: false
related_posts: false
---

Write the first paragraph here.

Add further paragraphs, links or images as appropriate.
```

With `inline: false`, the listing displays the **title as a link** to a locally hosted detail page. A title is essential. The filename determines the URL, so renaming a published item can break existing links.

Add `published: false` inside the front matter to keep a draft out of the build; remove it when ready. There is no verified automatic scheduled publication: do not rely on a future date to publish an item later. Preview the listing and detail page. To change the homepage count, edit `announcements.limit` in `_pages/about.md`.

## Add or update a team member

Edit `_data/team.yml`. This fictional example shows the complete YAML structure; replace the examples and preserve existing approved entries:

```yaml
- title: Current members
  members:
    - name: "Example Person"
      role: "Postdoctoral researcher"
      email: "person@example.org"
      photo: "/assets/img/team/example-person.jpg"
      start_year: 2024
- title: Alumni
  members:
    - name: "Former Example"
      role: "PhD Candidate"
      start_year: 2020
      end_year: 2025
      last_position: "Researcher, Example Institute"
```

- Replace `members: []` with a `members:` list when adding the first person. Additional `- name:` blocks use the same indentation.
- `name` and `role` identify each person. `email`, `photo`, `start_year`, `end_year` and `last_position` are optional, except that every alumnus needs both years (see below). Omit information that is unavailable; never invent dates. The current-members section hides dates even if recorded; alumni show their years and, when supplied, their last known position.
- Only the fields above are currently rendered. Adding a `website`, `github` or `orcid` field will not automatically display it. The Head of Department's CV and ORCID links are configured in `_pages/team.md`.
- Obtain permission before publishing personal details or photos. For current members, copy an approved photo to `assets/img/team/` and use the matching `/assets/img/team/filename.jpg` path. When a current member's `photo` is omitted, the gender-neutral `avatar-neutral.svg` placeholder is shown. Alumni are displayed in a table without photographs, even when a `photo` value is stored.
- To move a person to Alumni, move their entire block under that category's `members:` list. To remove someone, remove the complete block; use `members: []` if the list becomes empty.
- The Head of Department appears in `team.yml` and is displayed separately using the `head` settings in `_pages/team.md`. Update both when head details change; the template prevents a duplicate card. Team entries have no built-in draft switch.

### Ordering in `team.yml`

The page displays people in the order they appear in the file; the template does **not** sort. Keep the file in the following order, otherwise the page looks wrong (the data checks below will also fail):

1. **Current members:** the Head of Department first, then everyone else alphabetically by surname.
2. **Alumni:** most recent first by `end_year` (descending), and within the same `end_year` alphabetically by surname.

Sort surnames by their **final word**, folding diacritics for comparison only: `Schlünzen` sorts as `Schlunzen`, `Ångström` as `Angstrom`. Never change the stored spelling — names keep their accents and umlauts. Because `end_year` drives the alumni order, every alumnus must have `start_year` and `end_year`, and `end_year` must not be earlier than `start_year`; use the same year twice for a single-year stay.

After moving, adding or removing people, check the file without a Jekyll build:

```bash
python3 -m unittest tests.test_team_data -v
```

The test derives the expected order from the file itself, so it fails only when the file's order breaks the rules above; it does not contain a copy of the roster.

## Add a research highlight

Create `_highlights/short-descriptive-name.md`. Its filename forms part of the local URL; renaming it later can break links. Copy an existing file or adapt this fictional template:

```markdown
---
layout: highlight
title: "Example: a research result"
short_title: "A short link title"
citation: "Example Journal 1, 123 (2026)"
journal: "Example Journal"
year: 2026
date: 2026-11-16
order: 0
research_area: electronic-structure
doi: "10.0000/example"
summary: >-
  A short, factual explanation of the result for a broad audience.
---

Describe the research question, approach and result in your own words.

Add context and limitations as appropriate, checking the description against
the paper rather than reproducing a publisher's abstract.
```

Replace **every** example value, including the fictitious DOI. `short_title` is used for the research-page link; `summary` is plain text on the listing and detail page. Put Markdown formatting and links in the body. Enter the **bare DOI**, not the full `https://doi.org/` URL, because the template adds that prefix.

`order` controls both highlight and research-area link ordering: lower numbers come first, irrespective of `date`. Choose a unique number and preview the result. Existing highlights currently use `1`–`7`, so `0` places an item first.

### Research-area keys

Use one value exactly, with no surrounding whitespace:

| Key | Research area |
|---|---|
| `electronic-structure` | Scalable Machine Learning for Electronic-Structure Theory |
| `representation-learning` | Representation Learning for Chemical Space and Materials |
| `quantum-dynamics` | Quantum Dynamics and Electronic Transport |
| `magnetic-materials` | Magnetic Materials and Spin-Lattice Dynamics |
| `thz-driven-systems` | THz-Driven Materials and Molecular Systems |

An invalid key does **not** stop the Jekyll build; the highlight may be missing from its research section. Run the authoring check from the repository root with Python and PyYAML installed:

```bash
python3 -m unittest tests.test_research_highlights -v
```

Preview `/highlights/`, the individual page, DOI, and matching section of `/research/`. The test reads the keys from `_pages/research.md`; it is not automatically run by Jekyll.

## Edit other content

- **Homepage:** update text in `_pages/about.md`. It also contains HTML, Liquid and tile styling; avoid changing markup when only editing text. News is generated separately; selected papers are not displayed on the homepage.
- **Research:** edit descriptions in `_pages/research.md`. Adding or renaming an area is not yet a simple content-only task: its section identifier, heading anchor, highlight include, homepage tile, icon, key table and tests must stay in sync. Ask a developer for help.
- **Software:** `_pages/software.md` retains the software page and URL, but it is currently hidden from navigation and has no homepage spotlight.
- **CV:** edit `_pages/cv.md`. It is a separate Markdown copy; updating the original Quarto (`.qmd`) file does not update the website automatically. Review personal and appointment information before publication.

### Publications

`_bibliography/papers.bib` is the Jekyll Scholar source. The publications page filters this bibliography to journal articles and preprints without a journal version; other records remain in the bibliography but are not shown. Manually curated preprint-to-journal matches are in `_data/publication_policy.yml`. The homepage does not currently display selected papers. The total number of bibliography records is not a count of peer-reviewed papers.

**Do not run `_scripts/openalex_to_yaml.py` in the real worktree, even with `--help`.** Its entry point runs the importer and can overwrite the bibliography and generated `_data/` files. Test future imports in an isolated copy and review curated fields and selected papers. The scheduled GitHub Actions publication workflow can write and push generated data on the default branch; review its behaviour before relying on it.

## Build and preview on the VM

A preview is not a deployment. This VM currently has Podman and the `amirpourmand/al-folio:v0.16.2` image; Ruby and Bundler are not installed on the host. The commands below build a **snapshot in scratch storage**, leaving the working tree and its existing `_site/` untouched. The image uses Ruby 4.0.1 while CI uses Ruby 3.3, so success here is not proof of CI equivalence.

In a VM terminal:

```bash
cd /home/cangi21/projects/personal-website

# Create an isolated source snapshot and output directory.
PREVIEW_DIR=$(mktemp -d "$HOME/.hermes/cache/scratch/website-preview-XXXXXX")
mkdir -p "$PREVIEW_DIR/src" "$PREVIEW_DIR/output"
tar --exclude='./.git' --exclude='./_site' --exclude='./.jekyll-cache' \
    --exclude='./.sass-cache' --exclude='./.venv' --exclude='./vendor' \
    --exclude='*/__pycache__' -cf - . | tar -xf - -C "$PREVIEW_DIR/src"

# An explicit entrypoint avoids the image's normal startup script.
podman run --rm -e JEKYLL_ENV=production \
  -v "$PREVIEW_DIR/src:/work:Z" -v "$PREVIEW_DIR/output:/output:Z" \
  -w /work --entrypoint sh docker.io/amirpourmand/al-folio:v0.16.2 \
  -c 'bundle check && bundle exec jekyll build --destination /output'

printf 'Preview output: %s\n' "$PREVIEW_DIR/output"
```

Confirm that the build finishes successfully and review warnings. If the image is missing, pulling it requires network access. Avoid `podman compose up` for this workflow: the Compose configuration exposes ports differently and the image's normal startup script can alter Git state.

Serve the snapshot **only on the VM's loopback interface**. The command stays in the foreground; press **Ctrl-C** in that terminal to stop it:

```bash
python3 -m http.server 8080 --bind 127.0.0.1 --directory "$PREVIEW_DIR/output"
```

If port 8080 is occupied, choose another unused port and substitute it in the tunnel and browser URL. On your **laptop**, in another terminal, connect through the SSH gateway:

```bash
ssh -N -L 8080:127.0.0.1:8080 -J <gateway-user>@<gateway-host> <vm-user>@<vm-host>
```

Replace the placeholders with your SSH details. If your SSH configuration already supplies the gateway, use `ssh -N -L 8080:127.0.0.1:8080 <vm-alias>`. Keep the tunnel open and visit **http://127.0.0.1:8080/** in your laptop browser. Stop the tunnel with Ctrl-C.

The preview is a **static snapshot**: after further edits, stop the server and repeat the snapshot/build before restarting it. Scratch storage may be automatically pruned; it is not a backup. A server left running from an earlier build continues to serve the older snapshot.

## Run checks

For the complete Python suite, create a Python environment outside the repository and install the dependencies, if not already available:

```bash
cd /home/cangi21/projects/personal-website
python3 -m venv "$HOME/.cache/personal-website-venv"
"$HOME/.cache/personal-website-venv/bin/python" -m pip install -r requirements.txt
# Optional: enables the isolated Liquid-template metadata tests.
"$HOME/.cache/personal-website-venv/bin/python" -m pip install python-liquid
```

After building as above, in the **same VM shell** where `PREVIEW_DIR` is set:

```bash
SITE_DIR="$PREVIEW_DIR/output" \
PUBLICATIONS_HTML="$PREVIEW_DIR/output/publications/index.html" \
CSS_PATH="$PREVIEW_DIR/output/assets/css/main.css" \
PYTHONDONTWRITEBYTECODE=1 \
"$HOME/.cache/personal-website-venv/bin/python" -m unittest discover -s tests -v
```

Check the final result and any skipped tests. These paths enable tests of the generated site, rendered publications and compiled CSS. In a new shell, set `PREVIEW_DIR` to the directory printed by the build. The checks do not replace visual, accessibility, privacy or legal review. CI also runs PurgeCSS, which this local build does not verify.

## Review and publishing checklist

1. Inspect `git status --short --branch`, confirm `redesign`, and preserve all existing uncommitted work.
2. Verify facts, dates, URLs, DOIs, image permissions and consent. Do not publish example content or unapproved drafts.
3. Rebuild, run checks, and preview every affected listing and detail page. Test links, images, narrow screens and keyboard navigation.
4. For highlights, run the research-area test and check the correct area. For news, verify the list entry and detail page where relevant.
5. Review the complete diff **and untracked files** before committing approved work. Do not use `bin/deploy` for a preview.
6. **Do not push, merge, dispatch deployment workflows or deploy without explicit approval.** Qualifying changes to `main`/`master` can trigger deployment, and the publication workflow can update the default branch. Confirm the GitHub settings and release process before publishing.
