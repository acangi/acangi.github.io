---
layout: page
permalink: /publications/
title: Publications
nav: true
nav_order: 3
---

<!-- _pages/publications.md -->

<!-- Bibsearch Feature -->

{% include bib_search.liquid %}

<div class="publications">

{% comment %}
  Publication-page policy: journal articles and preprints that are not yet
  published in a journal. Everything else in the bibliography - conference
  talks and abstracts, conference papers, book chapters, books, technical
  reports, theses, datasets, software, repository-only records - is tracked
  but not rendered here.

  The filter lives in the `publications_bibliography` tag
  (_plugins/publications-policy.rb), not in the query. A jekyll-scholar query
  can only match BibTeX types and fields, and this bibliography still contains
  legacy entries whose type is stale (for example a Nature Computational
  Science article stored as @misc by an older importer). Filtering before the
  entries are grouped by year also keeps empty year headings off the page,
  which a layout-only filter cannot do.

  The importer (_scripts/openalex_to_yaml.py) writes the same decision into
  `publication_page` for every entry it regenerates, so the query's `@*` and
  the tag's policy stay in step. Preprints that can only be matched to their
  journal version through an arXiv identifier are listed in
  `_data/publication_policy.yml`, which is curated by hand and never
  regenerated.
{% endcomment %}
{% publications_bibliography --query @* %}

</div>
