---
layout: page
title: Research Highlights
seo_description: Research outputs from the department, each linked to its publication.
permalink: /highlights/
---

{% assign highlights = site.highlights | sort: 'order' %}

<div class="highlight-list">
{% for h in highlights %}
  <article class="highlight-card">
    <p class="highlight-card-meta">{{ h.citation }}</p>
    <h2 class="highlight-card-title">
      <a href="{{ h.url | relative_url }}">{{ h.title }}</a>
    </h2>
    <p class="highlight-card-summary">{{ h.summary }}</p>
    <p class="highlight-card-links">
      <a href="https://doi.org/{{ h.doi }}">DOI {{ h.doi }}</a>
    </p>
  </article>
{% endfor %}
</div>

<p class="highlight-back-link"><a href="{{ '/research/' | relative_url }}">← Research areas</a></p>
