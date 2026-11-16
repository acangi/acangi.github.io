---
layout: page
permalink: /publications/
title: Publications
description: 
nav: true
nav_order: 4
---

<!-- _pages/publications.md -->

<!-- Bibsearch Feature -->

{% include bib_search.liquid %}

<div class="publications">

{% comment %}
  Show every publication type: articles, book chapters, conference
  proceedings (inproceedings) and reports (techreport). A plain
  `@*[publication_page=true]` query silently dropped the proceedings
  and report entries.
{% endcomment %}
{% bibliography --query @*[publication_page=true] || @inproceedings || @techreport || @incollection %}

</div>
