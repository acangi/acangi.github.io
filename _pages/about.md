---
layout: about
title: About
permalink: /
subtitle:

#profile:
#  align: right
#  image: attila_cangi.webp
#  image_circular: false # crops the image to make it circular
#  more_info: >
#    <p>Center for Advanced Systems Understanding</p>
#    <p>Helmholtz-Zentrum Dresden-Rossendorf</p> 
#    <p>Conrad-Schiedt-Straße 20</p>
#    <p>02826 Görlitz</p>

selected_papers: true # includes a list of papers marked as "selected={true}"
social: true # includes social icons at the bottom of the page

announcements:
  enabled: true # includes a list of news items from the _news/ collection
  scrollable: true # adds a vertical scroll bar if there are more than 3 news items
  limit: 5 # leave blank to include all the news in the `_news` folder

# latest_posts shows entries from `site.posts` (the blog). The blog is currently
# empty and has no index page, so this section is left disabled; enable it once
# posts exist in _posts/:
# latest_posts:
#   enabled: true
#   scrollable: true # adds a vertical scroll bar if there are more than 3 new posts items
#   limit: 3 # leave blank to include all the blog posts
---

Attila Cangi is the head of the <a href="https://www.casus.science/?page_id=4660" target="_blank">Department of Machine Learning for Materials Design</a> at the <a href="https://www.casus.science" target="_blank">Center for Advanced Systems Understanding</a>, <a href="https://www.hzdr.de" target="_blank">Helmholtz-Zentrum Dresden-Rossendorf</a>.

<style>
  /* Homepage at-a-glance metrics line (kept local to this page on purpose) */
  .home-metrics {
    margin: 1.25rem 0;
    padding: 0.7rem 0;
    border-top: 1px solid var(--global-divider-color);
    border-bottom: 1px solid var(--global-divider-color);
    font-size: 0.95rem;
    color: var(--global-text-color-light);
  }
</style>

<p class="home-metrics">56+ publications · 12 postdocs supervised · R&amp;D 100 Award</p>

His research group combines quantum-mechanical theory, physics-informed machine learning, and high-performance computing to understand, predict, and design materials and molecular systems. Their work spans electronic structure, chemical representation learning, quantum dynamics, magnetic materials, and THz-driven matter.

[Explore our research →]({{ '/research/' | relative_url }})

He received his Ph.D. in Chemistry from the University of California, Irvine, and previously worked as a postdoctoral fellow at the Max Planck Institute of Microstructure Physics and as a staff member at Sandia National Laboratories.

Information on the **research team** is on the [Team]({{ '/team/' | relative_url }}) page, **recent news** on the [News]({{ '/news/' | relative_url }}) page, and the open-source software we develop on the [Software]({{ '/software/' | relative_url }}) page. Institutional profiles are listed on the <a href="https://www.casus.science/?page_id=4660" target="_blank">CASUS department website</a>.

{% include latent_dynamics.liquid %}
