---
layout: page
title: Team
description: People in the Department of Machine Learning for Materials Design
permalink: /team/
nav: true
nav_order: 5
head:
  name: "Attila Cangi, Ph.D."
  email: "a.cangi@hzdr.de"
  photo: "/assets/img/team/Attila_Cangi.jpg"
  cv: "/cv/"
  orcid: "https://orcid.org/0000-0001-9162-262X"
  # Alternate spellings used only to keep the head out of the data-driven list.
  aliases:
    - "Attila Cangi"
    - "Prof. Dr. Attila Cangi"
---

<div class="team-page">
<p class="team-intro">
Our department at <a href="https://www.casus.science">CASUS</a> and
<a href="https://www.hzdr.de">HZDR</a> works at the intersection of quantum
theory, machine learning, and scientific computing.
</p>

{% assign head_email = page.head.email | default: "" | downcase | strip %}
{% assign head_name = page.head.name | default: "" | downcase | strip %}

<section class="team-section team-section--head" aria-labelledby="team-head-heading">
<h2 class="team-section-title" id="team-head-heading">Head of Department</h2>
<div class="team-grid">
<article class="team-card team-card--head">
<div class="team-card-head">
{% if page.head.photo %}
<img class="team-card-photo" src="{{ page.head.photo | relative_url }}" alt="Portrait of {{ page.head.name | escape }}" width="96" height="96" loading="lazy" decoding="async">
{% else %}
<img class="team-card-photo team-card-photo--placeholder" src="{{ '/assets/img/team/avatar-neutral.svg' | relative_url }}" alt="" width="96" height="96" loading="lazy" decoding="async">
{% endif %}
<div class="team-card-identity">
<h3 class="team-card-name">{{ page.head.name | escape }}</h3>
</div>
</div>
<p class="team-card-links">
<a href="{{ page.head.cv | relative_url }}">CV</a>
<span aria-hidden="true">·</span>
<a href="{{ page.head.orcid }}">ORCID</a>
{% if page.head.email %}
<span aria-hidden="true">·</span>
<a href="mailto:{{ page.head.email | escape }}">{{ page.head.email | escape }}</a>
{% endif %}
</p>
</article>
</div>
</section>

{% comment %}
Count the entries that are not the head first, so an all-duplicate or empty
data file produces one neutral message instead of empty category headings.
{% endcomment %}
{% assign total_members = 0 %}
{% for category in site.data.team %}
{% if category.members %}
{% for person in category.members %}
{% assign member_name = person.name | default: "" | downcase | strip %}
{% assign member_email = person.email | default: "" | downcase | strip %}
{% assign is_head = false %}
{% if head_email != "" and member_email == head_email %}
{% assign is_head = true %}
{% endif %}
{% if head_name != "" and member_name == head_name %}
{% assign is_head = true %}
{% endif %}
{% for alias in page.head.aliases %}
{% assign alias_name = alias | downcase | strip %}
{% if alias_name != "" and member_name == alias_name %}
{% assign is_head = true %}
{% endif %}
{% endfor %}
{% unless is_head %}
{% assign total_members = total_members | plus: 1 %}
{% endunless %}
{% endfor %}
{% endif %}
{% endfor %}

{% if total_members == 0 %}
<p class="team-empty">No team members are listed at present.</p>
{% endif %}

{% for category in site.data.team %}
{% assign category_title = category.title | default: "Team" | strip %}
{% assign section_id = "team-section-" | append: forloop.index %}
{% assign is_alumni = false %}
{% if category_title == "Alumni" %}
{% assign is_alumni = true %}
{% endif %}
{% assign section_open = false %}

{% if category.members %}
{% for person in category.members %}
{% assign member_name = person.name | default: "" | downcase | strip %}
{% assign member_email = person.email | default: "" | downcase | strip %}
{% assign is_head = false %}
{% if head_email != "" and member_email == head_email %}
{% assign is_head = true %}
{% endif %}
{% if head_name != "" and member_name == head_name %}
{% assign is_head = true %}
{% endif %}
{% for alias in page.head.aliases %}
{% assign alias_name = alias | downcase | strip %}
{% if alias_name != "" and member_name == alias_name %}
{% assign is_head = true %}
{% endif %}
{% endfor %}

{% unless is_head %}
{% unless section_open %}
<section class="team-section" aria-labelledby="{{ section_id }}">
<h2 class="team-section-title" id="{{ section_id }}">{{ category_title | escape }}</h2>
<div class="team-grid">
{% assign section_open = true %}
{% endunless %}

{% assign photo = person.photo | default: "" | strip %}
{% assign role = person.role | default: "" | strip %}
{% assign email = person.email | default: "" | strip %}
{% assign last_position = person.last_position | default: "" | strip %}
{% assign period = "" %}
{% if is_alumni %}
{% assign start_year = person.start_year | default: "" | append: "" | strip %}
{% assign end_year = person.end_year | default: "" | append: "" | strip %}
{% if start_year != "" and end_year != "" %}
{% if start_year == end_year %}
{% assign period = start_year %}
{% else %}
{% assign period = start_year | append: "–" | append: end_year %}
{% endif %}
{% elsif start_year != "" %}
{% assign period = start_year %}
{% elsif end_year != "" %}
{% assign period = end_year %}
{% endif %}
{% endif %}

<article class="team-card">
<div class="team-card-head">
{% if photo != "" %}
<img class="team-card-photo" src="{{ photo | relative_url }}" alt="Portrait of {{ person.name | escape }}" width="96" height="96" loading="lazy" decoding="async">
{% else %}
<img class="team-card-photo team-card-photo--placeholder" src="{{ '/assets/img/team/avatar-neutral.svg' | relative_url }}" alt="" width="96" height="96" loading="lazy" decoding="async">
{% endif %}
<div class="team-card-identity">
<h3 class="team-card-name">{{ person.name | escape }}</h3>
{% if role != "" %}
<p class="team-card-role">{{ role | escape }}</p>
{% endif %}
</div>
</div>

{% if is_alumni and period != "" %}
<p class="team-card-period">{{ period }}</p>
{% endif %}

{% if is_alumni and last_position != "" %}
<p class="team-card-position"><span class="team-card-label">Last known position:</span> {{ last_position | escape }}</p>
{% endif %}

{% if email != "" %}
<p class="team-card-contact"><a href="mailto:{{ email | escape }}">{{ email | escape }}</a></p>
{% endif %}
</article>
{% endunless %}
{% endfor %}

{% if section_open %}
</div>
</section>
{% endif %}
{% endif %}
{% endfor %}
</div>
