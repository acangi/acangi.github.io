---
layout: page
title: Team
seo_description: "People in the Department of Machine Learning for Materials Design: current members and alumni of Attila Cangi's group at CASUS and HZDR."
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

{% comment %}
One section per category, in data order: the template never sorts, because the
data file is already ordered (current members by surname, alumni by end year).
Current members keep the portrait grid. Alumni render as one table with a row
header per person and no photos: a portrait adds nothing once someone has left,
and the table lets a reader scan names, years and destinations in one pass.
The scroll wrapper keeps the four columns legible on a phone without changing
the table's display, which would cost screen readers the row/column semantics.
{% endcomment %}
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
{% if is_alumni %}
<div class="team-table-scroll" role="region" aria-labelledby="{{ section_id }}-caption" tabindex="0">
<table class="team-alumni-table">
<caption id="{{ section_id }}-caption">Former members, most recent first.</caption>
<thead>
<tr>
<th scope="col">Name</th>
<th scope="col">Role</th>
<th scope="col">Years</th>
<th scope="col">Last known position</th>
</tr>
</thead>
<tbody>
{% else %}
<div class="team-grid">
{% endif %}
{% assign section_open = true %}
{% endunless %}

{% assign role = person.role | default: "" | strip %}
{% assign email = person.email | default: "" | strip %}
{% assign last_position = person.last_position | default: "" | strip %}

{% if is_alumni %}
{% assign period = "" %}
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

<tr class="team-alumni-row">
<th scope="row" class="team-alumni-name">{{ person.name | escape }}{% if email != "" %}<a class="team-alumni-email" href="mailto:{{ email | escape }}">{{ email | escape }}</a>{% endif %}</th>
<td class="team-alumni-role">{{ role | escape }}</td>
<td class="team-alumni-period">{{ period }}</td>
<td class="team-alumni-position">{% if last_position != "" %}{{ last_position | escape }}{% endif %}</td>
</tr>
{% else %}
{% assign photo = person.photo | default: "" | strip %}

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

{% if email != "" %}
<p class="team-card-contact"><a href="mailto:{{ email | escape }}">{{ email | escape }}</a></p>
{% endif %}
</article>
{% endif %}
{% endunless %}
{% endfor %}

{% if section_open %}
{% if is_alumni %}
</tbody>
</table>
</div>
{% else %}
</div>
{% endif %}
</section>
{% endif %}
{% endif %}
{% endfor %}
</div>
