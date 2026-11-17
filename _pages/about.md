---
layout: about
title: About
permalink: /

selected_papers: false # the homepage no longer lists selected papers; /publications/ has them all
social: true # includes social icons at the bottom of the page

announcements:
  enabled: true # includes a list of news items from the _news/ collection
  scrollable: false # show every published item; drafts stay unpublished
  limit: 4 # leave blank to include all the news in the `_news` folder

# latest_posts shows entries from `site.posts` (the blog). The blog is currently
# empty and has no index page, so this section is left disabled; enable it once
# posts exist in _posts/:
# latest_posts:
#   enabled: true
#   scrollable: true # adds a vertical scroll bar if there are more than 3 new posts items
#   limit: 3 # leave blank to include all the blog posts
---

<div class="nh-home" id="nordic-home">

  <style>
    /* Homepage research-area grid: one entry per research area, each an icon
       and the title, linking to /research/#anchor. Scoped to
       #nordic-home so it cannot leak into /research/, which keeps its own
       .research-areas layout. Lives here because it is a single-section
       component of this page. */
    #nordic-home .nh-area-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: clamp(0.75rem, 1.3vw, 1.1rem);
      margin: 0;
      padding: 0;
      list-style: none;
    }

    #nordic-home .nh-area-card {
      position: relative;
      display: flex;
      flex-direction: column;
      gap: 0.6rem;
      height: 100%;
      padding: clamp(1.1rem, 1.6vw, 1.45rem);
      border: 1px solid transparent;
      border-radius: 2px;
      background: var(--nh-card-1);
      color: var(--nordic-ink);
      text-decoration: none;
      transition:
        transform 0.4s ease,
        box-shadow 0.4s ease,
        border-color 0.4s ease;
    }

    #nordic-home .nh-area-grid > li:nth-child(3n + 2) .nh-area-card {
      background: var(--nh-card-2);
    }

    #nordic-home .nh-area-grid > li:nth-child(3n) .nh-area-card {
      background: var(--nh-card-3);
    }

    #nordic-home .nh-area-card::before {
      content: "";
      position: absolute;
      top: 0;
      left: clamp(1.1rem, 1.6vw, 1.45rem);
      width: 28px;
      height: 1px;
      background: var(--nordic-sage-deep);
      opacity: 0.55;
      transition:
        width 0.45s ease,
        opacity 0.45s ease;
    }

    #nordic-home .nh-area-card:hover,
    #nordic-home .nh-area-card:focus-visible {
      transform: translateY(-3px);
      border-color: var(--nordic-rule);
      box-shadow: var(--nordic-shadow);
      text-decoration: none;
    }

    #nordic-home .nh-area-card:hover::before,
    #nordic-home .nh-area-card:focus-visible::before {
      width: calc(100% - 2 * clamp(1.1rem, 1.6vw, 1.45rem));
      opacity: 0.9;
    }

    #nordic-home .nh-area-card:focus-visible {
      outline: 2px solid var(--nordic-sage-deep);
      outline-offset: 3px;
    }

    #nordic-home .nh-area-icon {
      display: block;
      flex: 0 0 auto;
      width: 3.25rem;
      height: 3.25rem;
      padding: 0.45rem;
      border-radius: 0.8rem;
      /* Fixed light plate: the icons are dark line art and must stay legible
         in both themes, as on /research/. */
      background: #f4f8fa;
    }

    #nordic-home .nh-area-icon img {
      display: block;
      width: 100%;
      height: 100%;
      object-fit: contain;
    }

    #nordic-home .nh-area-title {
      margin-top: auto;
      font-family: "DM Serif Display", "Iowan Old Style", Georgia, "Times New Roman", serif;
      font-size: clamp(1.05rem, 1.3vw, 1.2rem);
      font-weight: 400;
      line-height: 1.3;
      letter-spacing: -0.005em;
      text-wrap: balance;
      color: var(--nordic-ink);
    }

    /* The visualization keeps its figure and tooltip but no heading, so the
       section rule and a little breathing room above it are all the separation
       it needs. */
    #nordic-home .nh-viz-section {
      margin-top: clamp(2.4rem, 4.5vw, 3.6rem);
      padding-top: clamp(1.6rem, 2.4vw, 2.2rem);
      border-top: 1px solid var(--nordic-rule-strong);
    }

    /* The hero leads with the department name, which is longer than a person's
       name, so its display size is smaller than the retired .nh-name scale. The
       former monogram column now carries the head's name and affiliation. */
    #nordic-home .nh-hero-title {
      max-width: 26ch;
      margin: 0 0 0 -0.02em;
      font-family: "DM Serif Display", "Iowan Old Style", Georgia, "Times New Roman", serif;
      font-size: clamp(2.15rem, 4.6vw, 3.55rem);
      font-weight: 400;
      line-height: 1.06;
      letter-spacing: -0.018em;
      text-wrap: balance;
      color: var(--nordic-ink);
    }

    @media (max-width: 1099.98px) {
      #nordic-home .nh-area-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
    }

    @media (max-width: 719.98px) {
      #nordic-home .nh-area-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 479.98px) {
      #nordic-home .nh-area-grid {
        grid-template-columns: 1fr;
      }
    }

    @media (prefers-reduced-motion: reduce) {
      #nordic-home .nh-area-card,
      #nordic-home .nh-area-card::before {
        transition: none !important;
      }

      #nordic-home .nh-area-card:hover {
        transform: none;
      }
    }
  </style>

  <section class="nh-hero" aria-labelledby="nh-hero-title">
    <p class="nh-eyebrow">CASUS — Helmholtz-Zentrum Dresden-Rossendorf</p>

    <div class="nh-hero-grid">
      <div class="nh-hero-lead">
        <h1 class="nh-hero-title" id="nh-hero-title">Machine Learning for Materials Design</h1>
      </div>

      <div class="nh-role">
        <strong>Attila Cangi</strong>
        <p>
          Head of Department<br>
          <a href="https://www.casus.science" target="_blank" rel="noopener">Center for Advanced Systems Understanding</a>
          ·
          <a href="https://www.hzdr.de" target="_blank" rel="noopener">Helmholtz-Zentrum Dresden-Rossendorf</a><br>
          Görlitz, Germany
        </p>
      </div>
    </div>
  </section>

  <section class="nh-section" id="research" aria-labelledby="nh-research">
    <div class="nh-sec-head">
      <h2 id="nh-research">Research</h2>
      <span class="nh-sec-note"><a href="{{ '/research/' | relative_url }}">All research areas →</a> · <a href="{{ '/highlights/' | relative_url }}">Research highlights →</a></span>
    </div>

    <ol class="nh-area-grid">
      <li>
        <a class="nh-area-card" href="{{ '/research/' | relative_url }}#electronic-structure">
          <span class="nh-area-icon">
            <img src="{{ '/assets/img/research/01-electronic-structure.png' | relative_url }}" alt="" width="1024" height="1024" loading="lazy" decoding="async">
          </span>
          <span class="nh-area-title">Scalable Machine Learning for Electronic-Structure Theory</span>
        </a>
      </li>
      <li>
        <a class="nh-area-card" href="{{ '/research/' | relative_url }}#representation-learning">
          <span class="nh-area-icon">
            <img src="{{ '/assets/img/research/02-representation-learning.png' | relative_url }}" alt="" width="1024" height="1024" loading="lazy" decoding="async">
          </span>
          <span class="nh-area-title">Representation Learning for Chemical Space and Materials</span>
        </a>
      </li>
      <li>
        <a class="nh-area-card" href="{{ '/research/' | relative_url }}#quantum-dynamics">
          <span class="nh-area-icon">
            <img src="{{ '/assets/img/research/03-quantum-dynamics-transport.png' | relative_url }}" alt="" width="1024" height="1024" loading="lazy" decoding="async">
          </span>
          <span class="nh-area-title">Quantum Dynamics and Electronic Transport</span>
        </a>
      </li>
      <li>
        <a class="nh-area-card" href="{{ '/research/' | relative_url }}#magnetic-materials">
          <span class="nh-area-icon">
            <img src="{{ '/assets/img/research/04-magnetic-materials-spin-lattice.png' | relative_url }}" alt="" width="1024" height="1024" loading="lazy" decoding="async">
          </span>
          <span class="nh-area-title">Magnetic Materials and Spin-Lattice Dynamics</span>
        </a>
      </li>
      <li>
        <a class="nh-area-card" href="{{ '/research/' | relative_url }}#thz-driven-systems">
          <span class="nh-area-icon">
            <img src="{{ '/assets/img/research/05-thz-driven-systems.png' | relative_url }}" alt="" width="1024" height="1024" loading="lazy" decoding="async">
          </span>
          <span class="nh-area-title">THz-Driven Materials and Molecular Systems</span>
        </a>
      </li>
    </ol>
  </section>

  <section class="nh-section nh-viz-section" id="dynamics">
    <div class="nh-viz">
      {% include latent_dynamics.liquid %}
    </div>
  </section>

</div>
