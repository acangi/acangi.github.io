---
layout: highlight
title: "Inverting the Kohn–Sham Equations with Physics-Informed Machine Learning"
short_title: "Inverting the Kohn–Sham equations"
citation: "Mach. Learn. Sci. Technol. 5, 015050 (2024)"
journal: "Machine Learning: Science and Technology"
year: 2024
date: 2024-03-19
order: 4
research_area: quantum-dynamics
doi: "10.1088/2632-2153/ad3159"
summary: >-
  Physics-informed neural networks and Fourier neural operators are trained to recover
  exchange–correlation potentials from electron densities, with the combination of both methods
  emerging as the most promising route to large-scale inversion.
---

Kohn–Sham DFT maps interacting electrons onto non-interacting particles that share the same density. The exchange–correlation potential is what keeps the two densities equal, and obtaining that potential directly from a target density — the inverse problem — is a route to understanding which approximations work and why. Neural networks offer a way to learn the density-to-potential mapping.

The authors compare two physics-informed strategies: physics-informed neural networks and Fourier neural operators. They test both on a dataset of one-dimensional atomic and molecular models, discuss the strengths of each, and treat the study as a proof of concept. Their main conclusion is that combining the approaches has the greatest potential for inverting the Kohn–Sham equations at scale.
