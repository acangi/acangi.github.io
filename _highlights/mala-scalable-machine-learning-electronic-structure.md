---
layout: highlight
title: "Materials Learning Algorithms (MALA): Scalable Machine Learning for Electronic Structure Calculations in Large-Scale Atomistic Simulations"
short_title: "MALA: scalable ML for electronic structure"
citation: "Comput. Phys. Commun. 314, 109654 (2025)"
journal: "Computer Physics Communications"
year: 2025
date: 2025-05-01
order: 1
research_area: electronic-structure
doi: "10.1016/j.cpc.2025.109654"
summary: >-
  MALA is a scalable machine-learning framework that reproduces key density-functional-theory
  outputs for large atomistic systems, from local densities of states to total energies.
---

MALA learns the mapping from local atomic environments to electronic observables, so a simulation can obtain densities of states, electron densities and total energies without solving the Kohn–Sham problem at every step. The package covers the whole workflow — sampling training data, fitting models and running inference in parallel — and is built to interoperate with standard DFT and molecular-dynamics codes rather than replace them.

The paper demonstrates the framework on boron clusters, on aluminum across its solid–liquid phase boundary, and on the electronic structure of a stacking fault in a large beryllium slab. Scaling tests document the speedup achieved and expose the remaining bottlenecks, giving a realistic picture of when learned electronic structure becomes the cheaper option. The overall aim is to push electronic-structure accuracy into system sizes that conventional DFT cannot reach.
