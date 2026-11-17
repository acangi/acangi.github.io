---
layout: page
title: Software
description: Open-source software for machine learning and electronic-structure theory
permalink: /software/
# Kept as a page at /software/ but no longer shown in the navbar.
nav: false
nav_order: 4
---

Our methods are only useful if other people can run them. We develop and maintain open-source
software that brings machine learning into electronic-structure workflows, with a focus on
reproducibility, scalability on HPC systems, and documentation that lets new users get started
in minutes rather than weeks.

Both projects below are archived on [Zenodo](https://zenodo.org) so that every release is citable
with a DOI.

## MALA — Materials Learning Algorithms

MALA is a framework for scalable machine learning of electronic-structure properties, trained on
first-principles (DFT) data. Instead of solving the Kohn–Sham equations from scratch at every
molecular-dynamics step, MALA predicts the electronic structure — density of states, band
energies, total energy contributions — with a neural network, making large-scale atomistic
simulations tractable at near-DFT accuracy.

| | |
|---|---|
| **Description** | Scalable machine learning for electronic structure calculations |
| **Repository** | <https://github.com/mala-project/mala> |
| **Documentation** | <https://mala-project.github.io/mala/> |
| **Archived releases** | DOI [10.5281/zenodo.5557254](https://doi.org/10.5281/zenodo.5557254) |
| **Language / stack** | Python, PyTorch, MPI (with some Fortran components) |
| **License** | BSD-3-Clause |

### Highlights

- Trains on data produced by your own DFT code (e.g. Quantum ESPRESSO, Vienna Ab initio Simulation Package, S/PHI/nX) — no proprietary training set required.
- Scales from a laptop to leadership-class HPC systems through MPI-parallel data preprocessing and distributed training.
- Provides ready-to-use workflows for total energy, density of states, and band-structure prediction inside molecular dynamics.
- Ships as a Python package with a documented API, tutorials, and an extensive test suite.

### Installation

```bash
git clone https://github.com/mala-project/mala.git
cd mala
pip install -e .
```

### Citing MALA

If you use MALA in your work, please cite the paper and the Zenodo archive:

> A. Cangi, L. Fiedler, B. Brzoza, K. Shah, T. J. Callow, D. Kotik, S. Schmerler, M. C. Barry,
> J. M. Goff, A. Rohskopf, D. J. Vogel, N. Modine, A. P. Thompson, S. Rajamanickam,
> *Materials Learning Algorithms (MALA): Scalable machine learning for electronic structure
> calculations in large-scale atomistic simulations*,
> **Comput. Phys. Commun. 314**, 109654 (2025).
> [doi:10.1016/j.cpc.2025.109654](https://doi.org/10.1016/j.cpc.2025.109654)

```bibtex
@article{Cangi2025Materials,
  title   = {Materials Learning Algorithms (MALA): Scalable machine learning for electronic
             structure calculations in large-scale atomistic simulations},
  author  = {Cangi, Attila and Fiedler, Lenz and Brzoza, Bartosz and Shah, Karan and
             Callow, Timothy J. and Kotik, Daniel and Schmerler, Steve and Barry, Matthew C. and
             Goff, James and Rohskopf, Andrew and Vogel, Dayton J. and Modine, Normand A. and
             Thompson, Aidan P. and Rajamanickam, Sivasankaran},
  journal = {Computer Physics Communications},
  volume  = {314},
  pages   = {109654},
  year    = {2025},
  doi     = {10.1016/j.cpc.2025.109654}
}
```

## atoMEC — average-atom modelling for warm dense matter

atoMEC is a Python package that solves the average-atom model — a density-functional description
of a single atom embedded in a plasma — to obtain equation-of-state, electronic-structure, and
ionization properties of warm dense matter. It is designed as a readable, extensible reference
implementation: the physics is documented, the numerical choices are explicit, and every model
input can be inspected and modified.

| | |
|---|---|
| **Description** | Average-atom Python code for warm dense matter |
| **Repository** | <https://github.com/atomec-project/atoMEC> |
| **Documentation** | <https://atomec-project.github.io/atoMEC> |
| **Archived releases** | DOI [10.5281/zenodo.5205718](https://doi.org/10.5281/zenodo.5205718) |
| **Language / stack** | Python (NumPy, SciPy) |
| **License** | Open source — see the repository for the current license |

### Highlights

- Computes warm-dense-matter properties — ionization states, electron densities, equations of state — from a self-consistent average-atom model.
- Supports multiple exchange–correlation functionals and bound-state boundary conditions, so models can be compared on equal footing.
- Pure Python, easy to install, and simple to extend: suitable for teaching as well as production calculations.

### Installation

```bash
pip install atoMEC
```

### Citing atoMEC

> T. J. Callow, D. Kotik, E. Kraisler, A. Cangi, *atoMEC: An open-source average-atom Python
> code*, Proceedings of the 21st Python in Science Conference (SciPy 2022).
> [doi:10.25080/majora-212e5952-006](https://doi.org/10.25080/majora-212e5952-006)
> Archive DOI: [10.5281/zenodo.5205718](https://doi.org/10.5281/zenodo.5205718)

## Contributing

We welcome contributions — bug reports, documentation fixes, and new features. Both projects use
GitHub issues and pull requests; please open an issue before starting on a larger change so we can
agree on the design. We are also happy to support new users getting started; see
[Join Us]({{ '/join/' | relative_url }}) or write to [a.cangi@hzdr.de](mailto:a.cangi@hzdr.de).
