---
layout: highlight
title: "Data-Driven Magneto-Elastic Predictions with Scalable Classical Spin-Lattice Dynamics"
short_title: "Magneto-elastic spin-lattice dynamics"
citation: "npj Comput. Mater. 7, 153 (2021)"
journal: "npj Computational Materials"
year: 2021
date: 2021-09-28
order: 7
research_area: magnetic-materials
doi: "10.1038/s41524-021-00617-2"
summary: >-
  Magneto-elastic machine-learning interatomic potentials couple atomic spins to ionic motion,
  enabling large-scale spin-lattice dynamics and quantitative predictions across iron’s magnetic
  transition.
---

Magnetic moments are usually absent from machine-learned interatomic potentials, which limits their use for materials where magnetic order changes the physics. The authors combine two learned components: a collective atomic spin model and a standard ML interatomic potential. Together they define a potential energy surface that yields both the forces on the ions and the torques that drive spin precession, with both parts fitted to first-principles data.

The demonstration is a magneto-elastic potential for α-iron taken through the ferromagnetic–paramagnetic transition. Bulk modulus, magnetization and specific heat agree quantitatively with first-principles magneto-elastic calculations, showing that the coupled model captures the magnetostructural behavior rather than only the structural part. The framework is built for large-scale spin-lattice dynamics, where explicit magnetism has previously been too expensive to include.
