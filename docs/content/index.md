---
hide:
  - navigation
  - toc
---

<h1 style="display:none"></h1>

<p align="center" style="margin-bottom: 0.4rem;">
  <img src="bollhav_logo_large.png" alt="bollhav" width="300" style="position: relative; left: 12px;">
</p>

<p class="hero-lead" markdown>
**Bollhav**<br>
a Python framework that standardizes pipeline code
</p>

<div class="hero-main" markdown>

The idea is a clean separation: a **Model** is a pure data object that declares
what your data looks like and where it goes, and it ✨deliberately✨ contains
**no execution logic**. The actual work lives in a separate **execute** function
that takes the model as a parameter.

<p class="hero-orch">
  Orchestrate the models with a classical tool like <strong>Airflow</strong>, or
  use the built-in orchestration in <strong>bollhav state</strong>.
</p>

</div>

<div class="install" markdown>

```bash
pip install bollhav
```

</div>
