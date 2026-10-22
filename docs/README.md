<p align="center">
  <img src="bollhav_logo_large.png" alt="bollhav" width="300">
</p>

<div class="tagline">
<p align="center"><strong>Bollhav</strong><br>a framework that standardizes pipeline models</p>
<span class="note">
  <svg class="arrow" viewBox="0 0 60 50" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <path d="M56 4 C 42 6, 24 20, 6 44" />
    <path d="M7 31 L6 44 L19 42" />
  </svg>
  <span class="text">
    and in the process,<br>creates great metadata for
    <span class="pros"><span>execution</span><span>state</span><span>lineage</span></span>
  </span>
</span>
</div>

A **model** is a pure data object that declares what your data looks like and
where it goes, and it ✨deliberately✨ contains no execution logic. The actual
work lives in a separate **execute** function that takes the model as a
parameter.

<div class="tagline">
<p align="center">Orchestrate the models with a classical tool like <strong>Airflow</strong></p>
<span class="note">
  <svg class="arrow" viewBox="0 0 60 50" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <path d="M56 4 C 42 6, 24 20, 6 44" />
    <path d="M7 31 L6 44 L19 42" />
  </svg>
  <span class="text">or use the built-in<br>choreography in <strong>bollhav</strong>.</span>
</span>
</div>

```bash
pip install bollhav
```
