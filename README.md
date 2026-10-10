<p align="center">
  <img src="https://raw.githubusercontent.com/ebremstedt/bollhav/main/docs/content/bollhav_logo_large.png" alt="bollhav" width="300">
</p>

<p align="center">
  <strong>Bollhav</strong><br>
  A Python framework that standardizes pipeline code
</p>

<p align="center">
  <a href="https://pypi.org/project/bollhav/"><img src="https://img.shields.io/pypi/v/bollhav" alt="PyPI version"></a>
  <a href="https://pypi.org/project/bollhav/"><img src="https://img.shields.io/pypi/pyversions/bollhav" alt="Python versions"></a>
  <a href="https://github.com/ebremstedt/bollhav/blob/main/LICENSE"><img src="https://img.shields.io/pypi/l/bollhav" alt="License"></a>
</p>

<p align="center">
  <a href="https://bollhav.dev">Docs</a> ·
  <a href="https://learn.bollhav.dev">Learn</a> ·
  <a href="https://lab.bollhav.dev">Lab</a>
</p>

The idea is a clean separation: a **Model** is a pure data object that declares
what your data looks like and where it goes, and it ✨deliberately✨ contains
**no execution logic**. The actual work lives in a separate **execute** function
that takes the model as a parameter.

Orchestrate the models with a classical tool like **Airflow**, or use the
built-in choreography in **bollhav state**.

```bash
pip install bollhav
```

# Demo

<p align="center">
  <img src="https://raw.githubusercontent.com/ebremstedt/bollhav/main/docs/content/batch_recording.gif" alt="bollhav running a batch">
</p>
