<p align="center">
  <img src="bollhav_logo_large.png" alt="bollhav" width="300">
</p>

<p align="center"><strong>Bollhav</strong><br>a Python framework that standardizes pipeline code</p>

A **model** is a pure data object that declares what your data looks like and
where it goes, and it ✨deliberately✨ contains no execution logic. The actual
work lives in a separate **execute** function that takes the model as a
parameter.

Orchestrate the models with a classical tool like **Airflow**, or use the
built-in choreography in **bollhav state**.

```bash
pip install bollhav
```
