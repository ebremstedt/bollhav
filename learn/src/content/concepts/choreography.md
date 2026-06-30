---
title: "Choreography 💃"
body: "Decentralized model ordering"
---

In a traditional Airflow setup, an external scheduler holds the whole dependency graph and decides what runs after what. Choreography flips that: with state and upstream contracts, the ordering lives inside the models themselves. A downstream interval simply stays blocked until its upstream's covering window is applied — so all you need on the outside is a dumb trigger, like a cron running `python main.py` every few minutes. Each run works out what's actionable, does it, and leaves the rest blocked for next time. There's no central DAG to maintain that just duplicates the dependencies your models already declare.
