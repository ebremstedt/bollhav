<script>
  import Slide from '../lib/Slide.svelte'
</script>

<Slide layout="title" title="bollhav">
  <img class="plain logo" src="./bollhav.png" alt="" width="200" height="165" />
  <p class="lead">A Python framework that standardizes pipeline code</p>
  <p><code>pip install bollhav</code></p>
  <p class="start">
    <span class="keys"><kbd>→</kbd> press the right arrow to start</span>
    <span class="touch">swipe left to start</span>
  </p>
  <p class="links">
    <a href="https://bollhav.dev/docs/">Docs</a>
    <a href="https://bollhav.dev/learn/">Learn</a>
    <a href="https://bollhav.dev/lab/">Lab</a>
    <a href="https://github.com/ebremstedt/bollhav">GitHub</a>
  </p>
</Slide>

<style>
  .logo {
    width: clamp(120px, 22vw, 220px);
    order: -1; /* above the heading */
  }

  .start {
    margin-top: 1.5rem;
    font-family: var(--font-mono);
    font-size: 0.85rem;
    letter-spacing: 0.04em;
    color: var(--fg-2);
    white-space: nowrap;
  }

  .links {
    display: flex;
    gap: 1.4rem;
    justify-content: center;
    font-size: 0.95rem;
    font-weight: 600;
  }

  .links a {
    color: var(--fg-2);
    text-decoration: none;
  }

  .links a:hover {
    color: var(--accent);
  }

  /* a touch screen has no arrow keys */
  .touch {
    display: none;
  }
  @media (hover: none) and (pointer: coarse) {
    .keys {
      display: none;
    }
    .touch {
      display: inline;
    }
  }

  .start kbd {
    display: inline-block;
    margin-right: 0.4em;
    animation: nudge 1.6s ease-in-out infinite;
  }

  @keyframes nudge {
    0%,
    100% {
      transform: translateX(0);
    }
    50% {
      transform: translateX(4px);
    }
  }
</style>
