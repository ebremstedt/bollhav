<script>
  import Slide from '../lib/Slide.svelte'
</script>

<Slide layout="title" title="bollhav">
  <img class="plain logo" src="./bollhav.png" alt="" width="200" height="165" />
  <p class="lead">A Python framework that standardizes pipeline code</p>
  <p><code>pip install bollhav</code></p>
  <p class="start"><kbd>→</kbd> press the right arrow to start</p>
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
