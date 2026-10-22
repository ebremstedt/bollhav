<script>
  import Slide from '../lib/Slide.svelte'

  import { docsUrl } from '../lib/sites.js'

  const docs = $derived(docsUrl(document.documentElement.dataset.theme))
</script>

<Slide layout="title" title="bollhav">
  <img class="plain logo" src="./bollhav.png" alt="" width="200" height="165" />
  <div class="tagline">
    <p class="lead">
      A framework that standardizes pipeline
      <span class="noted"
        >models<span class="note">
          <svg class="arrow" viewBox="0 0 60 50" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="M56 4 C 42 6, 24 20, 6 44" />
            <path d="M7 31 L6 44 L19 42" />
          </svg>
          <span class="text">
            and in the process,<br />creates great metadata for
            <span class="pros"><span>execution</span><span>state</span><span>lineage</span><span>orchestration</span></span>
          </span>
        </span></span
      >
    </p>
  </div>
  <p><code>pip install bollhav</code></p>
  <p class="start">
    <span class="keys"><kbd>→</kbd> press the right arrow to start</span>
    <span class="touch">swipe left to start</span>
  </p>
  <p class="links">
    <a href={docs}>Docs</a>
    <a href="https://github.com/ebremstedt/bollhav">GitHub</a>
  </p>
</Slide>

<style>
  .logo {
    width: clamp(120px, 22vw, 220px);
    order: -1; /* above the heading */
  }

  /* the tagline, with a note above "models": the arrow rises from the word
     up to the right, the words at its end. Hidden on a narrow screen */
  .tagline {
    width: max-content;
    max-width: 100%;
    margin-inline: auto;
  }

  @media (min-width: 1101px) {
    .tagline .lead {
      max-width: none;
      white-space: nowrap;
    }
  }

  .noted {
    position: relative;
    white-space: nowrap;
  }

  .note {
    position: absolute;
    left: 55%;
    bottom: 100%;
    display: flex;
    align-items: flex-end;
    gap: 0.3rem;
    width: max-content;
    text-align: left;
    white-space: normal;
    font-family: var(--font-mono);
    font-size: 0.78rem;
    line-height: 1.35;
    letter-spacing: 0.02em;
    color: var(--fg-2);
  }

  .note .arrow {
    flex: none;
    width: 2.6rem;
    height: auto;
    color: var(--accent);
  }

  .note .text {
    display: block;
    margin-bottom: 1rem;
    transform: rotate(-3deg);
  }

  .note .pros {
    display: grid;
    gap: 0.1rem;
    margin-top: 0.3rem;
    color: var(--fg);
  }

  .note .pros > span {
    position: relative;
    padding-left: 1.1em;
  }

  .note .pros > span::before {
    content: '';
    position: absolute;
    left: 0.15em;
    top: 0.45em;
    width: 0.4em;
    height: 0.4em;
    border-radius: 50%;
    background: var(--accent);
  }

  @media (max-width: 1100px) {
    .note {
      display: none;
    }
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
