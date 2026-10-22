<script>
  import Slide from '../lib/Slide.svelte'

  // locally, the local docs
  const docs = import.meta.env.DEV ? 'http://localhost:5190/' : 'https://bollhav.dev/docs/'
</script>

<Slide layout="title" title="bollhav">
  <img class="plain logo" src="./bollhav.png" alt="" width="200" height="165" />
  <div class="tagline">
    <p class="lead">A Python framework that standardizes pipeline models</p>
    <aside class="note">
      <svg class="arrow" viewBox="0 0 60 50" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M56 4 C 42 6, 24 20, 6 44" />
        <path d="M7 31 L6 44 L19 42" />
      </svg>
      <div class="text">
        <p>and in the process,<br />creates great metadata for</p>
        <ul>
          <li>execution</li>
          <li>state</li>
          <li>lineage</li>
        </ul>
      </div>
    </aside>
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

  /* the tagline, with a note hanging off the end of it, up and to the side,
     an arrow pointing back at the words; on a narrow screen the note sits
     below the tagline instead */
  .tagline {
    position: relative;
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

  .note {
    position: absolute;
    left: calc(100% + 0.4rem);
    bottom: 0.9rem;
    display: flex;
    align-items: flex-end;
    gap: 0.3rem;
    width: max-content;
    text-align: left;
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
    margin-bottom: 1rem;
    transform: rotate(-3deg);
  }

  .note p {
    margin: 0;
    font-size: inherit;
    max-width: none;
  }

  .note ul {
    margin: 0.3rem 0 0;
    padding: 0;
    list-style: none;
    display: grid;
    gap: 0.1rem;
  }

  .note li {
    font-size: inherit;
    padding-left: 1.1em;
    color: var(--fg);
  }

  .note li::before {
    top: 0.45em;
    width: 0.5em;
    height: 0.5em;
  }

  @media (max-width: 1100px) {
    .note {
      position: static;
      width: auto;
      margin-top: 0.9rem;
      justify-content: center;
    }
    .note .arrow {
      display: none;
    }
    .note .text {
      margin: 0;
      transform: none;
      text-align: center;
    }
    .note ul {
      display: flex;
      justify-content: center;
      gap: 1.2rem;
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
