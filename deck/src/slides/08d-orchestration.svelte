<script>
  // The text of each column is Markdown, next to this file: the heading, then
  // one line that becomes the small label under it, then the paragraphs.
  import Slide from '../lib/Slide.svelte'
  import Markdown from '../lib/Markdown.svelte'
  import orchestration from './08d-orchestration.md?raw'
  import choreography from './08d-choreography.md?raw'
</script>

<Slide kicker="Running the models" title="Orchestration vs Choreography">
  <div class="cols">
    <div class="half">
      <img class="plain pic" src="./conductor.png" alt="" width="512" height="512" />
      <div class="text"><Markdown source={orchestration} /></div>
    </div>
    <div class="half">
      <div class="text"><Markdown source={choreography} /></div>
      <img class="plain pic" src="./dancer.png" alt="" width="512" height="512" />
    </div>
  </div>
  <p class="credit">
    <span class="attrib" tabindex="0">
      image attribution
      <span class="card" role="tooltip">
        <a href="https://www.flaticon.com/free-icons/conductor" title="conductor icons">Conductor icons created by mia elysia - Flaticon</a>
        <a href="https://www.flaticon.com/free-icons/ballet" title="ballet icons">Ballet icons created by NeXore88 - Flaticon</a>
      </span>
    </span>
  </p>
</Slide>

<style>
  /* a conductor and the orchestra beside the one, a dancer beside the other */
  .half {
    display: flex;
    align-items: flex-start;
    gap: 1.25rem;
    min-width: 0;
  }

  /* the pictures are black line art on transparent: on the dark theme they
     are inverted, so the lines come out light */
  .pic {
    flex: none;
    width: clamp(6.5rem, 13vw, 11rem);
    height: auto;
    margin-top: 0.3rem;
  }

  :global([data-theme='dark']) .pic {
    filter: invert(1);
  }

  @media (prefers-color-scheme: dark) {
    :global(:root:not([data-theme='light'])) .pic {
      filter: invert(1);
    }
  }

  /* the line right under the heading is the small label */
  .text :global(h3 + p) {
    font-family: var(--font-mono);
    font-size: 0.85rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-2);
    margin-bottom: 0.6rem;
  }

  /* where the pictures come from: two words, and the links in a card on hover */
  .credit {
    margin-top: 0.5rem;
    max-width: none;
    font-family: var(--font-mono);
    font-size: 0.68rem;
    letter-spacing: 0.04em;
    color: var(--fg-2);
  }

  .attrib {
    position: relative;
    display: inline-block;
    cursor: help;
    outline: none;
    text-decoration: underline dotted;
    text-decoration-thickness: 1.5px;
    text-underline-offset: 0.2em;
  }

  .attrib:hover,
  .attrib:focus-visible {
    color: var(--accent);
  }

  .card {
    position: absolute;
    left: 0;
    bottom: calc(100% + 0.6em);
    display: grid;
    gap: 0.4em;
    width: max-content;
    padding: 0.7em 0.9em;
    border: 1px solid var(--line);
    border-radius: 0.7em;
    background: var(--bg-2);
    box-shadow: 0 18px 40px -20px rgba(10, 20, 44, 0.7);
    font-size: 0.72rem;
    letter-spacing: 0.02em;
    color: var(--fg);
    opacity: 0;
    visibility: hidden;
    transform: translateY(4px);
    transition:
      opacity 0.2s,
      transform 0.2s,
      visibility 0.2s;
  }

  .attrib:hover .card,
  .attrib:focus-visible .card,
  .attrib:focus-within .card {
    opacity: 1;
    visibility: visible;
    transform: none;
  }

  .card a {
    color: var(--fg);
    text-decoration: none;
  }

  .card a:hover {
    color: var(--accent);
  }

  @media (max-width: 760px) {
    .half {
      flex-direction: column;
    }
    .half:last-child {
      flex-direction: column-reverse;
    }
  }
</style>
