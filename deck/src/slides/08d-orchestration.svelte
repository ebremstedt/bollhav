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
    <a href="https://www.flaticon.com/free-icons/conductor" title="conductor icons">Conductor icon by mia elysia</a>
    <span class="sep">·</span>
    <a href="https://www.flaticon.com/free-icons/ballet" title="ballet icons">Ballet icon by NeXore88</a>
    <span class="sep">·</span>
    Flaticon
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

  /* where the pictures come from */
  .credit {
    margin-top: 0.5rem;
    max-width: none;
    white-space: nowrap;
    font-family: var(--font-mono);
    font-size: 0.68rem;
    letter-spacing: 0.04em;
    color: var(--fg-2);
  }

  .credit a {
    color: inherit;
    text-decoration: none;
    border-bottom: 1px solid color-mix(in srgb, var(--fg-2) 40%, transparent);
  }

  .credit a:hover {
    color: var(--accent);
    border-color: var(--accent);
  }

  .credit .sep {
    opacity: 0.45;
    margin-inline: 0.3em;
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
