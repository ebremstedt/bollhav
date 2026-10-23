<script>
  import Slide from '../lib/Slide.svelte'

  // the name, split into the segments the annotations point at
  const parts = ['warehouse', '.', 'erp', '_', 'raw', '_', 'companyb', '.', 'orders', '_', 'latest']
  const isPart = (p) => p !== '_' && p !== '.'

  // every tag the name above produces, plus the default `all`, as a word
  // cloud: rows of words, biggest in the middle. Size is just for the look;
  // `dim` marks the composite names, `hi` the default tag.
  const cloud = [
    [
      { t: 'erp_raw_companyb', s: 0.95, dim: true },
      { t: 'orders_latest', s: 0.95, dim: true },
    ],
    [
      { t: 'warehouse', s: 1.35 },
      { t: 'erp', s: 1.5 },
      { t: 'raw', s: 1.35 },
    ],
    [{ t: 'all', s: 2.3, hi: true }],
    [
      { t: 'orders', s: 1.6 },
      { t: 'companyb', s: 1.4 },
      { t: 'latest', s: 1.3 },
    ],
    [{ t: 'erp_raw_companyb.orders_latest', s: 0.85, dim: true }],
    [{ t: 'warehouse.erp_raw_companyb.orders_latest', s: 0.7, dim: true }],
  ]
</script>

<Slide kicker="Tags" title="Passive tagging" layout="wide">
  <p class="lead">With default settings, every segment of a model's name becomes a tag.</p>

  <div class="flow">
  <div class="scroll">
    <div class="diagram">
      <!-- above: catalog, schema and table -->
      <div class="brk top catalog"><span class="lbl">catalog</span></div>
      <div class="brk top schema"><span class="lbl">schema</span></div>
      <div class="brk top tbl"><span class="lbl">table</span></div>

      <!-- the name itself -->
      {#each parts as p, i}
        <span class="seg" class:sep={!isPart(p)} style:grid-column={i + 1}>{p}</span>
      {/each}

      <!-- below: each part, with its tag -->
      {#each parts as p, i}
        {#if isPart(p)}
          <span class="tick" style:grid-column={i + 1}></span>
          <span class="tag" style:grid-column={i + 1}>{p}</span>
        {/if}
      {/each}

      <!-- bottom: schema.table, and the fully qualified name -->
      <div class="brk bottom schema-table"><span class="lbl">schema.table</span></div>
      <div class="brk bottom full"><span class="lbl">full model name</span></div>
    </div>
  </div>

  <span class="arrow" aria-hidden="true">→</span>

  <div class="cloud" aria-label="the resulting tags">
    {#each cloud as row}
      <div class="row">
        {#each row as w}
          {#if w.hi}
            <span class="allgrp" tabindex="0">
              <span class="word hi" style:font-size="{w.s}em">{w.t}</span>
              <span class="allnote" role="tooltip">on every model by default, so <code>TAGS='[all]'</code> runs everything</span>
            </span>
          {:else}
            <span class="word" class:dim={w.dim} style:font-size="{w.s}em">{w.t}</span>
          {/if}
        {/each}
      </div>
    {/each}
  </div>
  </div>

  <p class="teaser"><a href="#5">Why is this useful? →</a></p>
</Slide>

<style>
  .flow {
    display: flex;
    align-items: center;
    gap: clamp(0.75rem, 2vw, 2rem);
    min-width: 0;
  }

  .scroll {
    overflow-x: auto;
    min-width: 0;
    padding-block: 0.25rem;
  }

  .arrow {
    flex: none;
    font-family: var(--font-mono);
    font-size: clamp(1.6rem, 3vw, 2.6rem);
    color: var(--accent);
  }

  .cloud {
    flex: none;
    width: min(26rem, 40vw);
    display: grid;
    gap: 0.35em;
    justify-items: center;
    font-family: var(--font-mono);
    font-size: clamp(0.8rem, 1.15vw, 1.1rem);
    line-height: 1;
    color: var(--fg);
  }

  .row {
    display: flex;
    align-items: baseline;
    justify-content: center;
    gap: 0.7em;
    max-width: 100%;
  }

  .word {
    white-space: nowrap;
    font-weight: 600;
    letter-spacing: -0.01em;
  }

  .word.dim {
    color: var(--fg-2);
    font-weight: 400;
  }

  .word.hi {
    color: var(--accent);
    font-weight: 700;
  }

  /* `all`, with its explanation as a tooltip on hover, focus or tap */
  .allgrp {
    position: relative;
    display: inline-block;
    cursor: help;
    outline: none;
  }

  .allgrp .word {
    text-decoration: underline dotted;
    text-decoration-thickness: 2px;
    text-underline-offset: 0.12em;
  }

  .allnote {
    position: absolute;
    left: calc(100% + 0.8em);
    top: 50%;
    transform: translateY(-50%) translateX(-4px);
    width: 13em;
    padding: 0.5em 0.7em;
    border: 1px solid var(--line);
    border-radius: 0.6em;
    background: var(--bg-2);
    box-shadow: 0 12px 30px -16px rgba(10, 20, 44, 0.7);
    font-family: var(--font-body);
    font-size: 0.8em;
    line-height: 1.3;
    color: var(--fg);
    white-space: normal;
    opacity: 0;
    visibility: hidden;
    transition: opacity 0.2s, transform 0.2s, visibility 0.2s;
    pointer-events: none;
  }

  .allgrp:hover .allnote,
  .allgrp:focus-visible .allnote,
  .allgrp:focus-within .allnote {
    opacity: 1;
    visibility: visible;
    transform: translateY(-50%) translateX(0);
  }

  .allnote code {
    font-size: 0.9em;
    padding: 0 0.25em;
  }

  .teaser {
    margin-top: 0.5rem;
    max-width: none;
    text-align: center;
    font-style: italic;
    font-size: clamp(1rem, 1.5vw, 1.3rem);
  }

  .teaser a {
    color: var(--fg-2);
    text-decoration: none;
  }

  .teaser a:hover {
    color: var(--accent);
  }

  @media (max-width: 900px) {
    .flow {
      flex-wrap: wrap;
    }
    .arrow {
      display: none;
    }
    .cloud {
      width: 100%;
    }
  }

  .diagram {
    display: grid;
    grid-template-columns: repeat(11, auto);
    grid-template-rows: auto auto auto auto auto auto;
    justify-content: start;
    align-items: end;
    column-gap: 0;
    font-family: var(--font-mono);
    font-size: clamp(0.85rem, 1.7vw, 1.5rem);
    width: max-content;
    padding: 1.6em 0.5em 1.5em 0;
  }

  .seg {
    grid-row: 2;
    padding-inline: 0.06em;
    line-height: 1.2;
    font-weight: 600;
    color: var(--fg);
  }

  .seg.sep {
    color: var(--fg-2);
    font-weight: 400;
  }

  /* brackets */
  .brk {
    position: relative;
    height: 0.55em;
    border: 2px solid var(--accent);
    border-radius: 0.3em;
  }

  .brk.top {
    grid-row: 1;
    border-bottom: 0;
    border-bottom-left-radius: 0;
    border-bottom-right-radius: 0;
    margin-bottom: 0.25em;
  }

  .brk.bottom {
    border-top: 0;
    border-top-left-radius: 0;
    border-top-right-radius: 0;
  }

  .brk.catalog {
    grid-column: 1 / 2;
  }

  .brk.schema {
    grid-column: 3 / 8;
  }

  .brk.tbl {
    grid-column: 9 / 12;
  }

  .brk.schema-table {
    grid-row: 5;
    grid-column: 3 / 12;
    margin-top: 0.35em;
  }

  .brk.full {
    grid-row: 6;
    grid-column: 1 / 12;
    margin-top: 1.1em; /* just enough room for the schema.table label */
  }

  .lbl {
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    font-size: 0.5em;
    font-weight: 600;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--fg);
    white-space: nowrap;
  }

  .brk.top .lbl {
    bottom: 100%;
    padding-bottom: 0.4em;
  }

  .brk.bottom .lbl {
    top: 100%;
    padding-top: 0.4em;
  }

  /* each part: a tick down to its tag */
  .tick {
    grid-row: 3;
    justify-self: center;
    width: 2px;
    height: 0.7em;
    margin-top: 0.15em;
    background: var(--accent);
  }

  .tag {
    grid-row: 4;
    justify-self: center;
    font-size: 0.5em;
    font-weight: 600;
    padding: 0.2em 0.6em;
    border: 1px solid var(--accent);
    color: var(--fg);
    border-radius: 999px;
    white-space: nowrap;
  }

</style>
