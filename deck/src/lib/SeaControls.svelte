<script>
  // The sea's controls, behind a small wave beside the counter: on or off,
  // how many balls, and how stormy. The settings are remembered in this
  // browser; the panel itself starts closed on every load.
  let { density = $bindable(1), storm = $bindable(1), enabled = $bindable(true) } = $props()

  const KEY = 'sea-controls'
  let open = $state(false)
  let loaded = $state(false)

  $effect(() => {
    try {
      const saved = localStorage.getItem(KEY)
      if (saved) {
        const { density: d, storm: s, enabled: e } = JSON.parse(saved)
        if (typeof d === 'number') density = d
        if (typeof s === 'number') storm = s
        if (typeof e === 'boolean') enabled = e
      }
    } catch {
      // no storage, the defaults will do
    }
    loaded = true
  })

  $effect(() => {
    if (!loaded) return
    const snapshot = JSON.stringify({ density, storm, enabled })
    try {
      localStorage.setItem(KEY, snapshot)
    } catch {
      // no storage, the choice lasts for this page
    }
  })
</script>

<div class="sea" class:open>
  <button type="button" class="wave" aria-label="The ball sea" title="The ball sea" aria-expanded={open} onclick={() => (open = !open)}>
    <span aria-hidden="true">≈</span>
  </button>
  {#if open}
    <form aria-label="The ball sea" onsubmit={(e) => e.preventDefault()}>
      <label>
        <input type="checkbox" bind:checked={enabled} />
        <span>{enabled ? 'on' : 'off'}</span>
      </label>
      <label>
        <span>number of balls</span>
        <input type="range" min="0" max="1" step="0.05" bind:value={density} disabled={!enabled} />
      </label>
      <label>
        <span>storminess</span>
        <input type="range" min="0" max="1" step="0.05" bind:value={storm} disabled={!enabled} />
      </label>
    </form>
  {/if}
</div>

<style>
  .sea {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  /* a small wave; the sliders open beside it, quiet and unframed */
  .wave {
    display: grid;
    place-items: center;
    width: 1.6rem;
    height: 1.6rem;
    padding: 0;
    border: 1px solid var(--line);
    border-radius: 999px;
    background: color-mix(in srgb, var(--bg) 72%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    color: var(--fg-2);
    font-size: 0.9rem;
    line-height: 1;
    cursor: pointer;
  }

  .wave:hover,
  .open .wave {
    color: var(--accent);
  }

  form {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    height: 1.6rem;
    margin: 0;
    padding-inline: 0.6rem;
    border-radius: 999px;
    background: color-mix(in srgb, var(--bg) 70%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
  }

  label {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-family: var(--font-mono);
    font-size: 0.6rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    white-space: nowrap;
    color: var(--fg-2);
  }

  input {
    accent-color: var(--accent);
    width: 4rem;
    height: 0.8rem;
    margin: 0;
    opacity: 0.8;
  }

  input:disabled {
    opacity: 0.3;
  }

  input[type='checkbox'] {
    width: 0.8rem;
    cursor: pointer;
  }
</style>
