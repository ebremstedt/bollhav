<script lang="ts">
  // The sea's sliders, behind a wave button in the bottom bar: how many
  // balls, and how stormy. The setting, and whether the panel is open, are
  // remembered in this browser.
  import { sea } from '$lib/ui.svelte'

  const KEY = 'sea-controls'
  let open = $state(false)
  let loaded = $state(false)

  $effect(() => {
    try {
      const saved = localStorage.getItem(KEY)
      if (saved) {
        const { density: d, storm: s, open: o } = JSON.parse(saved)
        if (typeof d === 'number') sea.density = d
        if (typeof s === 'number') sea.storm = s
        if (typeof o === 'boolean') open = o
      }
    } catch {
      // no storage, the defaults will do
    }
    loaded = true
  })

  $effect(() => {
    if (!loaded) return
    const snapshot = JSON.stringify({ density: sea.density, storm: sea.storm, open })
    try {
      localStorage.setItem(KEY, snapshot)
    } catch {
      // no storage, the choice lasts for this page
    }
  })
</script>

<div class="sea-controls" class:open>
  <button type="button" class="toggle" aria-label="The ball sea" title="The ball sea" aria-expanded={open} onclick={() => (open = !open)}>
    <span aria-hidden="true">≈</span>
  </button>
  {#if open}
    <form aria-label="The ball sea" onsubmit={(e) => e.preventDefault()}>
      <label>
        <span>number of balls</span>
        <input type="range" min="0" max="1" step="0.05" bind:value={sea.density} />
      </label>
      <label>
        <span>storminess</span>
        <input type="range" min="0" max="1" step="0.05" bind:value={sea.storm} />
      </label>
    </form>
  {/if}
</div>
