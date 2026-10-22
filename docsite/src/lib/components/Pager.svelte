<script lang="ts">
  // The deck's bottom bar: a progress line along the top, the page counter
  // (and the sea's sliders on the home page) on the left, the keys in the
  // middle, previous and next on the right. The arrows move between pages in
  // reading order, unless the user is typing in a field; t toggles the theme
  // and f fullscreen.
  import { base } from '$app/paths'
  import { goto } from '$app/navigation'
  import { page } from '$app/state'
  import { toggleTheme } from '$lib/theme'
  import { toggleFullscreen } from '$lib/fullscreen'
  import { menu } from '$lib/ui.svelte'
  import SeaControls from './SeaControls.svelte'

  const order = $derived(page.data.order)
  const n = $derived(order.length)
  const index = $derived(Math.max(0, order.findIndex((p) => base + p.url === page.url.pathname)))
  const prev = $derived(index > 0 ? order[index - 1] : null)
  const next = $derived(index < n - 1 ? order[index + 1] : null)
  const home = $derived(page.data.path === 'README.md')
  const pad = (i: number) => String(i).padStart(2, '0')

  function onKey(e: KeyboardEvent) {
    if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey) return
    const t = e.target as HTMLElement | null
    if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return
    if (e.key === 'ArrowRight' && next) {
      e.preventDefault()
      goto(`${base}${next.url}`)
    } else if (e.key === 'ArrowLeft' && prev) {
      e.preventDefault()
      goto(`${base}${prev.url}`)
    } else if (e.key === 't') {
      toggleTheme()
    } else if (e.key === 'f') {
      toggleFullscreen()
    }
  }
</script>

<svelte:window onkeydown={onKey} />

<div class="progress" style:width="{((index + 1) / n) * 100}%"></div>

<footer class="bottom">
  <div class="group">
    <button type="button" class="counter" onclick={() => (menu.open = !menu.open)} title="Pages">
      {pad(index + 1)} <span class="sep">/</span> {pad(n)}
    </button>
    {#if home}
      <SeaControls />
    {/if}
  </div>
  <p class="keys" aria-label="Keyboard shortcuts">
    <kbd>←</kbd> <kbd>→</kbd> page <span class="sep">·</span> <kbd>t</kbd> theme <span class="sep">·</span> <kbd>f</kbd> fullscreen
  </p>
  <div class="group">
    <button type="button" class="toggle" disabled={!prev} onclick={() => prev && goto(`${base}${prev.url}`)} aria-label="Previous page">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M15 5l-7 7 7 7" />
      </svg>
    </button>
    <button type="button" class="toggle" disabled={!next} onclick={() => next && goto(`${base}${next.url}`)} aria-label="Next page">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M9 5l7 7-7 7" />
      </svg>
    </button>
  </div>
</footer>
