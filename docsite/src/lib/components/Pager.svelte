<script lang="ts">
  // A progress line along the top, and the keys: the arrows move between
  // pages in reading order, unless the user is typing in a field, t toggles
  // the theme and f fullscreen.
  import { base } from '$app/paths'
  import { goto } from '$app/navigation'
  import { page } from '$app/state'
  import { toggleTheme } from '$lib/theme'
  import { toggleFullscreen } from '$lib/fullscreen'

  const order = $derived(page.data.order)
  const n = $derived(order.length)
  const index = $derived(Math.max(0, order.findIndex((p) => base + p.url === page.url.pathname)))
  const prev = $derived(index > 0 ? order[index - 1] : null)
  const next = $derived(index < n - 1 ? order[index + 1] : null)

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

<p class="keys" aria-label="Keyboard shortcuts">
  <kbd>←</kbd> <kbd>→</kbd> page <span class="sep">·</span> <kbd>t</kbd> theme <span class="sep">·</span> <kbd>f</kbd> fullscreen
</p>
