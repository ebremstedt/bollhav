<script lang="ts">
  // Previous and next in reading order, a counter, and a progress line along
  // the top. The arrow keys move between pages too, unless the user is
  // typing in a field, and t toggles the theme.
  import { base } from '$app/paths'
  import { goto } from '$app/navigation'
  import { page } from '$app/state'
  import { toggleTheme } from '$lib/theme'

  const order = $derived(page.data.order)
  const n = $derived(order.length)
  // by pathname, not by href: base is relative while the server renders
  const index = $derived(Math.max(0, order.findIndex((p) => p.url === page.url.pathname)))
  const prev = $derived(index > 0 ? order[index - 1] : null)
  const next = $derived(index < n - 1 ? order[index + 1] : null)
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
    }
  }
</script>

<svelte:window onkeydown={onKey} />

<div class="progress" style:width="{((index + 1) / n) * 100}%"></div>

<p class="keys" aria-label="Keyboard shortcuts">
  <kbd>←</kbd> <kbd>→</kbd> page <span class="sep">·</span> <kbd>t</kbd> theme
</p>

<nav class="pager" aria-label="Previous and next page">
  {#if prev}
    <a class="step prev" href="{base}{prev.url}" title="Previous page (left arrow)">
      <span class="eyebrow">Previous</span>
      <span class="step-title">{prev.title}</span>
    </a>
  {:else}
    <span></span>
  {/if}
  <span class="counter" aria-label="page {index + 1} of {n}">
    {pad(index + 1)} <span class="sep">/</span> {pad(n)}
  </span>
  {#if next}
    <a class="step next" href="{base}{next.url}" title="Next page (right arrow)">
      <span class="eyebrow">Next</span>
      <span class="step-title">{next.title}</span>
    </a>
  {:else}
    <span></span>
  {/if}
</nav>
