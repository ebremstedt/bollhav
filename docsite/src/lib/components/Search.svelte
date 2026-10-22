<script lang="ts">
  // Full-text search over the built site. Pagefind writes its index next to
  // the pages at build time; during `vite dev` there is none, and the box
  // says so instead of searching.
  import { base } from '$app/paths'
  import { afterNavigate } from '$app/navigation'

  type Hit = { url: string; meta: { title?: string }; excerpt: string }
  type Pagefind = {
    init: () => Promise<void>
    search: (q: string) => Promise<{ results: { data: () => Promise<Hit> }[] }>
  }

  let query = $state('')
  let hits: Hit[] = $state([])
  let open = $state(false)
  let unavailable = $state(false)
  let pagefind: Pagefind | undefined
  let timer: ReturnType<typeof setTimeout> | undefined
  let input: HTMLInputElement | undefined = $state()

  async function engine(): Promise<Pagefind | undefined> {
    if (pagefind || unavailable) return pagefind
    try {
      pagefind = (await import(/* @vite-ignore */ `${base}/pagefind/pagefind.js`)) as Pagefind
      await pagefind.init()
    } catch {
      unavailable = true
    }
    return pagefind
  }

  async function run() {
    const q = query.trim()
    if (!q) {
      hits = []
      return
    }
    const pf = await engine()
    if (!pf) return
    const found = await pf.search(q)
    hits = await Promise.all(found.results.slice(0, 8).map((r) => r.data()))
  }

  function onInput() {
    open = true
    clearTimeout(timer)
    timer = setTimeout(run, 120)
  }

  function onKey(e: KeyboardEvent) {
    if (e.key === 'Escape') {
      open = false
      input?.blur()
    }
  }

  afterNavigate(() => {
    open = false
    query = ''
    hits = []
  })
</script>

<div class="search" class:open>
  <label class="search-box">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
      <circle cx="11" cy="11" r="7" />
      <path stroke-linecap="round" d="M20 20l-3.5-3.5" />
    </svg>
    <input
      type="search"
      placeholder="Search"
      aria-label="Search the documentation"
      autocomplete="off"
      bind:value={query}
      bind:this={input}
      oninput={onInput}
      onfocus={() => (open = true)}
      onkeydown={onKey}
    />
  </label>
  {#if open && query.trim()}
    <div class="results" role="listbox">
      {#if unavailable}
        <p class="hint">The search index is built with the site, so there is none while developing.</p>
      {:else if hits.length === 0}
        <p class="hint">Nothing for “{query.trim()}”</p>
      {:else}
        {#each hits as hit (hit.url)}
          <a href={hit.url} role="option" aria-selected="false">
            <strong>{hit.meta.title ?? hit.url}</strong>
            <span class="excerpt">{@html hit.excerpt}</span>
          </a>
        {/each}
      {/if}
    </div>
  {/if}
</div>
