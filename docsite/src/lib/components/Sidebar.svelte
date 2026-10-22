<script lang="ts">
  // The page list, built from the folders: home, docs, articles, then one
  // group per image folder under an "images" heading, each with its README
  // first. Every group and the images section fold on a click; all start
  // folded except what holds the current page, and the folds are remembered
  // per browser. A narrow screen gets the list as a drawer.
  import { untrack } from 'svelte'
  import { base } from '$app/paths'
  import { page } from '$app/state'
  import { menu } from '$lib/ui.svelte'

  type Group = (typeof page.data.nav)[number]

  // by pathname, not by href: base is a relative path while the server
  // renders, so an href comparison only works after hydration
  const current = (url: string) => (page.url.pathname === url ? 'page' : undefined)
  const close = () => (menu.open = false)

  const KEY = 'sidebar-folded-1'
  let folded = $state<Record<string, boolean>>({})
  let loaded = $state(false)

  $effect(() => {
    try {
      const saved = localStorage.getItem(KEY)
      if (saved) folded = JSON.parse(saved)
    } catch {
      // no storage, the defaults will do
    }
    loaded = true
  })

  $effect(() => {
    if (!loaded) return
    const snapshot = JSON.stringify(folded)
    try {
      localStorage.setItem(KEY, snapshot)
    } catch {
      // no storage, the choice lasts for this page
    }
  })

  // everything that can fold starts folded, except what holds the current page
  const holdsCurrent = (group: Group) => group.pages.some((p) => p.url === page.url.pathname)
  const sectionKey = (parent: string) => `section:${parent}`
  const isFolded = (group: Group) => folded[group.name] ?? !holdsCurrent(group)
  const isSectionFolded = (parent: string) =>
    folded[sectionKey(parent)] ?? !page.data.nav.some((g) => g.parent === parent && holdsCurrent(g))
  // open now means fold it, folded now means open it
  const toggle = (key: string, open: boolean) => (folded[key] = open)

  // arriving on a page opens whatever holds it; only the path is watched,
  // so a fold made on the page stays folded
  $effect(() => {
    const pathname = page.url.pathname
    const here = page.data.nav.find((g) => g.pages.some((p) => p.url === pathname))
    if (!here) return
    untrack(() => {
      if (folded[here.name]) folded[here.name] = false
      if (here.parent && folded[sectionKey(here.parent)]) folded[sectionKey(here.parent)] = false
    })
  })
</script>

{#if menu.open}
  <button type="button" class="scrim" aria-label="Close the page list" onclick={close}></button>
{/if}

<aside class="side" class:open={menu.open}>
  <nav aria-label="Pages">
    {#each page.data.nav as group, i (group.name)}
      {#if group.parent && group.parent !== page.data.nav[i - 1]?.parent}
        {@const open = !isSectionFolded(group.parent)}
        <p class="eyebrow section-title">
          <button type="button" class="fold" aria-expanded={open} onclick={() => toggle(sectionKey(group.parent), open)}>
            {group.parent}
          </button>
        </p>
      {/if}
      {#if !group.parent || !isSectionFolded(group.parent)}
        {#if group.label}
          {@const open = !isFolded(group)}
          <p class="eyebrow group-title" class:nested={group.parent}>
            <button type="button" class="fold" aria-expanded={open} onclick={() => toggle(group.name, open)}>
              {group.label}
            </button>
          </p>
          {#if open}
            <ol class:nested={group.parent}>
              {#each group.pages as p (p.url)}
                <li>
                  <a href="{base}{p.url}" aria-current={current(p.url)} onclick={close}>{p.title}</a>
                </li>
              {/each}
            </ol>
          {/if}
        {:else}
          <ol>
            {#each group.pages as p (p.url)}
              <li>
                <a href="{base}{p.url}" aria-current={current(p.url)} onclick={close}>{p.title}</a>
              </li>
            {/each}
          </ol>
        {/if}
      {/if}
    {/each}
  </nav>
</aside>
