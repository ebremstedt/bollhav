<script lang="ts">
  import { base } from '$app/paths'
  import { menu } from '$lib/ui.svelte'
  import Search from './Search.svelte'
  import ThemeToggle from './ThemeToggle.svelte'
  import { toggleFullscreen } from '$lib/fullscreen'
  import { deck, github } from '$lib/sites'
</script>

<header class="top">
  <div class="shell top-row">
    <div class="top-left">
      <button
        type="button"
        class="toggle menu-button"
        aria-label="Pages"
        aria-expanded={menu.open}
        onclick={() => (menu.open = !menu.open)}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">
          <path d="M4 7h16M4 12h16M4 17h16" />
        </svg>
      </button>
      <a class="brand" href="{base}/">
        <img src="{base}/bollhav_logo_small.png" alt="" width="28" height="23" />
        <span>bollhav</span>
      </a>
    </div>
    <Search />
    <div class="top-right">
      <nav class="links" aria-label="Site">
        <a href={deck}>Slide deck</a>
        <a href={github}>GitHub</a>
      </nav>
      <ThemeToggle />
      <button type="button" class="toggle wide-only" aria-label="Fullscreen" title="Fullscreen (f)" onclick={toggleFullscreen}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5" />
        </svg>
      </button>
    </div>
  </div>
</header>
