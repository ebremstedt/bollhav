<script lang="ts">
  import { base } from '$app/paths'
  import { menu } from '$lib/ui.svelte'
  import Search from './Search.svelte'
  import ThemeToggle from './ThemeToggle.svelte'

  const github = 'https://github.com/ebremstedt/bollhav'
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
      <a class="toggle" href={github} aria-label="The repo on GitHub" title="GitHub">
        <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
          <path
            d="M12 2a10 10 0 00-3.16 19.49c.5.09.68-.22.68-.48v-1.7c-2.78.6-3.37-1.34-3.37-1.34-.45-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.61.07-.61 1 .07 1.53 1.03 1.53 1.03.9 1.53 2.35 1.09 2.92.83.09-.65.35-1.09.63-1.34-2.22-.25-4.56-1.11-4.56-4.94 0-1.09.39-1.98 1.03-2.68-.1-.25-.45-1.27.1-2.65 0 0 .84-.27 2.75 1.02a9.6 9.6 0 015 0c1.91-1.29 2.75-1.02 2.75-1.02.55 1.38.2 2.4.1 2.65.64.7 1.03 1.59 1.03 2.68 0 3.84-2.34 4.68-4.57 4.93.36.31.68.92.68 1.85v2.74c0 .27.18.58.69.48A10 10 0 0012 2z"
          />
        </svg>
      </a>
      <ThemeToggle />
    </div>
  </div>
</header>
