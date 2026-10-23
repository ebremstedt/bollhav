<script>
  // The deck: renders `slides` (an ordered list of Svelte components) one at a
  // time with keyboard, swipe and button navigation, a progress bar, a slide
  // counter, an outline drawer, a theme toggle, fullscreen, and the URL hash
  // (#3 is the third slide) so any slide can be linked to.
  import { tick } from 'svelte'
  import Sea from './Sea.svelte'
  import SeaControls from './SeaControls.svelte'
  import { DOCS, GITHUB, docsUrl } from './sites.js'

  let {
    slides = [],
    title = 'bollhav',
    home = 'https://bollhav.dev',
    // learn and lab are kept out for now
    links = [
      { label: 'Docs', href: DOCS },
      { label: 'GitHub', href: GITHUB },
    ],
  } = $props()

  const n = $derived(slides.length)
  const pad = (i) => String(i).padStart(2, '0')
  const fromHash = () => {
    const i = parseInt(location.hash.slice(1), 10)
    return Number.isFinite(i) ? Math.min(Math.max(i - 1, 0), n - 1) : 0
  }

  let current = $state(fromHash())
  let dir = $state(1)
  let outlineOpen = $state(false)
  let theme = $state(initialTheme())
  let titles = $state([])
  let hoopOn = $state(false) // the hoop game; the button top right
  let seaOn = $state(true) // the sea's controls, beside the counter
  let density = $state(1)
  let storm = $state(1)
  let score = $state(0)
  let high = $state(readHigh())
  let root

  // a round lasts ROUND seconds; then the hoop goes away and the score resets
  const ROUND = 15
  let timeLeft = $state(ROUND)
  $effect(() => {
    if (!hoopOn) return
    timeLeft = ROUND
    const start = performance.now()
    const id = setInterval(() => {
      timeLeft = Math.max(0, Math.ceil(ROUND - (performance.now() - start) / 1000))
      if (timeLeft === 0) {
        hoopOn = false
        score = 0
      }
    }, 200)
    return () => clearInterval(id)
  })

  function readHigh() {
    try {
      return parseInt(localStorage.getItem('hoopHigh'), 10) || 0
    } catch {
      return 0
    }
  }

  // the best score survives in this browser
  $effect(() => {
    if (score > high) {
      high = score
      try {
        localStorage.setItem('hoopHigh', String(score))
      } catch {}
    }
  })

  const progress = $derived(n > 1 ? current / (n - 1) : 0)

  function initialTheme() {
    // ?theme=dark|light forces a theme for this visit (a projector link)
    const forced = new URLSearchParams(location.search).get('theme')
    if (forced === 'dark' || forced === 'light') {
      try {
        localStorage.setItem('theme', forced)
      } catch {}
      return forced
    }
    try {
      const saved = localStorage.getItem('theme')
      if (saved === 'dark' || saved === 'light') return saved
    } catch {}
    return matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  }

  function go(i) {
    const next = Math.min(Math.max(i, 0), n - 1)
    if (next === current) return
    dir = next > current ? 1 : -1
    current = next
    outlineOpen = false
  }
  const next = () => go(current + 1)
  const prev = () => go(current - 1)

  function toggleTheme() {
    theme = theme === 'dark' ? 'light' : 'dark'
    try {
      localStorage.setItem('theme', theme)
    } catch {}
  }

  function toggleFullscreen() {
    const el = document.documentElement
    if (document.fullscreenElement) document.exitFullscreen?.()
    else el.requestFullscreen?.().catch?.(() => {})
  }

  function onKey(e) {
    if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey) return
    const t = e.target
    if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return
    switch (e.key) {
      case 'ArrowRight':
      case 'ArrowDown':
      case 'PageDown':
      case 'Enter':
        e.preventDefault()
        next()
        break
      case ' ':
        e.preventDefault()
        e.shiftKey ? prev() : next()
        break
      case 'ArrowLeft':
      case 'ArrowUp':
      case 'PageUp':
      case 'Backspace':
        e.preventDefault()
        prev()
        break
      case 'Home':
        e.preventDefault()
        go(0)
        break
      case 'End':
        e.preventDefault()
        go(n - 1)
        break
      case 'o':
        outlineOpen = !outlineOpen
        break
      case 'f':
        toggleFullscreen()
        break
      case 't':
        toggleTheme()
        break
      case 'd':
        location.href = docsUrl(theme)
        break
      case 's':
        go(0)
        break
      case 'Escape':
        outlineOpen = false
        break
    }
  }

  // swipe (touch only, so mouse text selection is untouched). The decision
  // is made while the finger moves, not on release: a browser that decides
  // mid-gesture to scroll instead fires pointercancel and never pointerup.
  let swipe = null
  function onPointerDown(e) {
    if (e.pointerType !== 'touch' || outlineOpen) return
    swipe = { x: e.clientX, y: e.clientY, done: false }
  }
  function onPointerMove(e) {
    if (!swipe || swipe.done || e.pointerType !== 'touch') return
    const dx = e.clientX - swipe.x
    const dy = e.clientY - swipe.y
    if (Math.abs(dx) > 48 && Math.abs(dx) > Math.abs(dy) * 1.3) {
      swipe.done = true
      dx < 0 ? next() : prev()
    }
  }
  function onPointerEnd() {
    swipe = null
  }

  // theme -> <html data-theme>
  $effect(() => {
    document.documentElement.dataset.theme = theme
  })

  // current slide -> URL hash, and the other way round (back/forward)
  $effect(() => {
    const want = current === 0 ? '' : `#${current + 1}`
    if (location.hash !== want) history.replaceState(null, '', location.pathname + location.search + want)
  })
  const onHash = () => (current = fromHash())

  // scroll the newly shown slide to its top
  $effect(() => {
    void current
    tick().then(() => root?.querySelector('.slide.is-active')?.scrollTo(0, 0))
  })

  // outline titles come from each slide's first heading
  $effect(() => {
    titles = [...root.querySelectorAll('.slide')].map(
      (s, i) => {
        const h = s.querySelector('h1, h2, h3')
        return h?.dataset.title || h?.textContent?.trim() || `Slide ${i + 1}`
      },
    )
  })

  $effect(() => {
    document.title = current === 0 ? title : `${titles[current] ?? ''} · ${title}`
  })
</script>

<svelte:window
  onkeydown={onKey}
  onhashchange={onHash}
  onpointerdown={onPointerDown}
  onpointermove={onPointerMove}
  onpointerup={onPointerEnd}
  onpointercancel={onPointerEnd}
/>

{#if seaOn}
  <Sea {progress} {theme} {dir} hoop={hoopOn} onscore={(s) => (score = s)} {density} {storm} />
{/if}

<div class="deck" data-dir={dir} bind:this={root}>
  {#each slides as S, i (i)}
    <section class="slide" class:is-active={i === current} hidden={i !== current} aria-hidden={i !== current}>
      <div class="inner">
        {#if i > 0}
          <span class="num" aria-label="slide {i + 1} of {n}">{pad(i + 1)} / {pad(n)}</span>
        {/if}
        <S />
      </div>
    </section>
  {/each}
</div>

<div class="progress" style:width="{((current + 1) / n) * 100}%"></div>

<header class="chrome top">
  <a class="home" href={home} title="bollhav">
    <img src="./bollhav.png" alt="" width="30" height="25" />
    <span>{title}</span>
  </a>
  <div class="group">
    <nav class="links" aria-label="Site">
      {#each links as l}
        <a href={l.href === DOCS ? docsUrl(theme) : l.href}>{l.label}</a>
      {/each}
    </nav>
    {#if hoopOn}
      <span class="scorepill" title="Click a ball to launch it through the hoop. {ROUND} seconds per round">
        <b>{score}</b> <span class="sep">·</span> <span class="time" class:low={timeLeft <= 5}>{timeLeft}s</span>
        <span class="sep">·</span> best <b>{high}</b>
      </span>
    {/if}
    <button class="btn" class:on={hoopOn} onclick={() => (hoopOn = !hoopOn)} disabled={!seaOn} title="Hoop game" aria-label="Hoop game" aria-pressed={hoopOn}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
        <ellipse cx="12" cy="9" rx="8" ry="3" />
        <path d="M6 11.5l1.5 6.5M18 11.5l-1.5 6.5M9 12l.6 6M15 12l-.6 6M7.5 18h9" stroke-width="1.3" />
      </svg>
    </button>
    <button class="btn" onclick={toggleTheme} title="Theme (t)" aria-label="Toggle theme">
      {#if theme === 'dark'}
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
          <circle cx="12" cy="12" r="4" />
          <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
        </svg>
      {:else}
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
        </svg>
      {/if}
    </button>
    <button class="btn" onclick={() => (outlineOpen = !outlineOpen)} title="Outline (o)" aria-label="Outline" aria-expanded={outlineOpen}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
        <path d="M4 7h16M4 12h16M4 17h10" />
      </svg>
    </button>
    <button class="btn wide-only" onclick={toggleFullscreen} title="Fullscreen (f)" aria-label="Fullscreen">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5" />
      </svg>
    </button>
  </div>
</header>

<footer class="chrome bottom">
  <div class="group">
    <button class="counter" onclick={() => (outlineOpen = !outlineOpen)} title="Outline (o)">
      {pad(current + 1)} <span class="sep">/</span> {pad(n)}
    </button>
    <div class="wide-only">
      <SeaControls bind:density bind:storm bind:enabled={seaOn} />
    </div>
  </div>
  {#if n > 1}
    <p class="hint">
      <kbd>←</kbd> <kbd>→</kbd> <span class="sep">·</span> <kbd>f</kbd> fullscreen <span class="sep">·</span> <kbd>t</kbd> theme
      <span class="sep">·</span> <kbd>d</kbd> docs
    </p>
  {/if}
  <div class="group">
    <button class="btn" onclick={prev} disabled={current === 0} aria-label="Previous slide">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M15 5l-7 7 7 7" />
      </svg>
    </button>
    <button class="btn" onclick={next} disabled={current === n - 1} aria-label="Next slide">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M9 5l7 7-7 7" />
      </svg>
    </button>
  </div>
</footer>

{#if outlineOpen}
  <button class="backdrop" onclick={() => (outlineOpen = false)} aria-label="Close outline"></button>
  <aside class="outline" aria-label="Slides">
    <p class="outline-title">{title}</p>
    <ol>
      {#each titles as t, i}
        <li>
          <button class:active={i === current} onclick={() => go(i)}>
            <span class="n">{pad(i + 1)}</span>
            <span class="t">{t}</span>
          </button>
        </li>
      {/each}
    </ol>
    <nav class="outline-links" aria-label="Site">
      {#each links as l}
        <a href={l.href === DOCS ? docsUrl(theme) : l.href}>{l.label}</a>
      {/each}
    </nav>
  </aside>
{/if}

<style>
  .deck {
    position: relative;
    z-index: 1;
    touch-action: pan-y;
  }

  .slide {
    position: fixed;
    inset: 0;
    height: 100vh;
    height: 100dvh;
    overflow-y: auto;
    overflow-x: hidden;
    /* a slide is a scroll container, so the swipe rule must sit here: the
       browser only looks at touch-action up to the nearest scrollable box */
    touch-action: pan-y;
    display: flex;
    flex-direction: column;
    /* the bottom fifth belongs to the ball pile */
    padding-block: 4.5rem max(5rem, 20vh);
    padding-inline: clamp(1rem, 7vw, 7rem);
  }

  .slide[hidden] {
    display: none;
  }

  .inner {
    width: min(100%, 72rem);
    margin: auto; /* centres when there is room, never clips when there isn't */
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 1rem;
    min-width: 0;
  }

  .slide.is-active .inner {
    animation: deck-slide 0.5s var(--ease) both;
  }
  .deck[data-dir='-1'] .slide.is-active .inner {
    --dx: -28px;
  }

  /* stagger the slide's children in */
  .slide.is-active :global(.slide-body > *) {
    animation: deck-rise 0.6s var(--ease) both;
  }
  .slide.is-active :global(.slide-body > :nth-child(2)) { animation-delay: 60ms; }
  .slide.is-active :global(.slide-body > :nth-child(3)) { animation-delay: 120ms; }
  .slide.is-active :global(.slide-body > :nth-child(4)) { animation-delay: 180ms; }
  .slide.is-active :global(.slide-body > :nth-child(5)) { animation-delay: 240ms; }
  .slide.is-active :global(.slide-body > :nth-child(6)) { animation-delay: 300ms; }
  .slide.is-active :global(.slide-body > :nth-child(n + 7)) { animation-delay: 360ms; }

  .num {
    font-family: var(--font-mono);
    font-size: clamp(0.72rem, 1vw, 0.88rem);
    letter-spacing: 0.14em;
    color: var(--accent);
    font-variant-numeric: tabular-nums;
  }

  /* ---------- chrome ---------- */

  .progress {
    position: fixed;
    top: 0;
    left: 0;
    height: 3px;
    background: var(--accent);
    z-index: 6;
    transition: width 0.45s var(--ease);
  }

  .chrome {
    position: fixed;
    left: 0;
    right: 0;
    z-index: 5;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding-inline: clamp(0.75rem, 2vw, 1.5rem);
    pointer-events: none;
  }
  .chrome > * {
    pointer-events: auto;
  }
  .chrome.top {
    top: 0;
    padding-top: calc(0.75rem + env(safe-area-inset-top, 0px));
  }
  .chrome.bottom {
    bottom: 0;
    padding-bottom: calc(0.75rem + env(safe-area-inset-bottom, 0px));
  }

  .group {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  /* site links, as one pill beside the buttons */
  .links {
    display: flex;
    align-items: center;
    height: 2.5rem;
    margin-right: 0.5rem;
    padding-inline: 0.4rem;
    border: 1px solid var(--line);
    border-radius: 999px;
    background: color-mix(in srgb, var(--bg) 72%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
  }
  .links a {
    padding: 0.35rem 0.65rem;
    border-radius: 999px;
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: -0.005em;
    color: var(--fg-2);
    text-decoration: none;
  }
  .links a:hover {
    color: var(--accent);
    background: var(--bg-2);
  }

  .btn {
    width: 2.5rem;
    height: 2.5rem;
    border-radius: 999px;
    border: 1px solid var(--line);
    background: color-mix(in srgb, var(--bg) 72%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    color: var(--fg);
    display: grid;
    place-items: center;
    cursor: pointer;
    padding: 0;
    transition: border-color 0.2s, transform 0.2s var(--ease), opacity 0.2s;
  }
  .btn svg {
    width: 1.15rem;
    height: 1.15rem;
  }
  .btn:hover:not(:disabled) {
    border-color: var(--accent);
    transform: translateY(-1px);
  }
  .btn:disabled {
    opacity: 0.35;
    cursor: default;
  }

  .home {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    color: var(--fg);
    text-decoration: none;
    font-weight: 600;
    letter-spacing: -0.01em;
    padding: 0.3rem 0.6rem 0.3rem 0.3rem;
    border-radius: 999px;
  }
  .home img {
    width: 30px;
    height: auto;
    display: block;
  }
  .home:hover span {
    color: var(--accent);
  }

  /* the counter and the hint sit over the sea, so they get a soft pill */
  .counter,
  .hint {
    font-family: var(--font-mono);
    color: var(--fg-2);
    background: color-mix(in srgb, var(--bg) 70%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border-radius: 999px;
    padding: 0.5rem 0.8rem;
  }

  .counter {
    font-size: 0.85rem;
    letter-spacing: 0.12em;
    font-variant-numeric: tabular-nums;
    border: 0;
    cursor: pointer;
  }
  .counter:hover {
    color: var(--fg);
  }
  .btn.on {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--bg);
  }

  /* score and best, beside the hoop button */
  .scorepill {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    height: 2.5rem;
    padding-inline: 0.85rem;
    border: 1px solid var(--line);
    border-radius: 999px;
    background: color-mix(in srgb, var(--bg) 72%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    font-family: var(--font-mono);
    font-size: 0.85rem;
    letter-spacing: 0.04em;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }
  .scorepill b {
    font-weight: 700;
    font-size: 1rem;
  }
  .scorepill b:first-of-type {
    color: var(--warm-1); /* the score, orange red */
  }
  .scorepill b:last-of-type {
    color: var(--warm-2); /* the best, gold */
  }
  .scorepill .time {
    min-width: 2.2em;
    text-align: center;
  }
  .scorepill .time.low {
    color: var(--warm-1);
  }
  .sep {
    opacity: 0.45;
  }

  .hint {
    margin: 0;
    font-size: 0.78rem;
    letter-spacing: 0.04em;
    animation: deck-rise 0.6s 0.25s var(--ease) both;
  }

  @media (max-width: 640px) {
    .hint,
    .wide-only,
    .links,
    .home span {
      display: none;
    }
    .slide {
      padding-block: 4rem max(4.5rem, 22vh);
    }
  }

  /* ---------- outline drawer ---------- */

  .backdrop {
    position: fixed;
    inset: 0;
    z-index: 7;
    border: 0;
    padding: 0;
    background: rgba(5, 12, 30, 0.35);
    cursor: pointer;
  }

  .outline {
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    width: min(22rem, 100%);
    z-index: 8;
    background: var(--bg);
    border-left: 1px solid var(--line);
    padding: 1.25rem 1rem;
    padding-top: calc(1.25rem + env(safe-area-inset-top, 0px));
    overflow-y: auto;
    animation: deck-slide 0.35s var(--ease) both;
  }

  .outline-title {
    margin: 0 0 1rem 0.75rem;
    font-weight: 600;
    letter-spacing: -0.01em;
  }

  .outline ol {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 2px;
  }

  .outline li button {
    width: 100%;
    display: flex;
    gap: 0.9rem;
    align-items: baseline;
    text-align: left;
    background: none;
    border: 0;
    border-radius: 0.6rem;
    padding: 0.55rem 0.75rem;
    color: var(--fg);
    font: inherit;
    cursor: pointer;
  }
  .outline li button:hover {
    background: var(--bg-2);
  }
  .outline li button.active {
    background: var(--bg-2);
    box-shadow: inset 3px 0 0 var(--accent);
  }
  .outline .n {
    font-family: var(--font-mono);
    font-size: 0.75rem;
    letter-spacing: 0.1em;
    color: var(--accent);
    flex: none;
  }
  .outline .t {
    line-height: 1.3;
  }

  .outline-links {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 1rem;
    margin: 1.25rem 0.75rem 0;
    padding-top: 1rem;
    border-top: 1px solid var(--line);
    font-size: 0.9rem;
  }
  .outline-links a {
    color: var(--fg-2);
    text-decoration: none;
    font-weight: 600;
  }
  .outline-links a:hover {
    color: var(--accent);
  }
</style>
