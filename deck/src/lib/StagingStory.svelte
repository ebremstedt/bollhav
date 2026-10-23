<script>
  // Version C: a flat drawing that plays the mechanism. Chunks leave execute
  // one by one and pile up inside the staging table; then the whole pile moves
  // into the target at once and staging is empty again.
  import { onMount } from 'svelte'

  const W = 1180
  const H = 320
  const Y = 150 // the line everything sits on

  // execute box, staging cylinder, target cylinder (the same size), with room
  // between them for the arrow labels
  const ex = { x: 20, y: 88, w: 210, h: 124 }
  const st = { x: 465, y: 60, w: 230, h: 180, ry: 22 }
  const tg = { x: 930, y: 60, w: 230, h: 180, ry: 22 }

  const N = 4 // chunks per interval: the pile stays clear of the lid
  const CW = 26
  const CH = 14
  const PERIOD = 6 // seconds for one interval

  // where chunk i rests inside a cylinder, stacked bottom up
  const pile = (c, i) => [c.x + c.w / 2 - CW / 2, c.y + c.h - c.ry - 26 - i * (CH + 5)]
  const inStaging = (i) => pile(st, i)
  const inTarget = (i) => pile(tg, i)
  const start = [ex.x + ex.w - CW - 14, Y - CH / 2]

  let chunks = $state([])

  onMount(() => {
    if (matchMedia('(prefers-reduced-motion: reduce)').matches) return
    const at = (t) => t / PERIOD
    chunks.forEach((el, i) => {
      const [sx, sy] = start
      const [ax, ay] = inStaging(i)
      const [bx, by] = inTarget(i)
      const t0 = 0.3 + i * 0.5 // leaves execute
      const t1 = t0 + 0.7 // lands in staging
      const t2 = 3.6 // the apply starts
      const t3 = 4.4 // lands in target
      const t4 = 5.2 // gone, the interval is done
      el.animate(
        [
          { offset: 0, transform: `translate(${sx}px, ${sy}px)`, opacity: 0 },
          { offset: at(t0), transform: `translate(${sx}px, ${sy}px)`, opacity: 0 },
          { offset: at(t0 + 0.05), transform: `translate(${sx}px, ${sy}px)`, opacity: 1, easing: 'ease-in-out' },
          { offset: at(t1), transform: `translate(${ax}px, ${ay}px)`, opacity: 1 },
          { offset: at(t2), transform: `translate(${ax}px, ${ay}px)`, opacity: 1, easing: 'ease-in-out' },
          { offset: at(t3), transform: `translate(${bx}px, ${by}px)`, opacity: 1 },
          { offset: at(t4), transform: `translate(${bx}px, ${by}px)`, opacity: 0 },
          { offset: 1, transform: `translate(${bx}px, ${by}px)`, opacity: 0 },
        ],
        { duration: PERIOD * 1000, iterations: Infinity }
      )
    })
  })

  const cylinder = (c) => {
    const rx = c.w / 2
    const top = c.y + c.ry
    const bottom = c.y + c.h - c.ry
    return `M${c.x} ${top} L${c.x} ${bottom} A${rx} ${c.ry} 0 0 0 ${c.x + c.w} ${bottom} L${c.x + c.w} ${top}`
  }
</script>

<svg class="story" viewBox="0 0 {W} {H}" role="img" aria-label="chunks stream into a staging table, then move into the target in one transaction">
  <defs>
    <marker id="head" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 z" />
    </marker>
  </defs>

  <!-- execute -->
  <rect class="shape" x={ex.x} y={ex.y} width={ex.w} height={ex.h} rx="16" />
  <text class="name" x={ex.x + ex.w / 2} y={Y - 26} text-anchor="middle">main.py</text>
  <text class="sub" x={ex.x + ex.w / 2} y={Y - 1} text-anchor="middle">read</text>
  <text class="sub" x={ex.x + ex.w / 2} y={Y + 20} text-anchor="middle">transform</text>
  <text class="sub" x={ex.x + ex.w / 2} y={Y + 41} text-anchor="middle">write</text>

  <!-- staging -->
  <path class="shape" d={cylinder(st)} />
  <ellipse class="shape lid" cx={st.x + st.w / 2} cy={st.y + st.ry} rx={st.w / 2} ry={st.ry} />
  <text class="name" x={st.x + st.w / 2} y={st.y + st.h + 36} text-anchor="middle">staging table</text>
  <text class="sub" x={st.x + st.w / 2} y={st.y + st.h + 62} text-anchor="middle">dropped after the apply</text>

  <!-- target -->
  <path class="shape" d={cylinder(tg)} />
  <ellipse class="shape lid" cx={tg.x + tg.w / 2} cy={tg.y + tg.ry} rx={tg.w / 2} ry={tg.ry} />
  <text class="name" x={tg.x + tg.w / 2} y={tg.y + tg.h + 36} text-anchor="middle">target table</text>
  <text class="sub" x={tg.x + tg.w / 2} y={tg.y + tg.h + 62} text-anchor="middle">all rows or none</text>

  <!-- the two moves -->
  <line class="edge" x1={ex.x + ex.w + 10} y1={Y} x2={st.x - 10} y2={Y} marker-end="url(#head)" />
  <text class="lbl" x={(ex.x + ex.w + st.x) / 2} y={Y - 16} text-anchor="middle">chunk by chunk</text>
  <line class="edge" x1={st.x + st.w + 10} y1={Y} x2={tg.x - 10} y2={Y} marker-end="url(#head)" />
  <text class="lbl" x={(st.x + st.w + tg.x) / 2} y={Y - 16} text-anchor="middle">one transaction</text>

  <!-- the chunks -->
  {#each Array(N) as _, i}
    <rect class="chunk" bind:this={chunks[i]} width={CW} height={CH} rx="3" style:transform="translate({inStaging(i)[0]}px, {inStaging(i)[1]}px)" />
  {/each}
</svg>

<style>
  .story {
    display: block;
    width: min(100%, 64rem);
    height: auto;
    margin-inline: auto;
    overflow: visible;
  }

  .shape {
    fill: color-mix(in srgb, var(--bg-2) 70%, transparent);
    stroke: var(--accent);
    stroke-width: 2;
  }

  .lid {
    fill: var(--bg-2);
  }

  .edge {
    stroke: var(--accent);
    stroke-width: 2;
  }

  marker path {
    fill: var(--accent);
  }

  .name {
    fill: var(--fg);
    font-family: var(--font-display);
    font-size: 26px;
    font-weight: 600;
    letter-spacing: -0.01em;
  }

  .sub {
    fill: var(--fg-2);
    font-family: var(--font-mono);
    font-size: 15px;
    letter-spacing: 0.02em;
  }

  .lbl {
    fill: var(--accent);
    font-family: var(--font-mono);
    font-size: 13px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }

  .chunk {
    fill: var(--warm-2);
    opacity: 0.9; /* the static pile, for reduced motion; the animation takes over otherwise */
  }
</style>
