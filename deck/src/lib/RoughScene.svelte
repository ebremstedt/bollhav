<script>
  // A hand-drawn scene: frames, boxes, cylinders, tables and arrows placed by
  // coordinates in a viewBox, sketched with rough.js in the deck's colours.
  //
  //   <RoughScene
  //     width={1000}
  //     height={360}
  //     shapes={[
  //       { kind: 'frame', x: 16, y: 16, w: 968, h: 328, label: 'your computer' },
  //       { kind: 'box', x: 70, y: 135, w: 250, h: 110, label: 'execution', sub: 'python main.py' },
  //       { kind: 'cylinder', x: 600, y: 96, w: 300, h: 100, label: 'state', sub: 'what is missing?' },
  //       { kind: 'arrow', from: [320, 190], to: [600, 146], label: 'ask', packets: 'pingpong' },
  //     ]}
  //   />
  //
  // frame     an outline with its label in the top left corner (align: 'center' to centre it);
  //           dashed: true or dotted: true for the border
  // box       a rectangle with label and sub centred
  // cylinder  a database with label and sub centred
  // table     a table with a header row, label and sub under it
  // arrow     from [x, y] to [x, y], label above its middle (labelOffset: [dx, dy] to nudge it),
  //           packets 'dots' (a stream one way) or 'pingpong' (one dot there and back)
  // Any shape takes tone 'accent' (default) or 'muted'.
  //
  // The pen lives in sketch.js, shared with the other sketched drawings.
  // Strokes and fills are currentColor, so colours come from CSS and follow
  // the theme without redrawing.
  import { onMount } from 'svelte'
  import { sketcher, RY } from './sketch.js'

  let { width = 1000, height = 360, shapes = [] } = $props()

  let sketch = $state()

  onMount(() => {
    const draw = sketcher(sketch.ownerSVGElement)
    shapes.forEach((s, i) => {
      const seed = 11 + i * 7
      const tone = s.tone === 'muted' ? 'muted' : 'accent'
      let els = []
      switch (s.kind) {
        case 'frame':
          els = draw.frame(s.x, s.y, s.w, s.h, { seed, dashed: s.dashed, dotted: s.dotted })
          break
        case 'box':
          els = draw.box(s.x, s.y, s.w, s.h, { seed })
          break
        case 'cylinder':
          els = draw.cylinder(s.x, s.y, s.w, s.h, { seed })
          break
        case 'table':
          els = draw.table(s.x, s.y, s.w, s.h, { seed })
          break
        case 'arrow':
          els = draw.arrow(s.from[0], s.from[1], s.to[0], s.to[1], { seed })
          break
      }
      for (const el of els) {
        el.classList.add(tone)
        sketch.appendChild(el)
      }
    })
  })

  const mid = (a) => [(a.from[0] + a.to[0]) / 2 + (a.labelOffset?.[0] ?? 0), (a.from[1] + a.to[1]) / 2 + (a.labelOffset?.[1] ?? 0)]
  const path = (a) => `M${a.from[0]} ${a.from[1]} L${a.to[0]} ${a.to[1]}`
</script>

<svg class="scene" viewBox="0 0 {width} {height}" role="img">
  <g class="sketch" bind:this={sketch}></g>

  {#each shapes as s}
    {#if s.kind === 'frame'}
      {#if s.align === 'center'}
        <text class="frame-label" x={s.x + s.w / 2} y={s.y + 30} text-anchor="middle">{s.label}</text>
      {:else}
        <text class="frame-label" x={s.x + 18} y={s.y + 30}>{s.label}</text>
      {/if}
    {:else if s.kind === 'box' || s.kind === 'cylinder'}
      {@const cx = s.x + s.w / 2}
      {@const cy = s.y + s.h / 2 + (s.kind === 'cylinder' ? RY / 2 : 0)}
      <text class="label" x={cx} y={cy + (s.sub ? -2 : 9)} text-anchor="middle">{s.label}</text>
      {#if s.sub}
        <text class="sub" x={cx} y={cy + 24} text-anchor="middle">{s.sub}</text>
      {/if}
    {:else if s.kind === 'table'}
      {@const cx = s.x + s.w / 2}
      <text class="label" x={cx} y={s.y + s.h + 34} text-anchor="middle">{s.label}</text>
      {#if s.sub}
        <text class="sub" x={cx} y={s.y + s.h + 58} text-anchor="middle">{s.sub}</text>
      {/if}
    {:else if s.kind === 'arrow'}
      {#if s.label}
        {@const [lx, ly] = mid(s)}
        <text class="sub" x={lx} y={ly - 12} text-anchor="middle">{s.label}</text>
      {/if}
      {#if s.packets === 'dots'}
        {#each [0, 1, 2, 3] as k}
          <circle class="dot" r="5">
            <animateMotion dur="2s" begin="{k * 0.5}s" repeatCount="indefinite" path={path(s)} />
          </circle>
        {/each}
      {:else if s.packets === 'pingpong'}
        <circle class="dot ping" r="5">
          <animateMotion dur="2.6s" repeatCount="indefinite" calcMode="linear" keyPoints="0;1;1;0;0" keyTimes="0;0.4;0.5;0.9;1" path={path(s)} />
        </circle>
      {/if}
    {/if}
  {/each}
</svg>

<style>
  .scene {
    display: block;
    width: min(100%, 64rem);
    height: auto;
    margin-inline: auto;
    overflow: visible;
  }

  .sketch :global(.accent) {
    color: var(--accent);
  }

  .sketch :global(.muted) {
    color: var(--fg-2);
  }

  /* rough puts the fill path first, then the outline */
  .sketch :global(.tint > path:first-child:not(:only-child)) {
    opacity: 0.14;
  }

  .sketch :global(.hatch > path:first-child:not(:only-child)) {
    opacity: 0.55;
  }

  .sketch :global(.line) {
    opacity: 0.45;
  }

  /* a dotted frame: round dots */
  .sketch :global(.dotted path) {
    stroke-linecap: round;
    stroke-width: 3;
  }

  .label {
    fill: var(--fg);
    font-family: var(--font-display);
    font-size: 26px;
    font-weight: 600;
    letter-spacing: -0.01em;
  }

  .sub,
  .frame-label {
    fill: var(--fg-2);
    font-family: var(--font-mono);
    font-size: 16px;
    letter-spacing: 0.02em;
  }

  .frame-label {
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 14px;
  }

  .dot {
    fill: var(--warm-2);
  }

  .ping {
    fill: var(--warm-1);
  }

  @media (prefers-reduced-motion: reduce) {
    .dot {
      display: none;
    }
  }
</style>
