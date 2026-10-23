<script>
  // A hand-drawn scene: frames, boxes, cylinders and arrows placed by
  // coordinates in a viewBox, sketched with rough.js in the deck's colours.
  //
  //   <RoughScene
  //     width={1000}
  //     height={360}
  //     shapes={[
  //       { kind: 'frame', x: 16, y: 16, w: 968, h: 328, label: 'your computer' },
  //       { kind: 'box', x: 70, y: 135, w: 250, h: 110, label: 'execution', sub: 'python main.py' },
  //       { kind: 'cylinder', x: 600, y: 96, w: 300, h: 100, label: 'choreography', sub: 'state' },
  //       { kind: 'arrow', from: [320, 190], to: [600, 146], label: 'what is missing?', packets: 'pingpong' },
  //     ]}
  //   />
  //
  // frame     an outline with its label in the top left corner (align: 'center' to centre it);
  //           dashed: true for a dashed one
  // box       a rectangle with label and sub centred
  // cylinder  a database with label and sub centred
  // arrow     from [x, y] to [x, y], label above its middle (labelOffset: [dx, dy] to nudge it),
  //           packets 'dots' (a stream one way) or 'pingpong' (one dot there and back)
  // Any shape takes tone 'accent' (default) or 'muted'.
  //
  // Strokes and fills are currentColor, so colours come from CSS and follow
  // the theme without redrawing.
  import { onMount } from 'svelte'
  import rough from 'roughjs'

  let { width = 1000, height = 360, shapes = [] } = $props()

  const RY = 16 // a cylinder's lid

  let sketch = $state()

  onMount(() => {
    const rc = rough.svg(sketch.ownerSVGElement)
    const pen = { roughness: 1.3, bowing: 1.1, strokeWidth: 2.2, stroke: 'currentColor' }
    const tint = { ...pen, fill: 'currentColor', fillStyle: 'solid' }
    const hatch = { ...pen, fill: 'currentColor', fillStyle: 'hachure', hachureGap: 8, fillWeight: 1 }
    const add = (el, ...cls) => {
      el.classList.add(...cls)
      sketch.appendChild(el)
    }

    shapes.forEach((s, i) => {
      const seed = 11 + i * 7
      const tone = s.tone === 'muted' ? 'muted' : 'accent'
      switch (s.kind) {
        case 'frame':
          add(
            rc.rectangle(s.x, s.y, s.w, s.h, {
              ...pen,
              seed,
              roughness: 1.6,
              strokeLineDash: s.dashed ? [10, 8] : undefined,
            }),
            'frame',
            tone
          )
          break
        case 'box':
          add(rc.rectangle(s.x, s.y, s.w, s.h, { ...tint, seed }), 'tint', tone)
          break
        case 'cylinder': {
          const rx = s.w / 2
          const top = s.y + RY
          const bottom = s.y + s.h - RY
          add(
            rc.path(
              `M${s.x} ${top} L${s.x} ${bottom} A${rx} ${RY} 0 0 0 ${s.x + s.w} ${bottom} L${s.x + s.w} ${top} A${rx} ${RY} 0 0 1 ${s.x} ${top} Z`,
              { ...tint, seed }
            ),
            'tint',
            tone
          )
          add(rc.ellipse(s.x + rx, top, s.w, RY * 2, { ...hatch, seed: seed + 1 }), 'hatch', tone)
          break
        }
        case 'arrow': {
          const [x1, y1] = s.from
          const [x2, y2] = s.to
          add(rc.line(x1, y1, x2, y2, { ...pen, seed }), 'arrow', tone)
          // the head: two short strokes back from the tip along the line
          const angle = Math.atan2(y2 - y1, x2 - x1)
          const wing = (da) => [x2 - 16 * Math.cos(angle + da), y2 - 16 * Math.sin(angle + da)]
          add(rc.linearPath([wing(0.5), [x2, y2], wing(-0.5)], { ...pen, seed: seed + 1 }), 'arrow', tone)
          break
        }
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
