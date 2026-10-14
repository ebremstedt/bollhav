<script>
  // The ball sea, as physics: a tank seen from the side, full of balls that
  // are rigid bodies in Matter.js. With enough small, slippery balls the pile
  // behaves like a granular fluid: a push on one side sends a wave across the
  // tank that reflects off the far wall and comes back. The tank rocks gently
  // all the time, and a slide change pushes the balls in the direction of
  // travel. Colours are read from the CSS tokens, so the theme toggle
  // recolours it.
  import Matter from 'matter-js'

  const { Engine, Bodies, Body, Composite } = Matter

  let { progress = 0, theme = 'light', dir = 1 } = $props()

  // ---- knobs ----
  const PILE = 0.18 // share of the viewport height the settled pile fills
  const PACKING = 0.88 // measured: 1000 balls of r 5–12 fill ~20% of 1440×900
  const MAX_BALLS = 1100
  const MIN_BALLS = 120
  const R_MIN = 5 // ball radius range at desktop width; scaled down on narrow screens
  const R_MAX = 12
  const ROCK_PERIOD = 3.4 // seconds for one left-right rock of the tank
  const ROCK_TILT = 0.22 // radians the gravity vector swings each way
  const PUSH = 7 // px/step of sideways velocity a slide change gives the near wall's balls
  // bouncy and slippery: inelastic balls soak a wave up before it crosses the tank
  const MATERIAL = { restitution: 0.5, friction: 0, frictionStatic: 0, frictionAir: 0.002, density: 0.002 }

  let canvas
  let ctx
  let W = 0
  let H = 0
  let dpr = 1
  let engine
  let walls = []
  let balls = [] // { body, r, tone }
  let kick = 0
  let shove = 0
  let sprites = new Map()
  let tokens = { a: '#1c2f5e', b: '#3f67a8', c: '#9cc0e4', d: '#fbf6ea', rim: '#1c2f5e', alpha: 0.5 }

  const reduced = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches
  const WALL = 400 // thick walls so a fast ball can't tunnel through
  const STEP = 1000 / 60

  function readTokens() {
    const cs = getComputedStyle(document.documentElement)
    const g = (name, fallback) => cs.getPropertyValue(name).trim() || fallback
    tokens = {
      a: g('--ball-a', tokens.a),
      b: g('--ball-b', tokens.b),
      c: g('--ball-c', tokens.c),
      d: g('--ball-d', tokens.d),
      rim: g('--ball-rim', tokens.rim),
      alpha: parseFloat(g('--sea-alpha', '0.5')),
    }
    sprites = new Map()
  }

  function pickTone() {
    const r = Math.random()
    return r < 0.2 ? 'a' : r < 0.55 ? 'b' : r < 0.85 ? 'c' : 'd'
  }

  // smaller balls on a narrow screen, so a phone gets a sea and not a pile
  // of boulders
  const radiusScale = () => Math.min(1, Math.max(0.7, W / 1440))

  // how many balls fill PILE of the viewport height: the pile's area over
  // the mean ball area (r uniform in [rMin, rMax] → π(a² + ab + b²)/3)
  function targetCount() {
    const s = radiusScale()
    const a = R_MIN * s
    const b = R_MAX * s
    const meanArea = (Math.PI * (a * a + a * b + b * b)) / 3
    const n = Math.round((W * H * PILE * PACKING) / meanArea)
    return Math.min(MAX_BALLS, Math.max(MIN_BALLS, n))
  }

  function makeWalls() {
    if (walls.length) Composite.remove(engine.world, walls)
    const opts = { isStatic: true, friction: 0, restitution: MATERIAL.restitution }
    walls = [
      Bodies.rectangle(W / 2, H + WALL / 2, W + 2 * WALL, WALL, opts), // floor
      Bodies.rectangle(-WALL / 2, H / 2, WALL, 4 * H, opts), // left wall
      Bodies.rectangle(W + WALL / 2, H / 2, WALL, 4 * H, opts), // right wall
    ]
    Composite.add(engine.world, walls)
  }

  // balls rain in from above the viewport (or start settled, for reduced motion)
  function spawn(count, settled = false) {
    const bodies = []
    const s = radiusScale()
    for (let i = 0; i < count; i++) {
      const r = (R_MIN + Math.random() * (R_MAX - R_MIN)) * s
      const x = r + Math.random() * (W - 2 * r)
      const y = settled ? H - Math.random() * H * 0.3 : -r - Math.random() * H * 1.2
      const body = Bodies.circle(x, y, r, MATERIAL)
      balls.push({ body, r, tone: pickTone() })
      bodies.push(body)
    }
    Composite.add(engine.world, bodies)
  }

  // take the highest balls out (the tank empties from the top)
  function cull(count) {
    balls.sort((p, q) => p.body.position.y - q.body.position.y)
    const gone = balls.splice(0, count)
    Composite.remove(
      engine.world,
      gone.map((b) => b.body),
    )
  }

  function fill(settled = false) {
    const diff = targetCount() - balls.length
    if (diff > 0) spawn(diff, settled)
    else if (diff < 0) cull(-diff)
  }

  function layout() {
    dpr = Math.min(window.devicePixelRatio || 1, 2)
    W = window.innerWidth
    H = window.innerHeight
    canvas.width = Math.round(W * dpr)
    canvas.height = Math.round(H * dpr)
    sprites = new Map()
    makeWalls()
    // keep every ball inside the resized tank
    for (const b of balls) {
      const p = b.body.position
      if (p.x < b.r || p.x > W - b.r || p.y > H - b.r) {
        Body.setPosition(b.body, { x: Math.min(Math.max(p.x, b.r), W - b.r), y: Math.min(p.y, H - b.r) })
      }
    }
    fill(reduced())
  }

  // one pre-rendered ball per (colour, radius), drawn once and stamped
  function sprite(tone, r) {
    const key = tone + r
    let s = sprites.get(key)
    if (s) return s
    const pad = 2
    const size = (2 * r + 2 * pad) * dpr
    s = document.createElement('canvas')
    s.width = s.height = Math.ceil(size)
    const c = s.getContext('2d')
    c.scale(dpr, dpr)
    const x = r + pad
    const y = r + pad
    c.fillStyle = tokens[tone]
    c.beginPath()
    c.arc(x, y, r, 0, Math.PI * 2)
    c.fill()
    const hi = c.createRadialGradient(x - r * 0.35, y - r * 0.4, r * 0.05, x - r * 0.2, y - r * 0.2, r * 0.95)
    hi.addColorStop(0, 'rgba(255,255,255,0.5)')
    hi.addColorStop(0.45, 'rgba(255,255,255,0.08)')
    hi.addColorStop(1, 'rgba(255,255,255,0)')
    c.fillStyle = hi
    c.fill()
    c.globalAlpha = 0.7
    c.lineWidth = Math.max(1, r * 0.08)
    c.strokeStyle = tokens.rim
    c.stroke()
    sprites.set(key, s)
    return s
  }

  function render() {
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, W, H)
    // opaque: translucent balls blend where they touch and the pile goes white
    ctx.globalAlpha = 1
    for (const b of balls) {
      const { x, y } = b.body.position
      const s = sprite(b.tone, Math.round(b.r))
      const w = s.width / dpr
      ctx.drawImage(s, x - w / 2, y - w / 2, w, w)
    }
  }

  // a slide change: push the balls at the wall behind the direction of travel
  function pushWave(direction) {
    const band = W * 0.18
    for (const b of balls) {
      const p = b.body.position
      const inBand = direction > 0 ? p.x < band : p.x > W - band
      if (!inBand || p.y < H * 0.5) continue
      const v = b.body.velocity
      Body.setVelocity(b.body, { x: v.x + PUSH * direction, y: v.y - 2 })
    }
  }

  let last = 0
  let acc = 0
  function frame(t) {
    const sec = t / 1000
    const dt = last ? Math.min(50, t - last) : STEP
    last = t
    acc += dt

    kick *= Math.pow(0.5, dt / 900)
    shove *= Math.pow(0.5, dt / 300)

    // rock the tank: the gravity vector swings like a pendulum, harder just
    // after a slide change
    const tilt = ROCK_TILT * Math.sin((2 * Math.PI * sec) / ROCK_PERIOD) * (1 + kick)
    engine.gravity.x = Math.sin(tilt) + 0.5 * shove
    engine.gravity.y = Math.cos(tilt)

    // fixed physics steps, at most two per frame so a slow frame can't spiral
    let steps = 0
    while (acc >= STEP && steps < 2) {
      Engine.update(engine, STEP)
      acc -= STEP
      steps++
    }
    if (acc > STEP) acc = 0

    // a ball that somehow escaped the tank drops back in from the top
    for (const b of balls) {
      const p = b.body.position
      if (p.y > H + 2 * WALL || p.x < -2 * WALL || p.x > W + 2 * WALL) {
        Body.setPosition(b.body, { x: b.r + Math.random() * (W - 2 * b.r), y: -b.r })
        Body.setVelocity(b.body, { x: 0, y: 0 })
      }
    }
    render()
  }

  // click a ball and it shoots up, 40–70% of the way to the top, then lands.
  // Gravity in px/step² is gravity.y × Matter's gravityScale × step²; air
  // drag eats roughly a tenth of the rise, so aim a little higher.
  const G = 0.001 * STEP * STEP
  function onPointerDown(e) {
    if (!engine || reduced() || e.button) return
    if (e.target?.closest?.('a, button, input, textarea, [contenteditable]')) return
    const px = e.clientX
    const py = e.clientY
    for (const b of balls) {
      const p = b.body.position
      if (Math.hypot(p.x - px, p.y - py) > Math.max(b.r, 22)) continue
      const rise = (0.4 + Math.random() * 0.3) * H * 1.1
      Body.setVelocity(b.body, { x: (Math.random() - 0.5) * 5, y: -Math.sqrt(2 * G * rise) })
    }
  }

  // reduced motion: let the pile settle off-screen, then show it still
  function settle() {
    engine.gravity.x = 0
    engine.gravity.y = 1
    for (let i = 0; i < 180; i++) Engine.update(engine, STEP)
    render()
  }

  $effect(() => {
    ctx = canvas.getContext('2d')
    engine = Engine.create({ gravity: { x: 0, y: 1 } })
    readTokens()
    layout()

    let raf = 0
    let running = true
    const still = reduced()

    const loop = (t) => {
      if (!running) return
      if (document.hidden) last = 0
      else frame(t)
      raf = requestAnimationFrame(loop)
    }
    if (still) settle()
    else raf = requestAnimationFrame(loop)

    const onResize = () => {
      layout()
      if (still) settle()
    }
    window.addEventListener('resize', onResize)
    return () => {
      running = false
      cancelAnimationFrame(raf)
      window.removeEventListener('resize', onResize)
      Engine.clear(engine)
    }
  })

  // recolour when the theme flips (the tokens live on <html>)
  $effect(() => {
    void theme
    if (ctx) {
      readTokens()
      if (reduced()) render()
    }
  })

  // a slide change: a wave pushed off the wall behind the direction of
  // travel, and a harder rock for a moment. Nothing for a reduced-motion
  // viewer, whose pile stays still.
  $effect(() => {
    void progress
    if (!engine || reduced()) return
    kick = 1
    shove = dir
    pushWave(dir)
  })
</script>

<svelte:window onpointerdown={onPointerDown} />

<canvas bind:this={canvas} aria-hidden="true"></canvas>

<style>
  canvas {
    position: fixed;
    inset: 0;
    width: 100%;
    height: 100%;
    z-index: 0;
    pointer-events: none;
  }
</style>
