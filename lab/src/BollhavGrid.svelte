<script>
  // ───────────────────────────────────────────────────────────────────────
  // A faithful trace of *everything* a bollhav run does, in the code's real
  // vocabulary. A raw → clean pipeline, one terminal each:
  //   source_system_raw.orders    (upstream, errors at 04:00 then retries)
  //   source_system_clean.orders  (downstream, gated on the raw windows)
  //
  // Each line is categorised so it colours by what it IS:
  //   ddl     — locks + CREATE schema/table/index            (green)
  //   staging — staging schema / table create+drop           (violet)
  //   state   — state:/library: lines, in grades of orange   (running brightest)
  //   data    — rows written to staging / moved to target    (red)
  //   gate    — upstream contract checks                     (cyan)
  //   error   — a failed interval                            (bright red)
  //
  // In the live Lab this generator is deleted and events stream from relay.py.
  // ───────────────────────────────────────────────────────────────────────
  const HOURS = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00']
  const SET = 0.26, ISTEP = 0.2, GAP = 0.16, HOLD = 1.1
  const ERR_H = 4
  const rows = (h) => 800 + h * 260

  const RAW = { id: 'raw', full: 'source_system_raw.orders', schema: 'source_system_raw', table: 'orders', up: '' }
  const CLEAN = { id: 'clean', full: 'source_system_clean.orders', schema: 'source_system_clean', table: 'orders', up: 'source_system_raw.orders' }

  let _ev = []
  let _id = 0
  const push = (t, m, scope, indent, cat, text) =>
    _ev.push({ id: _id++, t, model: m.id, scope, indent, cat, text })

  function setupSteps(m) {
    return [
      ['state', 'acquire model lock (as to not double work)'],
      ['ddl', `CREATE SCHEMA ${m.schema}`],
      ['ddl', `CREATE TABLE ${m.full}`],
      ['ddl', `CREATE INDEX ${m.table}_window_idx`],
      ['staging', 'garbage collect old staging tables'],
      ['state', 'made sure that model library exist and that the errors table exists'],
      ['state', `state: registered the model ${m.full} in the model library`],
      ['state', `state: ensured state table exists for model ${m.full} (z_bollhav.${m.schema}__${m.table})`],
      ['state', `state: prefilled 6 intervals for model ${m.full} in the state table (z_bollhav.${m.schema}__${m.table})`],
      ['state', `state: read 6 actionable intervals for ${m.full}`],
    ]
  }

  function buildRaw(m) {
    const ivals = []
    let t = 0
    push(t, m, '', 0, 'header', `${m.full} · temporal · stateful · staged`); t += 0.02
    for (const [c, txt] of setupSteps(m)) { push(t, m, 'setup', 1, c, txt); t += SET }
    for (let h = 0; h < HOURS.length; h++) {
      const hour = HOURS[h], tag = hour.replace(':', '')
      const iv = { h, hour, runStart: t, errAt: null, retryAt: null, appliedAt: 0, x0: t }
      push(t, m, hour, 1, 'srun', `state: marked running ${hour}`); t += ISTEP
      push(t, m, hour, 2, 'staging', `created staging table z_bollhav_stg.${m.id}_${m.table}_${tag}`); t += ISTEP
      push(t, m, hour, 2, 'data', `wrote ${rows(h)} rows to staging table (append)`); t += ISTEP
      if (h === ERR_H) {
        iv.errAt = t
        push(t, m, hour, 2, 'error', `state: recorded error for ${m.full} (${hour}) — OperationalError: connection reset`); t += ISTEP
        t += HOLD
        iv.retryAt = t
        push(t, m, hour, 2, 'srun', `state: marked running ${hour}  ·  retry`); t += ISTEP
        push(t, m, hour, 2, 'staging', `created staging table z_bollhav_stg.${m.id}_${m.table}_${tag}`); t += ISTEP
        push(t, m, hour, 2, 'data', `wrote ${rows(h)} rows to staging table (append)`); t += ISTEP
      }
      push(t, m, hour, 2, 'data', 'moved data from staging to target (append)'); t += ISTEP
      push(t, m, hour, 2, 'staging', 'drop staging table'); t += ISTEP
      push(t, m, hour, 1, 'sdone', `state: marked applied ${hour}`); iv.appliedAt = t; t += ISTEP
      t += GAP
      ivals.push(iv)
    }
    return { ivals, total: t }
  }

  function buildClean(m, upIvals) {
    const ivals = []
    let t = 0.34
    push(t, m, '', 0, 'header', `${m.full} · temporal · stateful · depends on ${m.up}`); t += 0.02
    for (const [c, txt] of setupSteps(m)) { push(t, m, 'setup', 1, c, txt); t += SET }
    for (let h = 0; h < HOURS.length; h++) {
      const hour = HOURS[h], tag = hour.replace(':', '')
      const iv = { h, hour, runStart: 0, appliedAt: 0, blockFrom: t, blockUntil: t, x0: t }
      const upApplied = upIvals[h].appliedAt
      push(t, m, hour, 1, 'gate', `contract: checking 1 gated upstream for window ${hour}`); t += ISTEP
      if (t < upApplied) {
        push(t, m, hour, 2, 'gate', `contract: ${hour} BLOCKED by ${m.up}`); t += ISTEP
        push(t, m, hour, 2, 'sblock', `state: marked blocked ${hour} — STATE_002 (${m.up})`); t += ISTEP
        iv.blockUntil = upApplied
        t = Math.max(t, upApplied)
        push(t, m, hour, 2, 'gate', `contract: ${hour} SATISFIED (all gates open)`); t += ISTEP
      } else {
        push(t, m, hour, 2, 'gate', `contract: ${hour} SATISFIED (all gates open)`); t += ISTEP
      }
      iv.runStart = t
      push(t, m, hour, 1, 'srun', `state: marked running ${hour}`); t += ISTEP
      push(t, m, hour, 2, 'staging', `created staging table z_bollhav_stg.${m.id}_${m.table}_${tag}`); t += ISTEP
      push(t, m, hour, 2, 'data', `wrote ${rows(h)} rows to staging table (append)`); t += ISTEP
      push(t, m, hour, 2, 'data', 'moved data from staging to target (append)'); t += ISTEP
      push(t, m, hour, 2, 'staging', 'drop staging table'); t += ISTEP
      push(t, m, hour, 1, 'sdone', `state: marked applied ${hour}`); iv.appliedAt = t; t += ISTEP
      t += GAP
      ivals.push(iv)
    }
    return { ivals, total: t }
  }

  const R = buildRaw(RAW)
  const C = buildClean(CLEAN, R.ivals)
  const trace = _ev.slice().sort((a, b) => a.t - b.t || a.id - b.id)
  const total = Math.max(R.total, C.total) + 0.5

  // state-table contents per model — the rows only exist once prefill has run
  const ST_NAME = {
    raw: `z_bollhav.${RAW.schema}__${RAW.table}`,
    clean: `z_bollhav.${CLEAN.schema}__${CLEAN.table}`,
  }
  const prefillAt = {
    raw: trace.find((e) => e.model === 'raw' && e.text.includes('prefilled'))?.t ?? 0,
    clean: trace.find((e) => e.model === 'clean' && e.text.includes('prefilled'))?.t ?? 0,
  }

  const BASE = 9 * 3600
  const p2 = (n) => String(n).padStart(2, '0')
  function ts(t) {
    const tot = BASE + t
    return `${p2(Math.floor(tot / 3600) % 24)}:${p2(Math.floor(tot / 60) % 60)}:${p2(Math.floor(tot) % 60)}.${String(Math.floor((t % 1) * 1000)).padStart(3, '0')}`
  }

  function statusRaw(w, c) {
    if (c < w.runStart) return 'pending'
    if (w.errAt != null) {
      if (c < w.errAt) return 'running'
      if (c < w.retryAt) return 'error'
      if (c < w.appliedAt) return 'running'
      return 'applied'
    }
    return c < w.appliedAt ? 'running' : 'applied'
  }
  function statusClean(w, c) {
    if (c >= w.appliedAt) return 'applied'
    if (c >= w.runStart) return 'running'
    if (c >= w.blockFrom && c < w.blockUntil) return 'blocked'
    return 'pending'
  }
  const LANES = [
    { id: 'raw', label: RAW.full, rows: R.ivals, status: statusRaw },
    { id: 'clean', label: CLEAN.full, rows: C.ivals, status: statusClean },
  ]
  const TERMS = [
    { id: 'raw', name: RAW.full, nameClass: 'raw', meta: '', dep: null },
    { id: 'clean', name: CLEAN.full, nameClass: 'clean', meta: ' · depends on ', dep: CLEAN.up },
  ]

  // ── runtime state (runes) ──────────────────────────────────────────────
  let clock = $state(0)
  let paused = $state(true)
  let speed = $state(0.1)
  let bodyEls = []
  let openData = $state([false, false])  // data tables collapsed by default
  let openState = $state([false, false]) // state tables collapsed by default
  let tablesShown = $state(false)
  function toggleTables() {
    tablesShown = !tablesShown
    openData = [tablesShown, tablesShown]
    openState = [tablesShown, tablesShown]
  }

  const shown = $derived(trace.filter((e) => e.t <= clock))
  const byModel = $derived({
    raw: shown.filter((e) => e.model === 'raw' && e.cat !== 'header'),
    clean: shown.filter((e) => e.model === 'clean' && e.cat !== 'header'),
  })
  $effect(() => {
    byModel.raw.length; byModel.clean.length
    for (const el of bodyEls) if (el) el.scrollTop = el.scrollHeight
  })
  $effect(() => {
    let raf, last = null
    const tick = (tsf) => {
      if (last == null) last = tsf
      const dt = (tsf - last) / 1000
      last = tsf
      if (!paused && clock < total) clock = Math.min(total, clock + dt * speed)
      raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  })
  const restart = () => { clock = 0; paused = true }
  const EPS = 1e-6
  function stepNext() {
    paused = true
    const next = trace.find((e) => e.t > clock + EPS)
    clock = next ? next.t : total
  }
  function stepPrev() {
    paused = true
    let prev = 0
    for (const e of trace) { if (e.t < clock - EPS) prev = e.t; else break }
    clock = prev
  }

  const LEGEND = [
    ['ddl', 'DDL'], ['staging', 'staging'], ['srun', 'state'], ['data', 'data'], ['gate', 'gate'], ['error', 'error'],
  ]
</script>

<div class="wrap">
  <header>
    <div>
      <h1>bollhav <span class="accent">lab</span></h1>
      <p class="tag">An interactive example to see how bollhav actually works</p>
      <p class="tldr"><strong>TL;DR:</strong> it does DDL work + moves data + keeps track of what has moved by storing state</p>
    </div>
  </header>

  <div class="controls">
    <div class="ctl-row">
      <button onclick={() => (paused = !paused)}>{paused ? '▶ play' : '⏸ pause'}</button>
      <button onclick={stepPrev} title="previous event">⏮ prev step</button>
      <button onclick={stepNext} title="next event">⏭ next step</button>
      <button onclick={restart}>↻ restart</button>
      <button onclick={toggleTables}>{tablesShown ? '⊟ minimize tables' : '⊞ maximize tables'}</button>
    </div>
    <label class="speed">
      <input type="range" min="0.1" max="0.5" step="0.1" bind:value={speed} />
      <span>{speed}×</span>
    </label>
  </div>

  <!-- interval mini-map -->
  <div class="map">
    <div class="playhead" style="left:{(clock / total) * 100}%"></div>
    {#each LANES as lane (lane.id)}
      <div class="lane">
        <div class="lane-name {lane.id}">{lane.label}</div>
        <div class="track">
          {#each lane.rows as w (w.h)}
            <div class="cell {lane.status(w, clock)}"
                 style="left:{(w.x0 / total) * 100}%; width:{Math.max(1.3, ((w.appliedAt - w.x0) / total) * 100)}%">
              {w.hour}
            </div>
          {/each}
        </div>
      </div>
    {/each}
  </div>

  <p class="scenario">
    What this shows: <strong>two separate processes</strong> loading models that depend on each other.
    <span class="mname clean">source_system_clean.orders</span> stays blocked until
    <span class="mname raw">source_system_raw.orders</span> has applied each window — they coordinate
    through shared state, never talking to each other directly.
  </p>

  <!-- two terminals, stacked -->
  <div class="terminals">
    {#each TERMS as term, i (term.id)}
      {@const list = byModel[term.id]}
      {@const lane = LANES[i]}
      <div class="model-row">

        <!-- data table (the target) — left -->
        <div class="side data" class:open={openData[i]}>
          <button class="side-toggle" onclick={() => (openData[i] = !openData[i])} title="show/hide the data table">
            <span class="side-label {term.nameClass}">{openData[i] ? '▾ data table' : 'data table'}</span>
          </button>
          {#if openData[i]}
            {@const applied = lane.rows.filter((w) => lane.status(w, clock) === 'applied')}
            <div class="side-body">
              <div class="side-name {term.nameClass}">{term.name}</div>
              {#if applied.length}
                <table class="dt">
                  <tbody>
                    {#each applied as w (w.h)}
                      <tr><td class="dt-win">{w.hour}→{p2(w.h + 1)}:00</td><td class="dt-rows">{rows(w.h).toLocaleString()}</td></tr>
                    {/each}
                    <tr class="dt-total"><td>total</td><td class="dt-rows">{applied.reduce((s, w) => s + rows(w.h), 0).toLocaleString()}</td></tr>
                  </tbody>
                </table>
              {:else}
                <div class="side-empty">table empty — no windows applied yet</div>
              {/if}
            </div>
          {/if}
        </div>

        <!-- terminal — middle -->
        <div class="term m-{term.id}">
          <div class="term-head">
            <i class="tdot"></i>
            <span class="mname {term.nameClass}">{term.name}</span><span class="hmeta">{term.meta}</span>{#if term.dep}<span class="mname raw">{term.dep}</span>{/if}
          </div>
          <div class="term-body" bind:this={bodyEls[i]}>
            {#each list as e (e.id)}
              <div class="row {e.cat} i{e.indent}" class:cur={e === list.at(-1)}>
                <span class="ts">{ts(e.t)}</span>
                <i class="dot {e.cat}"></i>
                <span class="txt">{e.text}</span>
              </div>
            {/each}
          </div>
        </div>

        <!-- state table — right -->
        <div class="side state" class:open={openState[i]}>
          <button class="side-toggle" onclick={() => (openState[i] = !openState[i])} title="show/hide the state table">
            <span class="side-label">{openState[i] ? '▾ state table' : 'state table'}</span>
          </button>
          {#if openState[i]}
            <div class="side-body">
              <div class="side-name">{ST_NAME[term.id]}</div>
              {#if clock >= prefillAt[term.id]}
                <table class="st-table">
                  <tbody>
                    {#each lane.rows as w (w.h)}
                      {@const s = lane.status(w, clock)}
                      <tr><td class="st-win">{w.hour}→{p2(w.h + 1)}:00</td><td class="st-cell"><span class="sb {s}">{s}</span></td></tr>
                    {/each}
                  </tbody>
                </table>
              {:else}
                <div class="side-empty">empty — not prefilled yet</div>
              {/if}
            </div>
          {/if}
        </div>

      </div>
    {/each}
  </div>

  <!-- overall progress — drag to scrub -->
  <div class="progress" title="drag to scrub through the run">
    <div class="bar" style="width:{(clock / total) * 100}%"></div>
    <div class="knob" style="left:{(clock / total) * 100}%"></div>
    <input class="scrub" type="range" min="0" max={total} step="0.01" bind:value={clock}
           oninput={() => (paused = true)} aria-label="seek through run" />
  </div>

  <p class="note">
    Pacing is illustrative and does <strong>not</strong> reflect real run speed — actual runs may be far
    faster or far slower, and timing depends on data volume, the database, and worker parallelism.
  </p>

  <div class="legend">
    {#each LEGEND as [cls, label] (cls)}
      <span class="chip"><i class="dot {cls}"></i>{label}</span>
    {/each}
  </div>
</div>

<style>
  .wrap { max-width: 1140px; margin: 1.8rem auto; padding: 0 1rem; font-family: ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif; color: #c7d2e2; }
  header { display: flex; flex-direction: column; align-items: center; text-align: center; gap: 0.55rem; }
  h1 { margin: 0; font-size: 1.5rem; font-weight: 700; color: #e8eef7; letter-spacing: -0.01em; }
  .accent { color: #e8833a; }
  .tag { margin: 0.25rem 0 0; font-size: 0.83rem; color: #8b97a8; }
  .tldr { margin: 0.15rem 0 0; font-size: 0.8rem; color: #8b97a8; }
  .tldr strong { color: #c7d2e2; }

  .legend { display: flex; justify-content: center; gap: 0.8rem; flex-wrap: wrap; margin-top: 1.1rem; padding-top: 0.9rem; border-top: 1px solid #1c2636; }
  .chip { display: inline-flex; align-items: center; gap: 0.3rem; font-size: 0.74rem; color: #9aa6b8; }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; flex: none; }

  .controls { display: flex; flex-direction: column; align-items: center; gap: 0.55rem; margin: 1rem 0 0.7rem; }
  .ctl-row { display: flex; align-items: center; justify-content: center; gap: 0.5rem; flex-wrap: wrap; }
  button { background: #1a2434; color: #c7d2e2; border: 1px solid #283449; border-radius: 7px; padding: 0.32rem 0.65rem; font-size: 0.8rem; cursor: pointer; }
  button:hover { border-color: #3a4a63; }
  .speed { display: inline-flex; align-items: center; gap: 0.45rem; font-size: 0.78rem; color: #9aa6b8; }
  .speed input[type='range'] { width: 120px; accent-color: #e8833a; cursor: pointer; }
  .speed span { font-variant-numeric: tabular-nums; min-width: 2.6rem; }

  .map { position: relative; padding: 0.3rem 0 0.5rem; }
  .playhead { position: absolute; top: 0.2rem; bottom: 0.4rem; width: 2px; background: #34d3e0; box-shadow: 0 0 10px #34d3e0aa; z-index: 3; pointer-events: none; }
  .lane { margin: 0.4rem 0; }
  .lane-name { font-size: 0.76rem; font-weight: 600; color: #c7d2e2; font-family: ui-monospace, Menlo, monospace; margin-bottom: 0.2rem; text-align: center; }
  .track { position: relative; height: 26px; background: #111a28; border: 1px solid #1c2636; border-radius: 6px; }
  .cell { position: absolute; top: 3px; bottom: 3px; border-radius: 4px; display: flex; align-items: center; justify-content: center; font-size: 0.7rem; font-weight: 700; overflow: hidden; transition: background-color 0.18s ease, color 0.18s ease; }

  /* two terminals, stacked */
  .terminals { display: grid; grid-template-columns: 1fr; gap: 0.7rem; margin-top: 0.5rem; }
  .term { background: #0b121d; border: 1px solid #1c2636; border-radius: 9px; overflow: hidden; }
  .term-head { display: flex; align-items: center; justify-content: center; gap: 0.45rem; padding: 0.5rem 0.7rem; font-family: ui-monospace, Menlo, monospace; font-size: 0.74rem; font-weight: 700; color: #d7e0ec; border-bottom: 1px solid #1c2636; background: #0e1622; }
  .tdot { width: 9px; height: 9px; border-radius: 50%; flex: none; }
  .m-raw .tdot { background: #74bdf2; }
  .m-clean .tdot { background: #b884f0; }
  .mname.raw, .lane-name.raw { color: #74bdf2; }       /* raw model — blue */
  .mname.clean, .lane-name.clean { color: #b884f0; }   /* clean model — purple */
  .hmeta { color: #8b97a8; font-weight: 400; }
  .term-body { height: 15.5rem; overflow: auto; padding: 0.4rem 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.72rem; line-height: 1.5; }
  .row { display: flex; align-items: center; gap: 0.4rem; padding: 0.04rem 0.55rem; white-space: nowrap; border-left: 2px solid transparent; }
  .row.cur { background: #16202f; border-left-color: #34d3e0; }
  .row.i2 .dot { margin-left: 0.9rem; }
  .ts { flex: none; color: #57647a; font-variant-numeric: tabular-nums; }
  .txt { color: #9aa6b8; }

  /* model row: data table (left) · terminal (middle) · state table (right) */
  .model-row { display: flex; gap: 0.6rem; align-items: stretch; }
  .model-row .term { flex: 1 1 auto; min-width: 0; }
  .side { flex: 0 0 auto; display: flex; flex-direction: column; background: #0b121d; border: 1px solid #1c2636; border-radius: 9px; overflow: hidden; }
  .side.open { flex: 0 0 212px; }
  .side-toggle { background: #0e1622; color: #9aa6b8; border: none; cursor: pointer; font-family: ui-monospace, Menlo, monospace; font-size: 0.72rem; font-weight: 700; padding: 0; }
  .side:not(.open) .side-toggle { flex: 1; writing-mode: vertical-rl; text-orientation: mixed; padding: 0.6rem 0.4rem; letter-spacing: 0.02em; }
  .side.open .side-toggle { border-bottom: 1px solid #1c2636; padding: 0.45rem 0.6rem; text-align: left; }
  .side-toggle:hover { color: #d7e0ec; background: #14202f; }
  .side-label.raw { color: #74bdf2; }
  .side-label.clean { color: #b884f0; }
  .side-body { padding: 0.45rem 0.55rem 0.6rem; overflow: auto; }
  .side-name { font-family: ui-monospace, Menlo, monospace; font-size: 0.63rem; color: #6b7891; margin-bottom: 0.4rem; word-break: break-all; }
  .side-name.raw { color: #74bdf2; }
  .side-name.clean { color: #b884f0; }
  .side-empty { font-size: 0.65rem; color: #6b7891; font-style: italic; padding: 0.4rem 0; }
  .dt, .st-table { width: 100%; border-collapse: collapse; font-family: ui-monospace, Menlo, monospace; font-size: 0.65rem; }
  .dt td, .st-table td { padding: 0.12rem 0.2rem; }
  .dt-win, .st-win { color: #8b97a8; white-space: nowrap; }
  .dt-rows { text-align: right; color: #cbd5e6; font-variant-numeric: tabular-nums; }
  .dt-total td { border-top: 1px solid #1c2636; padding-top: 0.25rem; color: #9aa6b8; }
  .st-cell { text-align: right; }
  .sb { display: inline-block; padding: 0.03rem 0.4rem; border-radius: 4px; font-size: 0.62rem; font-weight: 700; }
  .sb.pending { background: #2f3a4d; color: #aeb9ca; }
  .sb.running { background: #f0a55c; color: #2a1707; }
  .sb.applied { background: #54c187; color: #062012; }
  .sb.blocked { background: #a78be6; color: #190b32; }
  .sb.error   { background: #e26d68; color: #2c0605; }

  /* status palette — mini-map bars + the windows tally */
  .cell.pending { background: #38445a; color: #c2cdde; }
  .cell.running { background: #f0a55c; color: #2a1707; }
  .cell.applied { background: #54c187; color: #062012; }
  .cell.blocked { background: #a78be6; color: #190b32; }
  .cell.error   { background: #e26d68; color: #2c0605; }

  /* category palette — the trace lines */
  .dot.ddl     { background: #4fb87e; }   /* green */
  .dot.staging { background: #d9c24e; }   /* gold */
  .dot.srun    { background: #f4a24e; }   /* state — brightest orange (running) */
  .dot.sdone   { background: #e07b2f; }   /* state — mid orange (applied) */
  .dot.sblock  { background: #bf6a26; }   /* state — deep orange (blocked) */
  .dot.state   { background: #d39c66; }   /* state — muted orange (setup/library) */
  .dot.data    { background: #e07ba8; }   /* pink/rose */
  .dot.gate    { background: #45d2de; }   /* cyan */
  .dot.error   { background: #ff5d57; }   /* bright red — reserved */

  .row.ddl     .txt { color: #84d3a4; }
  .row.staging .txt { color: #e3d07e; }
  .row.srun    .txt { color: #f6b878; }
  .row.sdone   .txt { color: #ef9b5e; }
  .row.sblock  .txt { color: #dd9355; }
  .row.state   .txt { color: #e0bb93; }
  .row.data    .txt { color: #f0a8cf; }
  .row.gate    .txt { color: #86dce6; }
  .row.error   .txt { color: #ff8a85; font-weight: 600; }

  /* overall progress — spans the full terminal width */
  .progress { position: relative; margin-top: 0.7rem; height: 11px; background: #111a28; border: 1px solid #1c2636; border-radius: 999px; cursor: pointer; }
  .progress .bar { height: 100%; background: linear-gradient(90deg, #34d3e0, #4fb87e); border-radius: 999px; transition: width 0.04s linear; pointer-events: none; }
  .progress .knob { position: absolute; top: 50%; width: 15px; height: 15px; margin-left: -7.5px; transform: translateY(-50%); border-radius: 50%; background: #d7f4f8; box-shadow: 0 0 0 2px #0c1320, 0 0 6px #34d3e0aa; pointer-events: none; transition: left 0.04s linear; }
  .scrub { position: absolute; top: -6px; left: 0; width: 100%; height: calc(100% + 12px); margin: 0; opacity: 0; cursor: pointer; }
  .scenario { margin: 1rem auto 0.3rem; max-width: 780px; text-align: center; font-size: 0.84rem; line-height: 1.55; color: #9aa6b8; }
  .scenario strong { color: #d7e0ec; }
  .note { margin: 0.6rem 0 0; font-size: 0.74rem; color: #6f7c90; line-height: 1.5; font-style: italic; text-align: center; }
  .note strong { color: #9aa6b8; font-style: normal; }
</style>
