<script>
  // State resets — the operator's "redo this". The chosen intervals flip
  // applied → pending so the next run reprocesses them; rows and history are
  // kept, and a running interval is never touched (see
  // bollhav.postgres.state.write). Shown in the side panels: the interval
  // side gets "reset selected" for the picked runs (`intervals`), the model
  // side (`model`) gets a typed range and the whole model. Every action asks
  // first. `sample` is any shown interval's ISO `since`, so a typed range
  // carries the same offset as what's on screen.
  import { resetState } from "../lib/api.js";
  import { ts } from "../lib/constants.js";

  // `span` ({since, until}) prefills the range — the gaps tab's clicked period
  let { name, intervals = [], model = false, span = null, sample = "", ondone = () => {} } = $props();

  let confirming = $state(null); // {label, body}
  let msg = $state(null); // {ok, text} — the last answer
  let rangeFrom = $state("");
  let rangeTo = $state("");

  // the range box follows the picked intervals (earliest since → latest
  // until); a new pick also drops a pending question
  $effect(() => {
    const w = intervals.filter((r) => r.since);
    if (span) {
      rangeFrom = ts(span.since);
      rangeTo = ts(span.until);
    } else if (w.length) {
      rangeFrom = ts(w.map((r) => r.since).sort()[0]);
      rangeTo = ts(w.map((r) => r.until).sort().at(-1));
    }
    confirming = null;
  });

  function shortName(full) {
    return (full || "").split(".").slice(-2).join(".");
  }

  // a typed bound ("2026-01-01", "2026-01-01 13:00") as an ISO timestamp with
  // the offset the shown intervals carry, so what you type matches what you see
  function bound(s) {
    const off = (sample || "").match(/([+-]\d\d:\d\d|Z)$/)?.[1] || "+00:00";
    let t = (s || "").trim().replace(" ", "T");
    if (/^\d{4}-\d\d-\d\d$/.test(t)) t += "T00:00";
    if (/^\d{4}-\d\d-\d\dT\d\d:\d\d$/.test(t)) t += ":00";
    return /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$/.test(t) ? t + off : null;
  }
  let rangeOk = $derived(!!(bound(rangeFrom) && bound(rangeTo)));

  // `what` says what is reset ("the whole model", "3 selected intervals",
  // "every interval in"); `detail` is an optional line of its own (the range)
  function ask(what, body, detail = "") {
    confirming = { what, detail, body };
    msg = null;
  }

  async function go() {
    const { what, detail, body } = confirming;
    const label = detail ? `${what} ${detail}` : what;
    confirming = null;
    try {
      const r = await resetState(name, body);
      msg = { ok: true, text: `reset ${r.reset} interval${r.reset === 1 ? "" : "s"} — ${label}` };
      ondone(r);
    } catch (e) {
      msg = { ok: false, text: `not reset — ${e.message}` };
    }
  }
</script>

<div class="reset">
  <div class="title">reset state</div>
  <div class="hint">
    marks intervals pending so the next run redoes them; a running interval is left alone
  </div>
  {#if intervals.length}
    <button
      class="rbtn act"
      onclick={() =>
        ask(`${intervals.length} selected interval${intervals.length === 1 ? "" : "s"}`, {
          intervals: intervals.map((r) => ({ since: r.since, until: r.until })),
        })}>reset selected ({intervals.length})</button
    >
  {/if}
  {#if model}
    <!-- from · to on one row, the button under them -->
    <div class="range">
      <input class="rin" placeholder="from 2026-01-01 00:00" bind:value={rangeFrom} />
      <input class="rin" placeholder="to 2026-02-01 00:00" bind:value={rangeTo} />
    </div>
    <button
      class="rbtn act"
      disabled={!rangeOk}
      onclick={() =>
        ask(
          "every interval in",
          { range: { since: bound(rangeFrom), until: bound(rangeTo) } },
          `${rangeFrom.trim()} → ${rangeTo.trim()}`,
        )}>reset range</button
    >
    <button class="rbtn act" onclick={() => ask("the whole model", { all: true })}>reset whole model</button>
  {/if}
  {#if confirming}
    <div class="confirm">
      <div class="q">Are you sure?</div>
      <div>This resets {confirming.what}</div>
      {#if confirming.detail}
        <div class="mono">{confirming.detail}</div>
      {/if}
      <!-- the model, one dotted segment per line (catalog / schema / table) -->
      <div class="who">
        {#each (name || "").split(".") as seg, i}
          <div>{seg}{i < name.split(".").length - 1 ? "." : ""}</div>
        {/each}
      </div>
      <div class="btns">
        <button class="rbtn danger" onclick={go}>yes, reset</button>
        <button class="rbtn" onclick={() => (confirming = null)}>no</button>
      </div>
    </div>
  {/if}
  {#if msg}
    <div class="msg" class:err={!msg.ok}>{msg.text}</div>
  {/if}
</div>

<style>
  .reset {
    margin-top: 16px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
  }
  .title {
    font-family: var(--box-title-font);
    font-size: var(--box-title-size);
    font-weight: var(--box-title-weight);
  }
  .hint {
    font-size: 12px;
    color: var(--muted);
    font-style: italic;
  }
  .rbtn {
    font-family: var(--btn-font);
    font-size: 15px;
    font-weight: var(--btn-weight);
    padding: 6px 12px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
    white-space: nowrap;
  }
  .rbtn:disabled {
    opacity: 0.5;
    cursor: default;
  }
  /* danger zone: the buttons that start a reset — wine red, hatched / / / / */
  .rbtn.act {
    color: #8b2332;
    border-color: #8b2332;
    font-weight: 600;
    background:
      repeating-linear-gradient(
        45deg,
        rgba(139, 35, 50, 0.16) 0 5px,
        transparent 5px 11px
      ),
      var(--control-bg);
  }
  .rbtn.act:hover:not(:disabled) {
    background:
      repeating-linear-gradient(
        45deg,
        rgba(139, 35, 50, 0.28) 0 5px,
        transparent 5px 11px
      ),
      var(--control-bg);
  }
  /* the one that does it: the site's wine red */
  .rbtn.danger {
    background: #8b2332;
    border-color: #8b2332;
    color: #fff;
  }
  /* from · to on one row, sharing the width */
  .range {
    display: flex;
    align-items: center;
    gap: 6px;
    align-self: stretch;
  }
  .rin {
    font-family: var(--font-mono);
    font-size: 13px;
    padding: 5px 8px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--input-bg);
    color: var(--control-fg);
    flex: 1 1 0;
    min-width: 0;
  }
  /* the question, one line each: what, the range if any, the model, the buttons */
  .confirm {
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 8px 10px;
    border: 1px solid #8b2332;
    border-radius: 8px;
    font-size: 14px;
    align-self: stretch;
  }
  .confirm .q {
    font-weight: 600;
  }
  .confirm .mono {
    font-family: var(--font-mono);
  }
  .confirm .who {
    font-family: var(--name-font);
    word-break: break-all;
  }
  .confirm .btns {
    display: flex;
    gap: 8px;
    margin-top: 6px;
  }
  .msg {
    font-size: 14px;
  }
  .msg.err {
    color: var(--err-msg);
  }
</style>
