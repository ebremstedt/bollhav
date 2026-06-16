<script>
  import { info } from "../lib/selection.svelte.js";
  import { view } from "../lib/view.svelte.js";
  import { getModelMeta } from "../lib/api.js";

  // tier-1 (upstream / sources / last_seen) is already on the graph node;
  // tier-2 (the property bag) is fetched from /model/{name}.
  let node = $derived(view.full?.nodes.find((n) => n.name === info.name) || null);
  let isView = $derived(node?.model_type === "VIEW");
  let meta = $state(null);
  let loading = $state(false);
  // tags + columns are collapsed by default — expand on click.
  let showTags = $state(false);
  let showCols = $state(false);

  $effect(() => {
    const name = info.name;
    meta = null;
    if (!name) return;
    loading = true;
    getModelMeta(name)
      .then((d) => (meta = d))
      .catch(() => (meta = null))
      .finally(() => (loading = false));
  });

  let parts = $derived((info.name || "").split("."));
  let cat = $derived(parts.length >= 3 ? parts[parts.length - 3] : null);
  let sch = $derived(parts.length >= 2 ? parts[parts.length - 2] : null);
  let tbl = $derived(parts[parts.length - 1]);

  function fmtTs(iso) {
    return iso ? iso.replace("T", " ").slice(0, 19) : "—";
  }
  // A column's type string: "type", "type(len)", or "type(p,s)".
  function colType(c) {
    let t = c.type || "?";
    if (c.length != null) t += `(${c.length})`;
    else if (c.precision != null)
      t += `(${c.precision}${c.scale != null ? "," + c.scale : ""})`;
    return t;
  }
  // Stack a dotted tag onto one line per segment (each non-final keeps its
  // trailing dot), rendered with `white-space: pre-line` so it HARD-breaks at
  // every dot inside the one chip. Single-word tags stay on one line.
  const stackDots = (s) =>
    s
      .split(".")
      .map((seg, i, a) => (i < a.length - 1 ? seg + "." : seg))
      .join("\n");

  let cols = $derived(meta?.columns ?? []);
  let metaTags = $derived(meta?.tags ?? []);
  let hasMeta = $derived(meta && Object.keys(meta).length > 0);
  // Prefer the typed upstream specs (name + contract + freshness) from the
  // metadata bag; fall back to bare names off the graph node.
  let upstreamSpecs = $derived(meta?.upstreams ?? null);
  let upstream = $derived(node?.upstream ?? []);
  let sources = $derived(node?.sources ?? []);

  // Split a dotted name into colour-coded parts: catalog (dark blue),
  // schema (blue), table (light blue). Non-final segments keep their dot.
  function nameParts(full) {
    const parts = String(full).split(".");
    if (parts.length >= 3)
      return [
        { t: parts[0] + ".", c: "cat" },
        { t: parts[1] + ".", c: "sch" },
        { t: parts.slice(2).join("."), c: "tbl" },
      ];
    if (parts.length === 2)
      return [
        { t: parts[0] + ".", c: "sch" },
        { t: parts[1], c: "tbl" },
      ];
    return [{ t: parts[0], c: "tbl" }];
  }

  // seconds -> "1d" / "6h" / "30m" for a freshness window
  function fmtDur(secs) {
    if (secs == null) return "";
    const d = secs / 86400;
    if (d >= 1) return `${+d.toFixed(d % 1 ? 1 : 0)}d`;
    const h = secs / 3600;
    if (h >= 1) return `${+h.toFixed(h % 1 ? 1 : 0)}h`;
    const m = secs / 60;
    if (m >= 1) return `${+m.toFixed(0)}m`;
    return `${secs}s`;
  }
  function freshLabel(f) {
    if (!f) return "";
    return `❄ ≤${fmtDur(f.within_seconds)} ${f.scope}`;
  }
</script>

{#snippet fqn(name)}{#each nameParts(name) as p}<span class={p.c}>{p.t}</span
    >{/each}{/snippet}

<aside class="meta-panel">
  <div class="head">
    <span class="title">{tbl}</span>
    <button class="x" onclick={() => (info.name = null)}>✕</button>
  </div>

  <div class="fqn stack">{@render fqn(info.name)}</div>

  {#if hasMeta && meta.description}
    <div class="desc">{meta.description}</div>
  {/if}

  <div class="row">
    <span>temporality</span>
    <b>{node?.kind ?? "—"}{node?.model_type ? ` · ${node.model_type}` : ""}</b>
  </div>

  {#if hasMeta}
    {#if !isView}
      <div class="row">
        <span>write mode</span>
        <b>{meta.write_mode || "—"}{meta.staging ? " · staged" : ""}</b>
      </div>
    {/if}
    {#if meta.batching}
      <div class="row">
        <span>batching</span>
        <b>{meta.batching.chunk} · {meta.batching.size} rows</b>
      </div>
    {/if}
    {#if meta.contract && (meta.contract.begin || meta.contract.end)}
      <div class="row">
        <span>contract</span>
        <b>{fmtTs(meta.contract.begin)} → {fmtTs(meta.contract.end)}</b>
      </div>
    {/if}
    {#if meta.partitioned_by}
      <div class="row"><span>partition</span><b>{meta.partitioned_by}</b></div>
    {/if}
    {#if meta.enabled === false}
      <div class="row"><span>enabled</span><b>false</b></div>
    {/if}
  {/if}

  <div class="row"><span>last seen</span><b>{fmtTs(node?.last_seen)}</b></div>

  <div class="sec">
    upstream ({upstreamSpecs ? upstreamSpecs.length : upstream.length})
  </div>
  {#if upstreamSpecs && upstreamSpecs.length}
    <ul class="list">
      {#each upstreamSpecs as u}
        <li>
          <span class="up-name stack">{@render fqn(u.name)}</span>{#if u.deactivate_for_dev}
            <em class="devoff">dev→prod</em>{/if}
          <span class="up-badges">
            <span class="contract">{u.contract}</span>
            {#if u.freshness}<span class="fresh">{freshLabel(u.freshness)}</span>{/if}
          </span>
        </li>
      {/each}
    </ul>
  {:else if upstream.length}
    <ul class="list">
      {#each upstream as u}<li><span class="up-name stack">{@render fqn(u)}</span></li>{/each}
    </ul>
  {/if}

  <div class="sec">sources ({sources.length})</div>
  {#if sources.length}
    <ul class="list">
      {#each sources as s}<li>
          <span class="up-name stack">{@render fqn(s.name)}</span>
          <em>({s.kind})</em>
        </li>{/each}
    </ul>
  {/if}

  {#if metaTags.length}
    <button
      class="sec sec-toggle"
      onclick={() => (showTags = !showTags)}
      aria-expanded={showTags}
    >
      <span class="caret">{showTags ? "▾" : "▸"}</span> tags ({metaTags.length})
    </button>
    {#if showTags}
      <div class="tags">
        {#each metaTags as t}
          <span class="chip">{stackDots(t)}</span>
        {/each}
      </div>
    {/if}
  {/if}

  {#if cols.length}
    <button
      class="sec sec-toggle"
      onclick={() => (showCols = !showCols)}
      aria-expanded={showCols}
    >
      <span class="caret">{showCols ? "▾" : "▸"}</span> columns ({cols.length})
    </button>
    {#if showCols}
      <table class="cols-table">
        <tbody>
          {#each cols as c}
            <tr>
              <td class="cname">{c.name}</td>
              <td class="ctype">{colType(c)}</td>
              <td class="cflag"
                >{c.primary_key ? "PK" : c.unique ? "UQ" : ""}</td
              >
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  {/if}

  {#if loading}
    <div class="foot">loading details…</div>
  {:else if meta && !hasMeta}
    <div class="foot">no stored metadata — re-run the pipeline to populate</div>
  {/if}
</aside>

<style>
  .meta-panel {
    width: 320px;
    flex: 0 0 320px;
    border-right: 1px solid var(--border);
    background: var(--bg);
    color: var(--fg);
    overflow-y: auto;
    padding: 12px 14px;
    box-sizing: border-box;
    font-size: 12px;
  }
  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }
  .title {
    font-weight: 700;
    font-size: 14px;
    word-break: break-word;
  }
  .x {
    border: none;
    background: transparent;
    color: var(--muted, #888);
    cursor: pointer;
    font-size: 14px;
    line-height: 1;
  }
  .x:hover {
    color: var(--fg);
  }
  .fqn {
    margin: 2px 0 8px;
    font-size: 11px;
    word-break: break-word;
  }
  .dim {
    color: var(--muted, #888);
  }
  /* dotted-name colour ramp: catalog (dark blue) · schema (blue) · table
     (light blue), used everywhere a name shows in this panel. */
  .cat {
    color: #2563eb;
  }
  .sch {
    color: #3b82f6;
  }
  .tbl {
    color: #60a5fa;
  }
  /* a `.stack`ed name puts each dotted segment on its own line */
  .stack :global(span),
  .stack span {
    display: block;
  }
  .desc {
    font-style: italic;
    color: var(--muted, #888);
    margin-bottom: 8px;
  }
  .row {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 2px 0;
  }
  .row span {
    color: var(--muted, #888);
  }
  .row b {
    text-align: right;
    word-break: break-word;
  }
  .sec {
    margin: 9px 0 3px;
    font-weight: 700;
    border-top: 1px solid var(--border);
    padding-top: 7px;
  }
  .list {
    margin: 0;
    padding-left: 16px;
    list-style: disc;
  }
  .list li {
    word-break: break-word;
    padding: 1px 0;
  }
  .list em {
    color: var(--muted, #888);
    font-style: normal;
  }
  /* upstream name stacked one dotted segment per line (catalog. / schema. /
     table), each non-final segment keeping its trailing dot. */
  .up-name {
    white-space: pre-line;
    overflow-wrap: anywhere;
  }
  /* contract level + freshness chips stacked under the upstream name —
     freshness sits beneath the contract, not beside it. */
  .up-badges {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 3px;
    margin: 3px 0 5px 2px;
  }
  .contract {
    font-size: 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    padding: 1px 5px;
    border-radius: 8px;
    background: #2b2f36;
    color: #ffd23f;
  }
  .fresh {
    font-size: 9px;
    font-weight: 600;
    padding: 1px 5px;
    border-radius: 8px;
    background: #16313a;
    color: #4aa3ff;
    white-space: nowrap;
  }
  .devoff {
    font-size: 9px;
    color: var(--muted, #888);
    margin-left: 4px;
  }
  /* a collapsible section header (tags / columns) — looks like .sec but is a
     button with a caret. */
  .sec-toggle {
    display: flex;
    align-items: center;
    gap: 6px;
    width: 100%;
    text-align: left;
    background: transparent;
    border: none;
    border-top: 1px solid var(--border);
    color: var(--fg);
    font: inherit;
    font-weight: 700;
    cursor: pointer;
  }
  .caret {
    color: var(--muted, #888);
    font-size: 10px;
  }
  /* columns rendered as a compact table */
  .cols-table {
    width: 100%;
    border-collapse: collapse;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
    margin-top: 3px;
  }
  .cols-table td {
    padding: 2px 6px 2px 0;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  .cols-table .cname {
    word-break: break-word;
  }
  .cols-table .ctype {
    color: var(--muted, #888);
    white-space: nowrap;
  }
  .cols-table .cflag {
    color: #ffd23f;
    font-weight: 700;
    text-align: right;
    white-space: nowrap;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
  }
  /* a dotted tag hard-breaks onto one line per segment (white-space: pre-line),
     shown in yellow, staying inside the panel */
  .chip {
    font-size: 10px;
    padding: 2px 7px;
    border-radius: 9px;
    border: 1px solid var(--control-border);
    background: var(--node-bg, var(--bg));
    color: #ffc107;
    max-width: 100%;
    white-space: pre-line;
    overflow-wrap: anywhere;
    word-break: break-word;
  }
  .foot {
    margin-top: 8px;
    color: var(--muted, #888);
    font-style: italic;
  }
</style>
