<script>
  import { info } from "../lib/selection.svelte.js";
  import { view } from "../lib/view.svelte.js";
  import { getModelMeta } from "../lib/api.js";

  // tier-1 (upstream / sources / last_seen) is already on the graph node;
  // tier-2 (the property bag) is fetched from /model/{name}.
  let node = $derived(view.full?.nodes.find((n) => n.name === info.name) || null);
  let meta = $state(null);
  let loading = $state(false);

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
  function colLabel(c) {
    let t = c.type || "?";
    if (c.length != null) t += `(${c.length})`;
    else if (c.precision != null)
      t += `(${c.precision}${c.scale != null ? "," + c.scale : ""})`;
    const marks =
      (c.primary_key ? " PK" : "") + (c.unique && !c.primary_key ? " UQ" : "");
    return `${c.name} : ${t}${marks}`;
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
  let upstream = $derived(node?.upstream ?? []);
  let sources = $derived(node?.sources ?? []);
</script>

<aside class="meta-panel">
  <div class="head">
    <span class="title">{tbl}</span>
    <button class="x" onclick={() => (info.name = null)}>✕</button>
  </div>

  <div class="fqn">
    {#if cat}<span class="dim">{cat}.</span>{/if}{#if sch}<span class="dim"
        >{sch}.</span
      >{/if}{tbl}
  </div>

  {#if hasMeta && meta.description}
    <div class="desc">{meta.description}</div>
  {/if}

  <div class="row">
    <span>kind</span>
    <b>{node?.kind ?? "—"}{node?.model_type ? ` · ${node.model_type}` : ""}</b>
  </div>

  {#if hasMeta}
    <div class="row">
      <span>write mode</span>
      <b>{meta.write_mode || "—"}{meta.staging ? " · staged" : ""}</b>
    </div>
    {#if meta.batching}
      <div class="row">
        <span>batching</span>
        <b>{meta.batching.chunk} · {meta.batching.size} rows</b>
      </div>
    {/if}
    {#if meta.bounds && (meta.bounds.begin || meta.bounds.end)}
      <div class="row">
        <span>bounds</span>
        <b>{fmtTs(meta.bounds.begin)} → {fmtTs(meta.bounds.end)}</b>
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

  <div class="sec">upstream ({upstream.length})</div>
  {#if upstream.length}
    <ul class="list">
      {#each upstream as u}<li>{u}</li>{/each}
    </ul>
  {/if}

  <div class="sec">sources ({sources.length})</div>
  {#if sources.length}
    <ul class="list">
      {#each sources as s}<li>{s.name} <em>({s.kind})</em></li>{/each}
    </ul>
  {/if}

  {#if metaTags.length}
    <div class="sec">tags ({metaTags.length})</div>
    <div class="tags">
      {#each metaTags as t}
        <span class="chip">{stackDots(t)}</span>
      {/each}
    </div>
  {/if}

  {#if cols.length}
    <div class="sec">columns ({cols.length})</div>
    <ul class="list cols">
      {#each cols as c}<li>{colLabel(c)}</li>{/each}
    </ul>
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
  .cols li {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
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
