<script>
  import { view } from "../lib/view.svelte.js";
  import { getModelMeta } from "../lib/api.js";

  // A run-independent model browser: the model registry (left, env-aware) and a
  // model's full stored bollhav metadata (right). Lineage/position lives on the
  // Lineage tab; this is the model's own properties.

  let selected = $state(null); // full_name
  let meta = $state(null); // the /model property bag for `selected`
  let loading = $state(false);

  let models = $derived((view.full?.nodes || []).filter((n) => n.type === "model"));
  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);

  // sort by identity level — full name / schema.table / table — with an
  // asc·desc toggle. The level also sets how much of the name each row shows.
  const SORT_KEYS = [
    ["full", "catalog.schema.table"],
    ["schematable", "schema.table"],
    ["table", "table"],
  ];
  let sortKey = $state("table");
  let sortDir = $state("asc");
  function nameAt(full, key) {
    const p = (full || "").split(".");
    if (key === "table") return p.slice(-1).join(".");
    if (key === "schematable") return p.slice(-2).join(".");
    return full;
  }

  let filtered = $derived.by(() => {
    const q = view.query.trim().toLowerCase();
    let list = models.slice();
    if (tagMatchSet) list = list.filter((m) => tagMatchSet.has(m.name));
    if (q) list = list.filter((m) => m.name.toLowerCase().includes(q));
    const sign = sortDir === "asc" ? 1 : -1;
    list.sort((a, b) => {
      const c = nameAt(a.name, sortKey)
        .toLowerCase()
        .localeCompare(nameAt(b.name, sortKey).toLowerCase());
      return (c || a.name.localeCompare(b.name)) * sign;
    });
    return list;
  });

  $effect(() => {
    void view.env;
    const list = filtered;
    if (list.length && (!selected || !list.some((m) => m.name === selected))) {
      selected = list[0].name;
    }
  });

  let node = $derived(models.find((m) => m.name === selected) || null);

  $effect(() => {
    const name = selected;
    void view.env;
    void view.refreshAt;
    meta = null;
    if (!name) return;
    loading = true;
    getModelMeta(name)
      .then((d) => (meta = d))
      .catch(() => (meta = null))
      .finally(() => (loading = false));
  });

  let hasMeta = $derived(meta && Object.keys(meta).length > 0);
  let cols = $derived(meta?.columns ?? []);

  // a single run-derived hint — the live status of the model (off the graph
  // node, no extra fetch), so a broken model is visible at a glance.
  let health = $derived(
    !node
      ? null
      : node.has_error
        ? { label: "error", c: "#e5202e" }
        : node.has_running
          ? { label: "running", c: "#4c78a8" }
          : node.has_stale
            ? { label: "stale", c: "#ffd23f" }
            : node.has_blocked
              ? { label: "blocked", c: "#f58518" }
              : { label: "ok", c: "#3bbf5b" },
  );

  // the contract window, for the at-a-glance row
  let contractSpan = $derived.by(() => {
    const c = meta?.contract;
    if (!c || !c.begin) return "—";
    const begin = c.begin.slice(0, 10);
    return c.end ? `${begin} → ${c.end.slice(0, 10)}` : `${begin} → ∞`;
  });

  const fmtTs = (iso) => (iso ? iso.replace("T", " ").slice(0, 19) : "—");
  const shortName = (full) => (full || "").split(".").slice(-1)[0];

  function colType(c) {
    let t = c.type || "?";
    if (c.length != null) t += `(${c.length})`;
    else if (c.precision != null)
      t += `(${c.precision}${c.scale != null ? "," + c.scale : ""})`;
    return t;
  }
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
</script>

{#snippet fqn(name)}{#each nameParts(name) as p}<span class={p.c}>{p.t}</span
    >{/each}{/snippet}

{#snippet kv(label, value)}
  <div class="kv"><span>{label}</span><b>{value}</b></div>
{/snippet}

<section class="models">
  <div class="bar">
    <span class="count"
      >{filtered.length} model{filtered.length === 1 ? "" : "s"}</span
    >
    <span class="spacer"></span>
    <span class="seg">
      {#each SORT_KEYS as [val, label]}
        <button
          class="seg-btn"
          class:active={sortKey === val}
          title="click to toggle asc / desc"
          onclick={() => {
            if (sortKey === val) sortDir = sortDir === "asc" ? "desc" : "asc";
            else {
              sortKey = val;
              sortDir = "asc";
            }
          }}
          >{label}{#if sortKey === val}<sup class="dir">({sortDir})</sup>{/if}</button
        >
      {/each}
    </span>
  </div>

  <div class="body">
    <div class="sidebar">
      {#each filtered as m (m.name)}
        <button
          class="item"
          class:sel={selected === m.name}
          onclick={() => (selected = m.name)}
          title={m.name}
        >
          <span class="iname">{nameAt(m.name, sortKey)}</span>
          <span class="ibadges">
            <span class="bdg temp" class:timeless={m.kind === "timeless"}
              >{m.kind === "timeless" ? "∞" : "⏱"}</span
            >
            <span class="bdg mat" class:view={m.model_type === "VIEW"}
              >{m.model_type === "VIEW" ? "V" : "T"}</span
            >
          </span>
        </button>
      {/each}
      {#if !filtered.length}
        <div class="empty">{view.full ? "no models match" : "loading…"}</div>
      {/if}
    </div>

    {#if node}
      <div class="detail">
        <div class="d-head">
          <span class="d-title">{shortName(selected)}</span>
          <span class="d-badges">
            <span class="pill temp" class:timeless={node.kind === "timeless"}
              >{node.kind}</span
            >
            <span class="pill mat" class:view={node.model_type === "VIEW"}
              >{node.model_type}</span
            >
            {#if hasMeta && meta.enabled === false}
              <span class="pill off">disabled</span>
            {/if}
            {#if health}
              <span class="health"
                ><span class="hdot" style="background:{health.c}"></span
                >{health.label}</span
              >
            {/if}
          </span>
        </div>
        <div class="d-fqn">{@render fqn(selected)}</div>
        {#if hasMeta && meta.description}
          <div class="desc">{meta.description}</div>
        {/if}

        <!-- at-a-glance -->
        <div class="stats">
          <div class="stat">
            <span class="s-l">chunk</span>
            <span class="s-v">{meta?.batching?.chunk ?? "—"}</span>
          </div>
          <div class="stat">
            <span class="s-l">write mode</span>
            <span class="s-v"
              >{node.model_type === "VIEW"
                ? "—"
                : hasMeta
                  ? meta.write_mode || "—"
                  : "—"}</span
            >
          </div>
          <div class="stat">
            <span class="s-l">contract</span>
            <span class="s-v">{contractSpan}</span>
          </div>
          <div class="stat">
            <span class="s-l">primary key</span>
            <span class="s-v" title={meta?.primary_key?.join(", ")}
              >{meta?.primary_key?.length ? meta.primary_key.join(", ") : "—"}</span
            >
          </div>
          <div class="stat">
            <span class="s-l">columns</span>
            <span class="s-v">{cols.length || "—"}</span>
          </div>
        </div>

        <!-- columns (the wide one) -->
        {#if cols.length}
          <div class="section-h">columns ({cols.length})</div>
          <table class="cols">
            <thead>
              <tr><th>column</th><th>type</th><th>null?</th><th>key</th></tr>
            </thead>
            <tbody>
              {#each cols as c}
                <tr class:pk={c.primary_key}>
                  <td class="cname">{c.name}</td>
                  <td class="ctype">{colType(c)}</td>
                  <td class="cnull">{c.nullable === false ? "NOT NULL" : ""}</td>
                  <td class="cflag">{c.primary_key ? "PK" : c.unique ? "UQ" : ""}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        {/if}

        <!-- the model's own properties, in bollhav terms -->
        <div class="cards">
          <div class="card">
            <div class="card-h">storage</div>
            {#if hasMeta}
              {@render kv("catalog", meta.catalog || "—")}
              {@render kv("schema", meta.schema || "—")}
              {@render kv("table", meta.table || "—")}
            {/if}
            {@render kv("model type", node.model_type)}
            {#if hasMeta && node.model_type !== "VIEW"}
              {@render kv("write mode", meta.write_mode || "—")}
              {#if meta.staging}{@render kv("staging", "yes")}{/if}
              {#if meta.partitioned_by}
                {@render kv("partitioned by", meta.partitioned_by)}
              {/if}
            {/if}
            {#if hasMeta && meta.dsn_env_var}
              {@render kv("dsn env var", meta.dsn_env_var)}
            {/if}
          </div>

          <div class="card">
            <div class="card-h">temporality &amp; contract</div>
            {@render kv("temporality", node.kind)}
            {#if hasMeta && meta.contract}
              {@render kv("contract begin", fmtTs(meta.contract.begin))}
              {@render kv(
                "contract end",
                meta.contract.end ? fmtTs(meta.contract.end) : "∞ open",
              )}
            {/if}
          </div>

          {#if hasMeta && meta.batching}
            <div class="card">
              <div class="card-h">batching</div>
              {@render kv("chunk", meta.batching.chunk)}
              {#if meta.batching.window != null}
                {@render kv("window", meta.batching.window)}
              {/if}
              {#if meta.batching.lookback != null}
                {@render kv("lookback", meta.batching.lookback)}
              {/if}
              {@render kv("batch size", `${meta.batching.size} rows`)}
              {@render kv("fixed intervals", String(meta.batching.fixed_intervals))}
            </div>
          {/if}

          {#if hasMeta && (meta.primary_key?.length || meta.unique_columns?.length)}
            <div class="card">
              <div class="card-h">keys</div>
              {#if meta.primary_key?.length}
                {@render kv("primary key", meta.primary_key.join(", "))}
              {/if}
              {#if meta.unique_columns?.length}
                {@render kv("unique", meta.unique_columns.join(", "))}
              {/if}
            </div>
          {/if}

          {#if meta?.tags?.length}
            {@const tags = meta.tags.filter((t) => !t.includes("."))}
            <div class="card">
              <div class="card-h">tags ({tags.length})</div>
              <div class="tags">
                {#each tags as t}<span class="chip">{t}</span>{/each}
              </div>
            </div>
          {/if}

          <div class="card">
            <div class="card-h">registry</div>
            {@render kv("last seen", fmtTs(node.last_seen))}
            {@render kv("enabled", hasMeta ? String(meta.enabled !== false) : "—")}
          </div>
        </div>

        {#if loading}
          <div class="foot">loading details…</div>
        {:else if meta && !hasMeta}
          <div class="foot">no stored metadata — re-run the pipeline to populate</div>
        {/if}
      </div>
    {:else}
      <div class="detail empty-detail">
        {view.full ? "Select a model" : "loading…"}
      </div>
    {/if}
  </div>
</section>

<style>
  .models {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    background: var(--bg);
    color: var(--fg);
  }
  .bar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .count {
    font-size: 12px;
    color: var(--muted);
  }
  .spacer {
    flex: 1;
  }
  .seg {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    border: 1px solid var(--control-border);
    border-radius: 6px;
    background: var(--control-bg);
    padding: 2px;
  }
  .seg-btn {
    font-size: 11px;
    padding: 3px 8px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: var(--control-fg);
    cursor: pointer;
  }
  .seg-btn.active {
    background: #2e7d32;
    color: #fff;
  }
  .dir {
    font-size: 8px;
    margin-left: 1px;
    color: #ffd23f;
  }
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
  }
  .sidebar {
    flex: 0 0 260px;
    overflow-y: auto;
    border-right: 1px solid var(--border);
    padding: 6px;
  }
  .item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;
    text-align: left;
    background: transparent;
    border: none;
    border-radius: 6px;
    padding: 5px 8px;
    color: var(--fg);
    font: inherit;
    font-size: 12px;
    cursor: pointer;
  }
  .item:hover {
    background: var(--control-bg);
  }
  .item.sel {
    background: #2e7d32;
    color: #fff;
  }
  .iname {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ibadges {
    display: inline-flex;
    gap: 3px;
    flex: 0 0 auto;
  }
  .bdg {
    font-size: 9px;
    width: 15px;
    text-align: center;
    border-radius: 3px;
    padding: 1px 0;
    background: #43a047;
    color: #fff;
  }
  .bdg.temp {
    background: #2f80ed;
  }
  .bdg.temp.timeless {
    background: #1e3a8a;
  }
  .bdg.mat {
    background: #1b5e20;
  }
  .bdg.mat.view {
    background: #66bb6a;
  }
  .item.sel .bdg {
    background: rgba(255, 255, 255, 0.25);
  }
  .item.sel .bdg.temp {
    background: #2f80ed;
  }
  .item.sel .bdg.temp.timeless {
    background: #1e3a8a;
  }

  .detail {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 16px 20px 28px;
    font-size: 12px;
  }
  .empty-detail {
    color: var(--muted);
    font-style: italic;
  }
  .d-head {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .d-title {
    font-weight: 800;
    font-size: 19px;
    word-break: break-word;
  }
  .d-badges {
    display: inline-flex;
    gap: 5px;
    align-items: center;
  }
  .pill {
    font-size: 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    padding: 2px 7px;
    border-radius: 9px;
    background: #43a047;
    color: #fff;
  }
  .pill.temp {
    background: #2f80ed;
  }
  .pill.temp.timeless {
    background: #1e3a8a;
  }
  .pill.mat {
    background: #1b5e20;
  }
  .pill.mat.view {
    background: #66bb6a;
  }
  .pill.off {
    background: #b3261e;
  }
  .health {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 11px;
    color: var(--muted);
    margin-left: 2px;
  }
  .hdot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
  }
  .d-fqn {
    margin: 7px 0 9px;
    font-size: 11px;
    word-break: break-word;
  }
  .cat {
    color: #2563eb;
  }
  .sch {
    color: #3b82f6;
  }
  .tbl {
    color: #60a5fa;
  }
  .desc {
    font-style: italic;
    color: var(--muted);
    margin-bottom: 12px;
    max-width: 70ch;
  }

  /* at-a-glance strip */
  .stats {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 6px;
  }
  .stat {
    flex: 1 1 130px;
    min-width: 120px;
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 11px;
    background: var(--control-bg, var(--bg));
  }
  .s-l {
    display: block;
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
    margin-bottom: 3px;
  }
  .s-v {
    display: block;
    font-size: 13px;
    font-weight: 700;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .section-h {
    margin: 20px 0 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-size: 11px;
    color: var(--fg);
    border-bottom: 1px solid var(--border);
    padding-bottom: 6px;
  }

  /* columns */
  .cols {
    width: 100%;
    border-collapse: collapse;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
  }
  .cols th {
    text-align: left;
    color: var(--muted);
    font-weight: 600;
    border-bottom: 1px solid var(--border);
    padding: 3px 10px 5px 0;
  }
  .cols td {
    padding: 3px 10px 3px 0;
    border-bottom: 1px solid var(--table-border, var(--border));
    vertical-align: top;
  }
  .cols tr.pk .cname {
    color: #ffd23f;
  }
  .cname {
    word-break: break-word;
    font-weight: 600;
  }
  .ctype {
    color: var(--muted);
    white-space: nowrap;
  }
  .cnull {
    color: #e0883a;
    white-space: nowrap;
    font-size: 10px;
  }
  .cflag {
    color: #ffd23f;
    font-weight: 700;
    text-align: right;
    white-space: nowrap;
  }

  /* property cards */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 14px;
    align-items: start;
    margin-top: 22px;
  }
  .card {
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 11px 14px 13px;
    background: var(--control-bg, var(--bg));
    min-width: 0;
  }
  .card-h {
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-size: 10px;
    color: var(--muted);
    margin-bottom: 8px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--border);
  }
  .kv {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    padding: 2px 0;
  }
  .kv span {
    color: var(--muted);
  }
  .kv b {
    text-align: right;
    word-break: break-word;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    margin-top: 2px;
  }
  .chip {
    font-size: 10px;
    color: #16a34a;
    background: rgba(22, 163, 74, 0.12);
    border-radius: 9px;
    padding: 2px 8px;
  }
  .foot {
    margin-top: 12px;
    color: var(--muted);
    font-style: italic;
  }
  .empty {
    padding: 16px 8px;
    color: var(--muted);
    font-style: italic;
    font-size: 12px;
  }
</style>
