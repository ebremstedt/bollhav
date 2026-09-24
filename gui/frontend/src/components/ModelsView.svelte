<script>
  import { view, matOk } from "../lib/view.svelte.js";
  import { ui } from "../lib/url.svelte.js";
  import ModelFilter from "./ModelFilter.svelte";
  import { getModelMeta } from "../lib/api.js";

  // A run-independent model browser: the model registry (left, env-aware) and a
  // model's full stored bollhav metadata (right). Lineage/position lives on the
  // Lineage tab; this is the model's own properties.

  let meta = $state(null); // the /model property bag for `selected`
  let loading = $state(false);

  let models = $derived((view.full?.nodes || []).filter((n) => n.type === "model"));
  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);

  // sort by identity level — full name / schema.table / table — with an
  // asc·desc toggle. The level also sets how much of the name each row shows.
  // [key, label]: the key also picks how much of the dotted name the list
  // shows (catalog → the full name, schema → schema.table, table → table)
  const SORT_KEYS = [
    ["full", "catalog"],
    ["schematable", "schema"],
    ["table", "table"],
  ];
  // the detail shows one pane at a time (a sub-menu switches), so a model's
  // page never turns into a wall of everything at once
  const PANES = [
    ["properties", "properties"],
    ["columns", "schema"],
  ];

  // how much of the dotted name a row shows and sorts by, per the sort level
  function nameAt(full, key) {
    const p = (full || "").split(".");
    if (key === "table") return p[p.length - 1] || full;
    if (key === "schematable") return p.slice(-2).join(".");
    return full;
  }

  // the tag / tag-expression filter (the row above the bar), then the sort
  let filtered = $derived.by(() => {
    let list = models.slice();
    if (tagMatchSet) list = list.filter((m) => tagMatchSet.has(m.name));
    list = list.filter(matOk);
    const sign = ui.modelsDir === "asc" ? 1 : -1;
    list.sort((a, b) => {
      const c = nameAt(a.name, ui.modelsSort)
        .toLowerCase()
        .localeCompare(nameAt(b.name, ui.modelsSort).toLowerCase());
      return (c || a.name.localeCompare(b.name)) * sign;
    });
    return list;
  });

  $effect(() => {
    void view.env;
    const list = filtered;
    if (list.length && (!ui.modelsOpen || !list.some((m) => m.name === ui.modelsOpen))) {
      ui.modelsOpen = list[0].name;
    }
  });

  let node = $derived(models.find((m) => m.name === ui.modelsOpen) || null);

  $effect(() => {
    const name = ui.modelsOpen;
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

  const fmtTs = (iso) => (iso ? iso.replace("T", " ").slice(0, 19) : "—");
  const shortName = (full) => (full || "").split(".").slice(-1)[0];

  // the schema table: click a heading to sort by it (again to flip), drag
  // a heading's right edge to resize the column
  const COL_HEADS = [
    ["name", "column"],
    ["type", "type"],
    ["null", "null?"],
    ["key", "key"],
  ];
  let colSort = $state({ key: null, dir: "asc" });
  let colHi = $state(null); // the highlighted (clicked) row of the schema table
  let colWidths = $state({}); // heading key -> px, once dragged
  const colCell = (c, key) =>
    key === "name"
      ? c.name
      : key === "type"
        ? colType(c)
        : key === "null"
          ? c.nullable === false
            ? "NOT NULL"
            : ""
          : c.primary_key
            ? "PK"
            : c.unique
              ? "UQ"
              : "";
  let sortedCols = $derived.by(() => {
    const list = cols.slice();
    if (!colSort.key) return list;
    const sign = colSort.dir === "asc" ? 1 : -1;
    return list.sort(
      (a, b) =>
        (String(colCell(a, colSort.key)).localeCompare(String(colCell(b, colSort.key))) ||
          a.name.localeCompare(b.name)) * sign,
    );
  });
  function sortColsBy(key) {
    if (colSort.key === key) colSort = { key, dir: colSort.dir === "asc" ? "desc" : "asc" };
    else colSort = { key, dir: "asc" };
  }
  function startResize(ev, key) {
    ev.preventDefault();
    const th = ev.currentTarget.parentElement;
    const startX = ev.clientX;
    const startW = th.offsetWidth;
    const move = (e) => (colWidths = { ...colWidths, [key]: Math.max(40, startW + e.clientX - startX) });
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  }

  function colType(c) {
    let t = c.type || "?";
    if (c.length != null) t += `(${c.length})`;
    else if (c.precision != null)
      t += `(${c.precision}${c.scale != null ? "," + c.scale : ""})`;
    return t;
  }
  // catalog / schema / table of a dotted name; a shorter name leaves the
  // leading cells empty
  function idParts(full) {
    const parts = String(full || "").split(".");
    while (parts.length < 3) parts.unshift("");
    return [parts.slice(0, -2).join("."), parts[parts.length - 2], parts[parts.length - 1]];
  }

</script>

{#snippet kv(label, value)}
  <tr><td class="k">{label}</td><td class="v">{value}</td></tr>
{/snippet}

<section class="models">
  <ModelFilter />
  <div class="bar">
    <!-- captioned boxes like the other tabs (name / tag filtering is the row above) -->
    <span class="group">
      <span class="group-label">sorting</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">models</span>
    <span class="seg">
      {#each SORT_KEYS as [val, label]}
        <!-- two lines: the key, and the direction under the active one -->
        <button
          class="seg-btn two-line"
          class:active={ui.modelsSort === val}
          title="click to toggle ascending / descending"
          onclick={() => {
            if (ui.modelsSort === val) ui.modelsDir = ui.modelsDir === "asc" ? "desc" : "asc";
            else {
              ui.modelsSort = val;
              ui.modelsDir = "asc";
            }
          }}
        >
          <span>{label}</span>
          <span class="dir" class:hidden={ui.modelsSort !== val}
            >{ui.modelsSort === val && ui.modelsDir === "desc" ? "descending" : "ascending"}</span
          >
        </button>
      {/each}
    </span>
        </span>
      </span>
    </span>
    <!-- which pane of the selected model's detail to show, one at a time -->
    <span class="group">
      <span class="group-label">display</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">models</span>
          <span class="seg">
            {#each PANES as [val, label]}
              <button class="seg-btn" class:active={ui.modelsPane === val} onclick={() => (ui.modelsPane = val)}
                >{label}</button
              >
            {/each}
          </span>
        </span>
      </span>
    </span>
  </div>

  <div class="body">
    <div class="sidebar">
      {#each filtered as m (m.name)}
        <button
          class="item"
          class:sel={ui.modelsOpen === m.name}
          onclick={() => (ui.modelsOpen = m.name)}
          title={m.name}
        >
          <span class="iname">{nameAt(m.name, ui.modelsSort)}</span>
        </button>
      {/each}
      {#if !filtered.length}
        <div class="empty">{view.full ? "no models match" : "loading…"}</div>
      {/if}
    </div>

    {#if node}
      <div class="detail">
        <div class="d-head">
          <span class="d-title">{shortName(ui.modelsOpen)}</span>
          <span class="d-badges">
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
        <!-- the model's identity, one column per part -->
        <table class="idtable">
          <thead>
            <tr><th>catalog</th><th>schema</th><th>table</th></tr>
          </thead>
          <tbody>
            <tr>
              {#each idParts(ui.modelsOpen) as part}<td>{part}</td>{/each}
            </tr>
          </tbody>
        </table>
        {#if hasMeta && meta.description}
          <div class="desc">{meta.description}</div>
        {/if}

        {#if ui.modelsPane === "columns"}
        {#if cols.length}
          <table class="cols">
            <thead>
              <tr>
                {#each COL_HEADS as [key, label]}
                  <th style:width={colWidths[key] ? `${colWidths[key]}px` : null}>
                    <button class="th-sort" onclick={() => sortColsBy(key)}
                      >{label}{#if colSort.key === key}
                        {colSort.dir === "asc" ? "▲" : "▼"}{/if}</button
                    >
                    <span
                      class="th-grip"
                      title="drag to resize"
                      onpointerdown={(e) => startResize(e, key)}
                    ></span>
                  </th>
                {/each}
              </tr>
            </thead>
            <tbody>
              {#each sortedCols as c}
                <tr
                  class:pk={c.primary_key}
                  class:hi={colHi === c.name}
                  onclick={() => (colHi = colHi === c.name ? null : c.name)}
                >
                  <td class="cname">{c.name}</td>
                  <td class="ctype">{colType(c)}</td>
                  <td class="cnull">{c.nullable === false ? "NOT NULL" : ""}</td>
                  <td class="cflag">{c.primary_key ? "PK" : c.unique ? "UQ" : ""}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        {:else}
          <div class="foot">no column metadata stored for this model</div>
        {/if}
        {/if}

        {#if ui.modelsPane === "properties"}
        <!-- the model's own properties, in bollhav terms: one table, a heading row per topic -->
        <table class="ptable">
          <tbody>
          <tr class="phead"><th colspan="2">storage</th></tr>
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

          <tr class="phead"><th colspan="2">temporality &amp; contract</th></tr>
            {@render kv("temporality", node.kind)}
            {#if hasMeta && meta.contract}
              <!-- an unset bound is unbounded: ∞ on either side -->
              {@render kv("contract begin", meta.contract.begin ? fmtTs(meta.contract.begin) : "∞")}
              {@render kv("contract end", meta.contract.end ? fmtTs(meta.contract.end) : "∞")}
            {/if}

          {#if hasMeta && meta.batching}
            <tr class="phead"><th colspan="2">batching</th></tr>
              {@render kv("chunk", meta.batching.chunk)}
              {#if meta.batching.window != null}
                {@render kv("window", meta.batching.window)}
              {/if}
              {#if meta.batching.lookback != null}
                {@render kv("lookback", meta.batching.lookback)}
              {/if}
              {@render kv("batch size", `${meta.batching.size} rows`)}
              {@render kv("fixed intervals", String(meta.batching.fixed_intervals))}
          {/if}

          {#if hasMeta && (meta.primary_key?.length || meta.unique_columns?.length)}
            <tr class="phead"><th colspan="2">keys</th></tr>
              {#if meta.primary_key?.length}
                {@render kv("primary key", meta.primary_key.join(", "))}
              {/if}
              {#if meta.unique_columns?.length}
                {@render kv("unique", meta.unique_columns.join(", "))}
              {/if}
          {/if}

          {#if meta?.tags?.length}
            {@const tags = meta.tags.filter((t) => !t.includes("."))}
            <tr class="phead"><th colspan="2">tags ({tags.length})</th></tr>
            <tr>
              <td colspan="2" class="tags-cell">
                <span class="tags">{#each tags as t}<span class="chip">{t}</span>{/each}</span>
              </td>
            </tr>
          {/if}

          <tr class="phead"><th colspan="2">registry</th></tr>
            {@render kv("last seen", fmtTs(node.last_seen))}
            {@render kv("enabled", hasMeta ? String(meta.enabled !== false) : "—")}
          </tbody>
        </table>
        {/if}

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
    justify-content: center;
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .seg {
    display: inline-flex;
    align-items: stretch; /* buttons fill the segment's height (see .sub) */
    gap: 3px;
    border: 1px solid var(--control-border);
    border-radius: 8px;
    background: var(--control-bg);
    padding: 3px;
  }
  .seg-btn {
    font-family: var(--box-option-font);
    font-size: var(--box-option-size);
    font-weight: var(--box-option-weight);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 5px 14px;
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
  .seg-btn.two-line {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    line-height: 1.15;
    padding: 4px 12px;
  }
  .dir {
    font-size: var(--box-option-sub-size);
    opacity: 0.85;
    display: inline-block;
    min-width: 66px; /* "descending" — so the button never resizes */
    text-align: center;
  }
  .dir.hidden {
    visibility: hidden;
  }
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
    /* everything below the bar (list + detail) reads a quarter larger than
       the rest of the app */
  }
  /* drag the bottom-right corner to widen it; names longer than the width
     scroll sideways instead of being cut off */
  .sidebar {
    flex: 0 0 auto;
    width: 350px;
    min-width: 200px;
    max-width: 60vw;
    resize: horizontal;
    overflow: auto;
    border-right: 1px solid var(--border);
    padding: 8px;
  }
  .item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    width: max-content;
    min-width: 100%;
    text-align: left;
    background: transparent;
    border: none;
    border-radius: 8px;
    padding: 6px 10px;
    color: var(--fg);
    font: inherit;
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    font-size: var(--name-size);
    cursor: pointer;
  }
  .item:hover {
    background: var(--control-bg);
  }
  .item.sel {
    background: var(--row-hi);
    box-shadow: inset 3px 0 0 #2e7d32;
  }
  .iname {
    white-space: nowrap;
  }

  .detail {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 20px 25px 35px;
    font-size: 15px;
  }
  .empty-detail {
    color: var(--muted);
    font-style: italic;
  }
  .d-head {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
  }
  .d-title {
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    font-size: 24px;
    word-break: break-word;
  }
  .d-badges {
    display: inline-flex;
    gap: 6px;
    align-items: center;
  }
  .pill {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    padding: 2px 9px;
    border-radius: 11px;
    background: #43a047;
    color: #fff;
  }
  .pill.off {
    background: #b3261e;
  }
  .health {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 14px;
    color: var(--muted);
    margin-left: 2px;
  }
  .hdot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
  }
  .idtable {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    border-collapse: collapse;
    margin: 8px 0 12px;
  }
  .idtable th {
    font-family: var(--table-head-font);
    font-size: var(--table-head-size);
    font-weight: var(--table-head-weight);
    color: var(--muted);
    text-align: left;
    padding: 1px 18px 1px 0;
  }
  .idtable td {
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    padding: 1px 18px 1px 0;
    color: var(--fg);
  }
  .desc {
    font-style: italic;
    color: var(--muted);
    margin-bottom: 15px;
    max-width: 70ch;
  }



  /* columns: the table hugs its content, so each column is as wide as its
     longest value and "type" sits right after the longest column name
     (a dragged heading still widens / narrows its column) */
  .cols {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    width: auto;
    border-collapse: collapse;
  }
  .cols th {
    font-family: var(--table-head-font);
    font-size: var(--table-head-size);
    font-weight: var(--table-head-weight);
    position: relative;
    text-align: left;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding: 3px 30px 6px 0;
    white-space: nowrap;
  }
  /* heading = a sort button; the thin strip at its right edge resizes */
  .th-sort {
    font: inherit;
    color: inherit;
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
  }
  .th-grip {
    position: absolute;
    top: 0;
    bottom: 0;
    right: 0;
    width: 9px;
    cursor: col-resize;
    border-right: 2px solid transparent;
  }
  .th-grip:hover {
    border-right-color: var(--control-border);
  }
  .cols tbody tr {
    cursor: pointer;
  }
  .cols tr.hi td {
    background: var(--row-hi);
  }
  .cols tr.hi td:first-child {
    box-shadow: inset 3px 0 0 #2e7d32;
  }
  .cols td {
    padding: 3px 30px 3px 0;
    border-bottom: 1px solid var(--table-border, var(--border));
    vertical-align: top;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .cname {
    word-break: break-word;
    font-weight: 600;
  }
  /* plain text throughout: no colour coding in the schema table */
  .ctype {
    white-space: nowrap;
  }
  .cnull {
    white-space: nowrap;
    font-size: 12px;
  }
  .cflag {
    font-weight: 700;
    text-align: center;
    white-space: nowrap;
  }
  .cols th:last-child {
    text-align: center;
  }

  /* the properties: one table, a heading row per topic */
  .ptable {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    border-collapse: collapse;
    width: 100%;
    max-width: 950px;
  }
  .ptable .phead th {
    font-family: var(--table-head-font);
    font-size: var(--table-head-size);
    font-weight: var(--table-head-weight);
    text-align: left;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    padding: 18px 0 5px;
    border-bottom: 1px solid var(--border);
  }
  .ptable td {
    padding: 3px 18px 3px 0;
    vertical-align: top;
  }
  .ptable td.k {
    color: var(--muted);
    white-space: nowrap;
    width: 1%;
  }
  .ptable td.v {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
  }
  .ptable .tags-cell {
    padding-top: 8px;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 2px;
  }
  .chip {
    font-size: 12px;
    color: var(--fg);
    background: rgba(22, 163, 74, 0.12);
    border-radius: 11px;
    padding: 2px 10px;
  }
  .foot {
    margin-top: 15px;
    color: var(--muted);
    font-style: italic;
  }
  .empty {
    padding: 20px 10px;
    color: var(--muted);
    font-style: italic;
    font-size: 15px;
  }
</style>
