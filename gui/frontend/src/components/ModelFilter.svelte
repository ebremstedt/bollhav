<script>
  // Filter models by tag / tag expression, on a row of its own above every
  // tab's bar (it used to be in the header). A model's own name is one of its
  // tags, so this covers picking a single model too. The expression is applied
  // with the button or Enter, since it's evaluated server-side (/match).
  import { view, applyTagFilter, clearFocus, setMatFilter } from "../lib/view.svelte.js";

  // the site-wide materialization filter
  const MATS = [
    ["TABLE", "table"],
    ["VIEW", "view"],
    ["all", "all"],
  ];

  let allTags = $derived(
    view.full
      ? [...new Set(view.full.nodes.flatMap((n) => n.tags || []))].sort()
      : [],
  );

  // suggestion list under the tag box. A native <datalist> can't be capped
  // or scrolled, so this is our own: tags containing what's typed (all of
  // them while the box is empty), ten rows tall, then it scrolls.
  let tagOpen = $state(false);
  let tagSuggestions = $derived.by(() => {
    const q = view.tagExpr.trim().toLowerCase();
    return q ? allTags.filter((t) => t.toLowerCase().includes(q)) : allTags;
  });

</script>

<!-- a row of its own, above the tab's bar -->
<div class="filterbar">
  <span class="group">
    <span class="group-label">filter models</span>
    <span class="group-body">
<span class="sub">
  <span class="sub-label">model name, tag or tag expression</span>
  <span class="tagrow">
    <span class="tagbox">
      <input
        class="search tagsearch"
        value={view.tagExpr}
        oninput={(e) => {
          view.tagExpr = e.currentTarget.value;
          tagOpen = true;
        }}
        onfocus={() => (tagOpen = true)}
        onblur={() => (tagOpen = false)}
        onkeydown={(e) => {
          if (e.key === "Enter") {
            applyTagFilter();
            tagOpen = false;
          } else if (e.key === "Escape") {
            tagOpen = false;
          }
        }}
      />
      {#if tagOpen && tagSuggestions.length}
        <ul class="taglist">
          {#each tagSuggestions as t}
            <li>
              <!-- mousedown is swallowed so the input keeps focus: otherwise
                   its blur closes the list before the click lands -->
              <button
                type="button"
                onmousedown={(e) => e.preventDefault()}
                onclick={() => {
                  view.tagExpr = t;
                  tagOpen = false;
                  applyTagFilter();
                }}>{t}</button
              >
            </li>
          {/each}
        </ul>
      {/if}
    </span>
    <button class="toggle" onclick={applyTagFilter}>filter</button>
    <button class="toggle" onclick={clearFocus}>✕ clear</button>
  </span>
</span>
<span class="sub mat">
  <span class="sub-label">materialization</span>
  <span class="seg">
    {#each MATS as [val, label]}
      <button
        class="seg-btn"
        class:active={view.matFilter === val}
        onclick={() => setMatFilter(val)}>{label}</button
      >
    {/each}
  </span>
</span>
    </span>
  </span>
</div>

<style>
  .filterbar {
    display: flex;
    padding: 8px 16px 0;
    background: var(--bg);
  }
  /* the box spans the page and the input takes all the width the buttons
     leave over */
  .filterbar .group {
    flex: 1;
  }
  .filterbar .sub {
    flex: 1;
    align-items: stretch;
  }
  .filterbar .sub.mat {
    flex: 0 0 auto;
    align-items: center;
  }
  .seg {
    display: inline-flex;
    align-items: stretch;
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
  .filterbar .sub-label {
    text-align: center;
  }
  .search {
    font-size: 18px;
    padding: 6px 13px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--input-bg);
    color: var(--control-fg);
    width: 100%;
    min-width: 0;
  }
  .search::placeholder {
    color: var(--placeholder);
  }
  .tagrow {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .tagbox {
    position: relative;
    display: flex;
    flex: 1;
  }
  .tagsearch {
    font-family: var(--font-mono);
    color: var(--control-fg);
  }
  .toggle {
    font-family: var(--btn-font);
    font-size: var(--btn-size);
    font-weight: var(--btn-weight);
    padding: 6px 15px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
    white-space: nowrap;
  }
  .taglist {
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    right: 0;
    margin: 0;
    padding: 4px 0;
    list-style: none;
    /* ten 30px rows, then it scrolls */
    max-height: 308px;
    overflow-y: auto;
    background: var(--bg);
    border: 1px solid var(--control-border);
    border-radius: 8px;
    box-shadow: 0 6px 18px rgba(0, 0, 0, 0.18);
    z-index: 60;
  }
  .taglist button {
    font-family: var(--font-mono);
    display: block;
    width: 100%;
    text-align: left;
    padding: 5px 12px;
    font-size: 15px;
    line-height: 20px;
    background: none;
    border: none;
    color: var(--fg);
    cursor: pointer;
  }
  .taglist button:hover {
    background: var(--control-bg);
  }
</style>
