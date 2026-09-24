<script>
  import {
    view,
    setDetail,
    setHideUpstreams,
    recenter,
  } from "../lib/view.svelte.js";
  import ModelFilter from "./ModelFilter.svelte";

  // detail-level radio: simple (names only) vs verbose (everything).
  // [level, label, hover tip]
  const DETAILS = [
    ["lappland", "simple", "bare — just boxes, names, and arrows"],
    [
      "stockholm",
      "verbose",
      "everything — pills, status lights, runs/errors buttons, and contract labels",
    ],
  ];

</script>

<ModelFilter />
<div class="subbar">
  <!-- captioned boxes, like the other tabs' bars -->
  <span class="group">
    <span class="group-label">display</span>
    <span class="group-body">
      <span class="sub">
        <span class="sub-label">detail</span>
        <span
          class="tip-wrap"
          data-tip="Detail level — how much decoration to show on the graph."
        >
          <span class="seg">
            {#each DETAILS as [val, label, tip]}
              <button
                class="seg-btn"
                class:active={view.detail === val}
                data-tip={tip}
                title={val}
                onclick={() => setDetail(val)}>{label}</button
              >
            {/each}
          </span>
        </span>
      </span>
      <span class="sub">
        <span class="sub-label">upstreams</span>
        <span
          class="tip-wrap"
          data-tip="When focusing a model or tag-filtering, include the upstream chain or show only the matched models."
        >
          <span class="seg">
            <button
              class="seg-btn"
              class:active={!view.hideUpstreams}
              onclick={() => setHideUpstreams(false)}>show</button
            >
            <button
              class="seg-btn"
              class:active={view.hideUpstreams}
              onclick={() => setHideUpstreams(true)}>hide</button
            >
          </span>
        </span>
      </span>
    </span>
  </span>
  <!-- the viewport itself: framing the graph -->
  <span class="group">
    <span class="group-label">alignment</span>
    <span class="group-body">
      <span class="sub">
        <span class="sub-label">models</span>
        <span
          class="tip-wrap"
          data-tip="Recenter models — frame the whole graph (caps zoom so a lone node isn't blown up)."
        >
          <button class="toggle" onclick={() => recenter()}>recenter</button>
        </span>
      </span>
    </span>
  </span>
</div>

<style>
  .subbar {
    display: flex;
    align-items: center;
    justify-content: center; /* the boxes sit centred, like the top menu */
    gap: 10px;
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    background: var(--bg);
    color: var(--fg);
  }
  .toggle {
    font-family: var(--box-option-font);
    font-size: var(--box-option-size);
    font-weight: var(--box-option-weight);
    padding: 6px 15px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
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
  .tip-wrap {
    position: relative;
    display: inline-flex;
  }
  .tip-wrap:hover::after {
    content: attr(data-tip);
    position: absolute;
    top: calc(100% + 9px);
    left: 0;
    width: 230px;
    white-space: normal;
    background: #222;
    color: #fff;
    font-size: 12px;
    line-height: 1.35;
    padding: 7px 9px;
    border-radius: 6px;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.3);
    z-index: 50;
    pointer-events: none;
  }
  .tip-wrap:hover::before {
    content: "";
    position: absolute;
    top: calc(100% + 3px);
    left: 14px;
    border: 6px solid transparent;
    border-bottom-color: #222;
    z-index: 50;
    pointer-events: none;
  }
</style>
