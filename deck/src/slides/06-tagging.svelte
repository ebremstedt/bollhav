<script>
  import Slide from '../lib/Slide.svelte'
  import Code from '../lib/Code.svelte'
</script>

<Slide kicker="Tags" title="Tag expression syntax" layout="wide">
  <p class="lead">Used to target the models that you wish to run.</p>
  <p>Include one or many models, while excluding others, in one expression.</p>
  <div class="cols">
    <div>
      <h3>Simple</h3>
      <Code lang="bash" code={`$ TAGS='[orders]' python main.py`} />
      <p class="muted">Every model tagged <code>orders</code></p>
    </div>
    <div>
      <h3>With or, and negation</h3>
      <Code lang="bash" code={`$ TAGS='[(erp|crm) & not:latest]' python main.py`} />
      <p class="muted">
        Every model from <code>erp</code> or <code>crm</code>, except the ones tagged <code>latest</code>
      </p>
    </div>
  </div>

  <p class="teaser">
    <span class="why" tabindex="0">
      Why not just use regex?
      <span class="card" role="tooltip">
        <span><b>Reload is a first-class concept.</b> The <code>r:</code> prefix ties reload intent to the selection. Regex would need a second mechanism for "match these models <i>and</i> reload them".</span>
        <span><b>Multiple groups are trivial.</b> <code>[sales][finance]</code> reads as "sales or finance". OR across groups with AND within gets unwieldy fast in regex: <code>(?=.*foo)(?=.*bar)|baz</code> is not something you want in an environment variable.</span>
        <span><b>Negation is explicit.</b> <code>[all &amp; not:foo]</code> says exactly what it means. Regex negation, <code>^(?!.*foo).*$</code>, is easy to get wrong and hard to read.</span>
        <span><b>Fewer mistakes.</b> Regex has footguns everywhere: unescaped dots, greedy quantifiers, anchoring. Tag expressions have a small surface: tags, <code>&amp;</code>, <code>|</code>, <code>not:</code>, <code>r:</code> and brackets. If it parses, it does what you expect.</span>
        <span><b>Environment-variable friendly.</b> Short, readable strings work well as <code>TAGS=</code> values. Regex special characters are awkward to pass through a shell and easy to break with quoting.</span>
      </span>
    </span>
  </p>
</Slide>

<style>
  .teaser {
    margin-top: 0.5rem;
    max-width: none;
    text-align: center;
    font-style: italic;
    font-size: clamp(1rem, 1.5vw, 1.3rem);
    color: var(--fg-2);
  }

  .why {
    position: relative;
    display: inline-block;
    cursor: help;
    outline: none;
    text-decoration: underline dotted;
    text-decoration-thickness: 2px;
    text-underline-offset: 0.15em;
  }

  .why:hover,
  .why:focus-visible {
    color: var(--accent);
  }

  /* the explanation, as a card above the line */
  .card {
    position: absolute;
    left: 50%;
    bottom: calc(100% + 0.8em);
    transform: translateX(-50%) translateY(4px);
    width: min(34rem, 86vw);
    display: grid;
    gap: 0.6em;
    padding: 0.9em 1.1em;
    border: 1px solid var(--line);
    border-radius: 0.8em;
    background: var(--bg-2);
    box-shadow: 0 18px 40px -20px rgba(10, 20, 44, 0.7);
    text-align: left;
    font-style: normal;
    font-size: clamp(0.8rem, 1vw, 0.95rem);
    line-height: 1.35;
    color: var(--fg);
    opacity: 0;
    visibility: hidden;
    transition: opacity 0.2s, transform 0.2s, visibility 0.2s;
    pointer-events: none;
  }

  .why:hover .card,
  .why:focus-visible .card,
  .why:focus-within .card {
    opacity: 1;
    visibility: visible;
    transform: translateX(-50%) translateY(0);
  }

  .card b {
    font-weight: 600;
  }

  .card :global(code) {
    font-size: 0.9em;
    padding: 0 0.3em;
  }
</style>
