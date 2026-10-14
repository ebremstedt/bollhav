<script>
  // A schema drawn as a card with its tables as chips.
  //
  //   <Schema name="orders" tables={['a', 'b', 'c']} />
  //   <Schema name="orders" tables={['a', 'a_v2']} highlight={['a_v2']} />
  //   <Schema name="orders" tables={['a']} old />   (dashed: on its way out)
  let { name, tables = [], highlight = [], old = false } = $props()
</script>

<div class="schema" class:old>
  <div class="name">{name}</div>
  <div class="tables">
    {#each tables as t}
      <span class="table" class:new={highlight.includes(t)}>{t}</span>
    {/each}
  </div>
</div>

<style>
  .schema {
    display: grid;
    gap: 0.6rem;
    padding: 0.85rem 1rem 1rem;
    border: 1px solid var(--line);
    border-radius: 14px;
    background: color-mix(in srgb, var(--bg-2) 75%, transparent);
    font-family: var(--font-mono);
    font-size: clamp(0.85rem, 1.3vw, 1.1rem);
    min-width: 0;
  }

  .schema.old {
    border-style: dashed;
    opacity: 0.7;
  }

  .name {
    display: flex;
    align-items: center;
    gap: 0.5em;
    font-weight: 600;
    letter-spacing: 0.03em;
  }

  .name::before {
    content: '';
    width: 0.6em;
    height: 0.6em;
    border-radius: 50%;
    background: radial-gradient(circle at 35% 32%, #ffffffb8 0%, var(--accent) 65%);
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--fg) 35%, transparent);
  }

  .tables {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }

  .table {
    padding: 0.15em 0.6em;
    border: 1px solid var(--line);
    border-radius: 0.5em;
    background: var(--bg);
  }

  .table.new {
    border-color: var(--accent);
    color: var(--accent);
    font-weight: 600;
  }
</style>
