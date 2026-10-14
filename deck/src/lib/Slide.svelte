<script>
  // One slide's content. Wrap a slide's markup in this and it gets the deck's
  // typography (see app.css, "slide typography").
  //
  //   title   the slide heading (an <h2>; <h1> on the title layout)
  //   kicker  a small label above the heading (the section this slide is in)
  //   layout  default  heading + content, left-aligned
  //           title    the opening slide: big <h1>, centred
  //           center   everything centred (a statement, a single image)
  //           wide     the content column runs the full width
  let { title = '', kicker = '', layout = 'default', children } = $props()
</script>

<div class="slide-body layout-{layout}">
  {#if kicker}
    <p class="kicker">{kicker}</p>
  {/if}
  {#if title}
    {#if layout === 'title'}
      <h1>{title}</h1>
    {:else}
      <h2>{title}</h2>
    {/if}
  {/if}
  {@render children?.()}
</div>

<style>
  .kicker {
    margin: 0 0 -0.4rem;
    font-family: var(--font-mono);
    font-size: clamp(0.72rem, 1vw, 0.88rem);
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--fg-2);
  }

  .layout-title,
  .layout-center {
    justify-items: center;
    text-align: center;
  }

  .layout-title :global(p),
  .layout-center :global(p),
  .layout-center :global(blockquote),
  .layout-title :global(blockquote) {
    margin-inline: auto;
  }

  .layout-title :global(ul),
  .layout-center :global(ul) {
    text-align: left;
  }

  /* wide: no reading-measure cap, for columns, tables and big images */
  .layout-wide :global(p),
  .layout-wide :global(li) {
    max-width: none;
  }
</style>
