# bollhav deck

A slide deck as a small Svelte app: one slide at a time over a sea of balls,
in the logo's colours, with keyboard, swipe and button navigation.

The sea is a physics simulation ([Matter.js](https://brm.io/matter-js/)):
about a thousand small balls are rigid bodies in a tank. The tank rocks so
they slosh, a slide change pushes a wave off the wall behind you, and a
click on a ball shoots it up to land again. The
knobs (ball count and size, rocking period and tilt, push strength, the
balls' bounce and drag) are at the top of `src/lib/Sea.svelte`.

```bash
cd deck
npm ci
npm run dev        # http://localhost:5173
npm run build      # dist/
```

## Writing slides

Each slide is a `.svelte` file in `src/slides/`, listed in order in
`src/slides/index.js`. A slide wraps plain HTML in `<Slide>`:

```svelte
<script>
  import Slide from '../lib/Slide.svelte'
  import Code from '../lib/Code.svelte'
</script>

<Slide title="State">
  <p class="lead">One line that frames the slide.</p>
  <ul>
    <li>pending → running → applied</li>
  </ul>
  <Code file="models.py" code={`
    daily = Model(state=State(), ...)
  `} />
</Slide>
```

| `<Slide>` prop | Values |
|---|---|
| `title` | the heading. Omit it for a slide without one |
| `kicker` | a small label above the heading, for the section the slide is in |
| `layout` | `default` (left-aligned), `title` (opening slide, big heading), `center`, `wide` |

Inside a slide, `p`, `ul`/`ol`, `h3`, `blockquote` (+ `cite`), `table`, `img`
and inline `code` are styled. A few helpers:

- `p.lead` — a larger, muted lead line under the heading.
- `div.cols` — children become columns (stack on a phone).
- `div.table` — wrap a table so it scrolls sideways instead of overflowing.
- `img.plain` — no frame or shadow (logos).

`<Code>` takes `code` (a template literal; its indentation is stripped),
`lang` (`python` default, `bash`, `sql`, `plain`) and an optional `file`
caption. `<Schema name tables highlight old>` draws a schema as a card with
its tables as chips. Images live in `public/` and are referenced as
`./name.png`.

`src/slides/examples/` holds one slide per layout type for reference. They
are not in the deck; copy one to start a new slide.

## Navigating

| Key | |
|---|---|
| `→` `↓` `space` `enter` | next slide |
| `←` `↑` `shift+space` | previous slide |
| `home` / `end` | first / last |
| `o` | outline (click any slide title to jump) |
| `f` | fullscreen |
| `t` | light / dark |
| `h` | the hoop game (also the hoop button, top right) |

The hoop game: a hoop sweeps across the screen for 15 seconds; click a ball
to launch it, and every launched ball that passes through the hoop scores.
The best score is kept in the browser.

Swipe on touch screens. The URL hash is the slide number (`#4`), so a slide
can be linked to. The theme follows the OS until toggled; the choice is saved.

## On the site

The deck is the landing page: the `bollhav-deck` static site in
`.do/app.yaml` serves it at `bollhav.dev/`, with the docs under `/docs`,
learn under `/learn` and lab under `/lab`. It is built from `deck/` on every
push to main. The top-bar links (`links` prop on `Deck`) and the `home` link
are set in `src/App.svelte`.

`vite.config.js` still reads `BASE_PATH` at build time, like `lab/` and
`learn/`, in case the deck ever moves under a prefix.
