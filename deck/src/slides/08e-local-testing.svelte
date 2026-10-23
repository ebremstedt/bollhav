<script>
  import Slide from '../lib/Slide.svelte'
  import RoughScene from '../lib/RoughScene.svelte'

  // everything on one machine: the loop that runs the models, and one
  // postgres holding both the state it asks and the tables it writes
  const shapes = [
    { kind: 'frame', x: 16, y: 10, w: 968, h: 344, label: 'your computer', dotted: true, tone: 'muted' },
    { kind: 'box', x: 70, y: 135, w: 250, h: 110, label: 'execution', sub: 'python main.py' },
    { kind: 'frame', x: 540, y: 36, w: 410, h: 304, label: 'docker run postgres', dotted: true, align: 'center', tone: 'muted' },
    { kind: 'cylinder', x: 575, y: 100, w: 340, h: 104, label: 'state', sub: 'what is missing?' },
    { kind: 'cylinder', x: 575, y: 220, w: 340, h: 104, label: 'storage', sub: 'the target tables' },
    { kind: 'arrow', from: [322, 176], to: [573, 154], label: 'ask', packets: 'pingpong', labelOffset: [0, -4] },
    { kind: 'arrow', from: [322, 206], to: [573, 274], label: 'write', packets: 'dots', labelOffset: [0, 30] },
  ]
</script>

<Slide kicker="Running the models" title="End to end pipelines, locally*" layout="wide">
  <RoughScene width={1000} height={360} {shapes} />
  <p class="footnote">* <em>If you use bollhav state</em></p>
</Slide>

<style>
  p.footnote {
    font-family: var(--font-mono);
    font-size: 0.85rem;
    letter-spacing: 0.02em;
    color: var(--fg);
  }
</style>
