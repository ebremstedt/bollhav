<script>
  import Slide from '../../lib/Slide.svelte'
  import Code from '../../lib/Code.svelte'
</script>

<Slide title="Two columns" layout="wide">
  <div class="cols">
    <div>
      <h3>Left</h3>
      <p>Placeholder. Put a <code>div.cols</code> in the slide and each child becomes a column. They stack on a phone.</p>
      <ul>
        <li>Text on one side</li>
        <li>Code, a table or an image on the other</li>
      </ul>
    </div>
    <div>
      <h3>Right</h3>
      <Code
        lang="bash"
        code={`
          $ export TAGS='[orders]'
          $ python main.py   # one worker
          $ python main.py   # a second splits the windows
        `}
      />
    </div>
  </div>
</Slide>
