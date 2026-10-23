<script>
  import Slide from '../lib/Slide.svelte'
  import StagingStory from '../lib/StagingStory.svelte'
</script>

<Slide kicker="Writing data" title="Stage data before insertion" layout="wide">
  <StagingStory />
  <ul>
    <li>Streaming means memory is kept low</li>
    <li>Transactions ensure no part intervals</li>
    <li>Cleanup included</li>
  </ul>
</Slide>
