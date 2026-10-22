// The deck, in order. Add a slide: create a .svelte file next to this one,
// import it, put it where it belongs in the list. One slide per layout type
// lives in examples/ for reference; they are not shown.
import Title from './01-title.svelte'
import GracefulChanges from './02-graceful-changes.svelte'
import TwoWays from './04-two-ways.svelte'
import PassiveTagging from './05-passive-tagging.svelte'
import Tagging from './06-tagging.svelte'
import Generators from './07-generators.svelte'
import Metadata from './08-metadata.svelte'
import Terminal from './08b-terminal.svelte'
import Orchestration from './08d-orchestration.svelte'
import Choreography from './08e-choreography.svelte'
import Closing from './08c-closing.svelte'
import End from './09-end.svelte'

export default [Title, GracefulChanges, TwoWays, PassiveTagging, Tagging, Generators, Metadata, Terminal, Orchestration, Choreography, Closing, End]
