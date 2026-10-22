import adapter from '@sveltejs/adapter-static'
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte'

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    // Every page is prerendered into build/, one folder per page, so a static
    // host serves the result with no configuration of its own.
    adapter: adapter({ pages: 'build', assets: 'build', strict: true }),
    // Served at bollhav.dev/docs: one App Platform app hosts the deck, the
    // docs, learn and lab (see .do/app.yaml), which sets BASE_PATH=/docs at
    // build time so every link and asset URL carries the prefix. Unset
    // (local dev) it is the root.
    paths: { base: process.env.BASE_PATH || '' },
  },
}

export default config
