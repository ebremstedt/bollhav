import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

export default defineConfig({
  // The deck is served at the root of bollhav.dev (see .do/app.yaml), so no
  // BASE_PATH is set for it. Same convention as lab/ and learn/ all the same:
  // set it at build time and every asset URL carries the prefix.
  base: process.env.BASE_PATH || '/',
  plugins: [svelte()],
  // the deck is always on 5180 locally, the docs on 5190: each links to the other
  server: { port: 5180, strictPort: true },
  preview: { port: 4180, strictPort: true },
})
