import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

export default defineConfig({
  // Served at bollhav.dev/lab: one App Platform app hosts docs, learn and lab
  // (see .do/app.yaml), which sets BASE_PATH=/lab at build time so every asset
  // URL gets the prefix. Unset (local dev, `npm run dev`) it is the root.
  base: process.env.BASE_PATH || '/',
  plugins: [svelte()],
})
