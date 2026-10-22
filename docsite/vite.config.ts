import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [sveltekit()],
  // the pages live in ../docs, outside the site: let vite serve and watch them.
  // The docs are always on 5190 locally, the deck on 5180: each links to the other
  server: { port: 5190, strictPort: true, fs: { allow: ['..'] } },
})
