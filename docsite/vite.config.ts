import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [sveltekit()],
  // the pages live in ../docs, outside the site: let vite serve and watch them
  server: { fs: { allow: ['..'] } },
})
