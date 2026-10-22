import { error, redirect } from '@sveltejs/kit'
import { base } from '$app/paths'
import { building } from '$app/environment'
import { allPages, pageFor } from '$lib/server/content'
import { render } from '$lib/server/markdown'

// one entry per Markdown file, so every page is prerendered whether or not
// something links to it; a folder's README is served at the folder. The root
// is prerendered too, as the page that forwards to the Model overview
export const entries = () => [
  { path: '' },
  ...allPages().map((p) => ({ path: p.path.replace(/\/README\.md$/, '').replace(/\.md$/, '') })),
]

export function load({ params, url }) {
  // there is no home page: the docs open on the Model overview. In dev the
  // query goes along (the theme travels in it); a prerender has no query
  if (params.path === '') redirect(308, `${base}/model/${building ? '' : url.search}`)
  const page = pageFor(params.path)
  if (!page) error(404, 'No such page')
  const known = new Set(allPages().map((p) => p.path))
  return { title: page.title, path: page.path, source: page.source, html: render(page, known) }
}
