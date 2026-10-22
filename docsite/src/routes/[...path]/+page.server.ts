import { error } from '@sveltejs/kit'
import { allPages, pageFor } from '$lib/server/content'
import { render } from '$lib/server/markdown'

// one entry per Markdown file, so every page is prerendered whether or not
// something links to it; a folder's README is served at the folder
export const entries = () =>
  allPages().map((p) => ({
    path: p.path === 'README.md' ? '' : p.path.replace(/\/README\.md$/, '').replace(/\.md$/, ''),
  }))

export function load({ params }) {
  const page = pageFor(params.path)
  if (!page) error(404, 'No such page')
  const known = new Set(allPages().map((p) => p.path))
  return { title: page.title, path: page.path, source: page.source, html: render(page, known) }
}
