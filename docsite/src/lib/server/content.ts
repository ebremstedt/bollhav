// The pages of the site, read from content/ where collect.mjs put every
// Markdown file of docs/ at its path. The sidebar is derived from the
// folders, a page's title from its first heading, and the reading order from
// the two together: nothing is written by hand except the order of the groups.
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, posix, relative } from 'node:path'
import { contentDir, pagesDir } from './paths'

export type Page = { path: string; url: string; title: string; group: string; markdown: string; source: string }
export type PageRef = { url: string; title: string }
// name is the folder, unique; label is what the sidebar prints
export type Group = { name: string; label: string; parent: string; pages: PageRef[] }

// the sidebar order; a folder not listed comes after these, alphabetically
const order = ['', 'model', 'concepts', 'backends', 'examples', 'articles', 'about']

// a folder's README.md is its overview and lives at the folder's own URL
export const urlFor = (path: string) => {
  if (path === 'README.md') return '/'
  if (path.endsWith('/README.md')) return `/${posix.dirname(path)}/`
  return `/${path.replace(/\.md$/, '')}/`
}

// the first heading of any level; a page with none is named after its file.
// The home page's first heading is a section, so it is the site's name.
function titleOf(markdown: string, path: string): string {
  if (path === 'README.md') return 'bollhav'
  const match = markdown.match(/^#{1,6}\s+(.+?)\s*#*\s*$/m)
  return match ? match[1].replace(/`/g, '') : posix.basename(path, '.md')
}

const groupOf = (path: string) => (path.includes('/') ? path.split('/')[0] : '')

function* walk(dir: string): Generator<string, void, void> {
  for (const name of readdirSync(dir).sort()) {
    const full = join(dir, name)
    if (statSync(full).isDirectory()) yield* walk(full)
    else if (name.endsWith('.md')) yield relative(contentDir, full).split('\\').join('/')
  }
}

const groupRank = (group: string) => {
  const at = order.indexOf(group)
  return at === -1 ? order.length : at
}

const pageRank = (page: Page) => {
  const rest = page.group ? page.path.slice(page.group.length + 1) : page.path
  return [rest === 'README.md' ? 0 : 1, rest.toLowerCase()] as const
}

const compare = (a: Page, b: Page) => {
  if (a.group !== b.group) {
    const byRank = groupRank(a.group) - groupRank(b.group)
    return byRank || a.group.localeCompare(b.group)
  }
  const [ra, ka] = pageRank(a)
  const [rb, kb] = pageRank(b)
  return ra - rb || ka.localeCompare(kb)
}

/** Every page in reading order: home, then the groups in `order`, each with its overview first. */
export function allPages(): Page[] {
  const pages: Page[] = []
  for (const path of walk(contentDir)) {
    const markdown = readFileSync(join(contentDir, path), 'utf8')
    pages.push({
      path,
      url: urlFor(path),
      title: titleOf(markdown, path),
      group: groupOf(path),
      markdown,
      source: posix.join(pagesDir, path),
    })
  }
  return pages.sort(compare)
}

export function pageFor(param: string): Page | undefined {
  const clean = param.replace(/\/+$/, '')
  const pages = allPages()
  if (clean === '') return pages.find((p) => p.path === 'README.md')
  return pages.find((p) => p.path === `${clean}.md`) ?? pages.find((p) => p.path === `${clean}/README.md`)
}

export const refOf = ({ url, title }: Page): PageRef => ({ url, title })

export function groups(pages: Page[]): Group[] {
  const out: Group[] = []
  for (const page of pages) {
    const name = page.group
    const last = out[out.length - 1]
    if (last && last.name === name) last.pages.push(refOf(page))
    else out.push({ name, label: name, parent: '', pages: [refOf(page)] })
  }
  return out
}
