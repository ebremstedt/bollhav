// The pages of docs/, copied into content/ at their path there, and the
// images beside them into static/, where the built site serves them. The
// site is built from those two folders alone, so a page exists on the site
// because the file exists in docs/, and nothing is registered anywhere.
// With --watch it keeps going and copies a file again whenever it changes,
// which is what `npm run dev` uses: edit a page, reload the browser.
import { cpSync, mkdirSync, readdirSync, rmSync, statSync, watch } from 'node:fs'
import { dirname, extname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const site = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const pages = resolve(process.env.PAGES_DIR ?? resolve(site, '../docs'))
const content = join(site, 'content')
const assets = join(site, 'static')
const images = new Set(['.png', '.gif', '.jpg', '.jpeg', '.svg', '.webp'])

const skip = (name) => name.startsWith('.')
const destOf = (name) => (name.endsWith('.md') ? content : images.has(extname(name)) ? assets : null)

function copy(full) {
  const rel = relative(pages, full)
  const dest = destOf(rel)
  if (!dest) return null
  mkdirSync(dirname(join(dest, rel)), { recursive: true })
  cpSync(full, join(dest, rel))
  return rel
}

function collect() {
  rmSync(content, { recursive: true, force: true })
  rmSync(assets, { recursive: true, force: true })
  let count = 0
  function walk(dir) {
    for (const name of readdirSync(dir).sort()) {
      const full = join(dir, name)
      if (skip(name)) continue
      if (statSync(full).isDirectory()) walk(full)
      else if (copy(full)) count += 1
    }
  }
  walk(pages)
  console.log(`${count} files collected from ${pages}`)
}

collect()

if (process.argv.includes('--watch')) {
  watch(pages, { recursive: true }, (_event, file) => {
    if (!file || file.split('/').some(skip)) return
    const full = join(pages, file)
    try {
      statSync(full)
      const rel = copy(full)
      if (rel) console.log(`updated ${rel}`)
    } catch {
      const dest = destOf(file)
      if (dest) {
        rmSync(join(dest, file), { force: true })
        console.log(`removed ${file}`)
      }
    }
  })
  console.log('watching docs/ for changes')
}
