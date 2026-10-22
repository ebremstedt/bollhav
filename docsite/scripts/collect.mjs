// The images of docs/, copied into static/ at their path there, where the
// built site serves them beside the pages. (The pages themselves are
// imported by the site, see src/lib/server/content.ts, so vite watches them
// and a save reloads the page in dev.) With --watch it keeps going and
// copies an image again whenever it changes.
import { cpSync, mkdirSync, readdirSync, rmSync, statSync, watch } from 'node:fs'
import { dirname, extname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const site = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const pages = resolve(process.env.PAGES_DIR ?? resolve(site, '../docs'))
const assets = join(site, 'static')
const images = new Set(['.png', '.gif', '.jpg', '.jpeg', '.svg', '.webp'])

const skip = (name) => name.startsWith('.')
const destOf = (name) => (images.has(extname(name)) ? assets : null)

function copy(full) {
  const rel = relative(pages, full)
  const dest = destOf(rel)
  if (!dest) return null
  mkdirSync(dirname(join(dest, rel)), { recursive: true })
  cpSync(full, join(dest, rel))
  return rel
}

function collect() {
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
  console.log(`${count} images collected from ${pages}`)
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
  console.log('watching docs/ for image changes')
}
