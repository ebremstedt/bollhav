// Markdown to HTML at build time. Beyond GitHub's rendering: a mermaid fence
// becomes a block the browser draws, a fence with a `title="…"` gets a
// caption, MkDocs admonitions (`!!! note "Title"` and the folding `???`)
// become callouts, and a relative link is resolved against the repo. A link
// to a page stays a link to that page; a link to anything else in the repo
// goes to the file on GitHub; a link to nothing fails the build.
import { existsSync, statSync } from 'node:fs'
import { join, posix } from 'node:path'
import hljs from 'highlight.js/lib/core'
import bash from 'highlight.js/lib/languages/bash'
import dockerfile from 'highlight.js/lib/languages/dockerfile'
import ini from 'highlight.js/lib/languages/ini'
import javascript from 'highlight.js/lib/languages/javascript'
import json from 'highlight.js/lib/languages/json'
import markdown from 'highlight.js/lib/languages/markdown'
import plaintext from 'highlight.js/lib/languages/plaintext'
import python from 'highlight.js/lib/languages/python'
import sql from 'highlight.js/lib/languages/sql'
import typescript from 'highlight.js/lib/languages/typescript'
import xml from 'highlight.js/lib/languages/xml'
import yaml from 'highlight.js/lib/languages/yaml'
import { base } from '$app/paths'
import { Marked, type Renderer, type Tokens, type TokenizerAndRendererExtension } from 'marked'
import { gfmHeadingId } from 'marked-gfm-heading-id'
import { markedHighlight } from 'marked-highlight'
import { github, pagesDir, repoRoot } from './paths'
import { urlFor, type Page } from './content'

for (const [name, language] of Object.entries({ bash, dockerfile, ini, javascript, json, markdown, plaintext, python, sql, typescript, xml, yaml })) {
  hljs.registerLanguage(name, language)
}
hljs.registerAliases(['sh', 'shell', 'console'], { languageName: 'bash' })
hljs.registerAliases(['text'], { languageName: 'plaintext' })

const escape = (s: string) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;')

function resolveLink(href: string, page: Page, pages: Set<string>): string {
  if (/^(?:[a-z][a-z0-9+.-]*:|#|\/)/i.test(href)) return href
  const hashAt = href.indexOf('#')
  const target = hashAt === -1 ? href : href.slice(0, hashAt)
  const hash = hashAt === -1 ? '' : href.slice(hashAt)
  // first among the pages, then anywhere in the repo
  const inPages = posix.normalize(posix.join(posix.dirname(page.path), decodeURI(target))).replace(/\/$/, '')
  if (!inPages.startsWith('..') && pages.has(inPages)) return base + urlFor(inPages) + hash
  const inRepo = posix.normalize(posix.join(pagesDir, inPages))
  if (inRepo.startsWith('..')) throw new Error(`${page.path}: the link ${href} leaves the repo`)
  const onDisk = join(repoRoot, inRepo)
  if (!existsSync(onDisk)) throw new Error(`${page.path}: the link ${href} points at nothing in the repo`)
  const kind = statSync(onDisk).isDirectory() ? 'tree' : 'blob'
  return `${github}/${kind}/main/${inRepo}${hash}`
}

// MkDocs admonitions: `!!! kind "Title"` with a body indented four spaces.
// `???` folds, closed; `???+` folds, open. Without a title the kind is the
// title.
const admonition: TokenizerAndRendererExtension = {
  name: 'admonition',
  level: 'block',
  start: (src) => src.match(/^(?:!!!|\?\?\?\+?)[ \t]/m)?.index,
  tokenizer(src) {
    const match = /^(!!!|\?\?\?\+?)[ \t]+([\w-]+)(?:[ \t]+"([^"]*)")?[ \t]*(?:\n|$)((?:(?:[ \t]*\n)|(?: {4}.*(?:\n|$)))*)/.exec(src)
    if (!match) return undefined
    const [raw, marker, kind, title, body] = match
    const text = body.replace(/^ {4}/gm, '')
    return {
      type: 'admonition',
      raw,
      kind,
      title: title ?? kind.charAt(0).toUpperCase() + kind.slice(1),
      fold: marker.startsWith('???') ? (marker === '???+' ? 'open' : 'closed') : null,
      tokens: this.lexer.blockTokens(text, []),
    }
  },
  renderer(token) {
    const body = this.parser.parse(token.tokens ?? [])
    const kind = escape(token.kind)
    const title = escape(token.title)
    if (token.fold) {
      return `<details class="admonition ${kind}"${token.fold === 'open' ? ' open' : ''}><summary>${title}</summary>\n${body}</details>\n`
    }
    return `<div class="admonition ${kind}"><p class="admonition-title">${title}</p>\n${body}</div>\n`
  },
}

export function render(page: Page, pages: Set<string>): string {
  const marked = new Marked(
    gfmHeadingId(),
    markedHighlight({
      langPrefix: 'hljs language-',
      highlight(code, lang) {
        if (lang === 'mermaid') return code
        const language = hljs.getLanguage(lang) ? lang : 'plaintext'
        return hljs.highlight(code, { language }).value
      },
    }),
    { extensions: [admonition] },
    {
      renderer: {
        // an image next to the pages is served at the same path under the
        // site; the page itself is a folder, so the src is resolved from the
        // page's path, not its URL
        image(this: Renderer, token: Tokens.Image) {
          let src = token.href
          if (!/^(?:[a-z][a-z0-9+.-]*:|\/)/i.test(src)) {
            const inPages = posix.normalize(posix.join(posix.dirname(page.path), decodeURI(src)))
            if (inPages.startsWith('..') || !existsSync(join(repoRoot, pagesDir, inPages))) {
              throw new Error(`${page.path}: the image ${src} is not next to the pages`)
            }
            src = `${base}/${inPages}`
          }
          const title = token.title ? ` title="${escape(token.title)}"` : ''
          return `<img src="${escape(src)}" alt="${escape(token.text)}"${title}>`
        },
        link(this: Renderer, token: Tokens.Link) {
          const text = this.parser.parseInline(token.tokens)
          const title = token.title ? ` title="${escape(token.title)}"` : ''
          return `<a href="${escape(resolveLink(token.href, page, pages))}"${title}>${text}</a>`
        },
        // the fence's info string: the language first, then MkDocs options
        // such as title="models.py", which becomes a caption
        code(this: Renderer, token: Tokens.Code) {
          const info = (token.lang ?? '').trim()
          const lang = info.split(/\s+/)[0] || 'plaintext'
          if (lang === 'mermaid') {
            const source = escape(token.text)
            return `<pre class="mermaid" data-src="${source}">${source}</pre>\n`
          }
          const body = token.escaped ? token.text : escape(token.text)
          const pre = `<pre><code class="hljs language-${escape(lang)}">${body}</code></pre>`
          const title = /title="([^"]*)"/.exec(info)?.[1]
          return title ? `<figure class="code"><figcaption>${escape(title)}</figcaption>${pre}</figure>\n` : `${pre}\n`
        },
      },
    },
  )
  const html = marked.parse(page.markdown, { async: false }) as string
  return html.replace(/<table>/g, '<div class="table"><table>').replace(/<\/table>/g, '</table></div>')
}
