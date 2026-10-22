// Draws every mermaid block inside the node, and draws them again when the
// theme changes or the page does. The block keeps its source in data-src, so
// a redraw starts from the text, not from the previous SVG. Mermaid is loaded
// on the client only, the first time a page needs it.
import { currentTheme } from './theme'

type Mermaid = typeof import('mermaid').default

export function mermaidDiagrams(node: HTMLElement, path: string) {
  let mermaid: Mermaid | undefined
  let pending = 0

  async function draw() {
    const blocks = Array.from(node.querySelectorAll<HTMLElement>('pre.mermaid'))
    if (blocks.length === 0) return
    const run = ++pending
    mermaid ??= (await import('mermaid')).default
    if (run !== pending) return
    for (const block of blocks) {
      block.textContent = block.dataset.src ?? block.textContent
      block.removeAttribute('data-processed')
    }
    // Mermaid sizes a node around its label as measured when it draws. The
    // page font arrives a moment after the first paint, so wait for it and
    // draw with it, or the boxes come out sized for the fallback font and
    // the labels get clipped.
    await document.fonts.ready
    if (run !== pending) return
    const fontFamily = getComputedStyle(document.body).fontFamily
    mermaid.initialize({
      startOnLoad: false,
      theme: currentTheme() === 'dark' ? 'dark' : 'default',
      fontFamily,
      themeVariables: { fontFamily },
    })
    await mermaid.run({ nodes: blocks })
  }

  const observer = new MutationObserver(() => void draw())
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })
  const media = matchMedia('(prefers-color-scheme: dark)')
  const onMedia = () => void draw()
  media.addEventListener('change', onMedia)
  void draw()

  return {
    update(next: string) {
      if (next !== path) {
        path = next
        void draw()
      }
    },
    destroy() {
      observer.disconnect()
      media.removeEventListener('change', onMedia)
    },
  }
}
