import { dev } from '$app/environment'
import { currentTheme } from '$lib/theme'

// The deck. Locally it is the local one, on another port, so another origin
// with its own storage: the theme, and whether the balls have dropped this
// visit, go along in the URL. Live, both sites are bollhav.dev and share.
export const deck = dev ? 'http://localhost:5180/' : 'https://bollhav.dev/'
export const github = 'https://github.com/ebremstedt/bollhav'

export function deckUrl() {
  if (!dev) return deck
  const q = new URLSearchParams({ theme: currentTheme() })
  try {
    if (sessionStorage.getItem('sea') === 'dropped') q.set('sea', 'dropped')
  } catch {}
  return `${deck}?${q}`
}

export const toDeck = (e: MouseEvent) => {
  if (!dev) return
  e.preventDefault()
  location.href = deckUrl()
}
