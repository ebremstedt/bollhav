// The docs. Locally they are the local ones, on another port, so another
// origin with its own storage: the theme, and whether the balls have dropped
// this visit, go along in the URL. Live, both sites are bollhav.dev and share.
// the docs open on the Model overview (there is no home page: / forwards there)
export const DOCS = import.meta.env.DEV ? 'http://localhost:5190/model/' : 'https://bollhav.dev/docs/model/'
export const GITHUB = 'https://github.com/ebremstedt/bollhav'

export function docsUrl(theme) {
  if (!import.meta.env.DEV) return DOCS
  const q = new URLSearchParams()
  if (theme) q.set('theme', theme)
  if (seaDropped()) q.set('sea', 'dropped')
  const s = q.toString()
  return s ? `${DOCS}?${s}` : DOCS
}

function seaDropped() {
  try {
    return sessionStorage.getItem('sea') === 'dropped'
  } catch {
    return false
  }
}
