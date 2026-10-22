// <html data-theme> wins, otherwise the system preference applies. Which
// icon shows is decided in CSS; the diagrams listen for the change.
export const currentTheme = (): 'light' | 'dark' => {
  const set = document.documentElement.dataset.theme
  if (set === 'dark' || set === 'light') return set
  return matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export function toggleTheme() {
  const next = currentTheme() === 'dark' ? 'light' : 'dark'
  document.documentElement.dataset.theme = next
  try {
    localStorage.setItem('theme', next)
  } catch {}
}
