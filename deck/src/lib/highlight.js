// A tiny tokenizer for code on slides. Not a real parser: it only has to make
// keywords, strings, comments and numbers read well on a navy block.

const esc = (s) => s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[c])

const PY_KEYWORDS =
  'from|import|as|def|class|return|if|elif|else|for|in|while|with|not|and|or|is|None|True|False|' +
  'lambda|yield|try|except|finally|raise|pass|async|await|global|nonlocal|del|assert|break|continue|match|case'

const SQL_KEYWORDS =
  'select|from|where|and|or|not|in|as|join|left|right|inner|outer|on|group|by|order|limit|offset|insert|into|' +
  'values|update|set|delete|create|table|view|schema|drop|with|union|all|distinct|case|when|then|else|end|null|' +
  'is|like|between|having|exists|true|false|primary|key|references|if'

// Sticky (`y`) regexes: each is tried at the current index only. Order matters.
const RULES = {
  python: [
    ['c', /#.*/y],
    ['s', /"""[\s\S]*?"""|'''[\s\S]*?'''|[rbf]?"(?:\\.|[^"\\\n])*"|[rbf]?'(?:\\.|[^'\\\n])*'/y],
    ['d', /@[\w.]+/y],
    ['k', new RegExp(`\\b(?:${PY_KEYWORDS})\\b`, 'y')],
    ['n', /\b\d[\d_]*(?:\.\d+)?\b/y],
    ['t', /\b[A-Z][A-Za-z0-9_]*\b/y],
  ],
  bash: [
    ['c', /#.*/y],
    ['p', /^\$ /my],
    ['s', /"(?:\\.|[^"\\])*"|'[^']*'/y],
    ['v', /\$\{?\w+\}?|\b[A-Z][A-Z0-9_]+(?==)/y],
    ['k', /\b(?:export|cd|python|pip|docker|npm|uv|uvx|echo|bollhav|bollhav-gui)\b/y],
    ['n', /\b\d+\b/y],
  ],
  sql: [
    ['c', /--.*/y],
    ['s', /'[^']*'/y],
    ['k', new RegExp(`\\b(?:${SQL_KEYWORDS})\\b`, 'iy')],
    ['n', /\b\d+(?:\.\d+)?\b/y],
  ],
  plain: [],
}

const WORD = /[A-Za-z_]\w*|\s+|./y

export function highlight(code, lang = 'python') {
  const rules = RULES[lang] || RULES.plain
  let out = ''
  let i = 0
  while (i < code.length) {
    let hit = null
    for (const [cls, re] of rules) {
      re.lastIndex = i
      const m = re.exec(code)
      if (m && m[0].length) {
        hit = `<span class="tk-${cls}">${esc(m[0])}</span>`
        i += m[0].length
        break
      }
    }
    if (hit === null) {
      WORD.lastIndex = i
      const m = WORD.exec(code)
      hit = esc(m[0])
      i += m[0].length
    }
    out += hit
  }
  return out
}

// Strip the indentation a template literal picks up from the slide source,
// plus leading/trailing blank lines.
export function dedent(text) {
  const lines = text.replace(/^\n+|\s+$/g, '').split('\n')
  const indent = lines
    .filter((l) => l.trim())
    .reduce((min, l) => Math.min(min, l.match(/^ */)[0].length), Infinity)
  return lines.map((l) => l.slice(Math.min(indent, l.length))).join('\n')
}
