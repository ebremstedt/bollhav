// The deck's pen for rough.js: the stroke, the fills and the shapes in one
// place, so every sketched drawing looks the same (RoughScene, StagingStory).
//
//   const draw = sketcher(svgElement)
//   for (const el of draw.table(20, 20, 230, 180, { seed: 3 })) g.appendChild(el)
//
// Every drawer returns the <g> elements to append. Strokes and fills are
// currentColor, so colours come from CSS and follow the theme; the fill path
// is the first child of a 'tint' or 'hatch' group, and the caller fades it:
//
//   .tint  > path:first-child:not(:only-child) { opacity: 0.14 }
//   .hatch > path:first-child:not(:only-child) { opacity: 0.55 }
//   .line { opacity: 0.45 }
//   .dotted path { stroke-linecap: round; stroke-width: 3 }
import rough from 'roughjs'

export const pen = { roughness: 1.3, bowing: 1.1, strokeWidth: 2.2, stroke: 'currentColor' }
export const tint = { ...pen, fill: 'currentColor', fillStyle: 'solid' }
export const hatch = { ...pen, fill: 'currentColor', fillStyle: 'hachure', hachureGap: 8, fillWeight: 1 }
export const RY = 16 // a cylinder's lid

export function sketcher(svg) {
  const rc = rough.svg(svg)
  const tag = (el, ...cls) => {
    el.classList.add(...cls)
    return el
  }

  return {
    // an outline, optionally dashed or dotted (dots are round: see .dotted in the CSS)
    frame(x, y, w, h, { seed = 1, dashed = false, dotted = false } = {}) {
      const dash = dotted ? [0.1, 7] : dashed ? [10, 8] : undefined
      const opts = { ...pen, seed, roughness: 1.6, strokeLineDash: dash }
      return [tag(rc.rectangle(x, y, w, h, opts), 'frame', ...(dotted ? ['dotted'] : []))]
    },

    // a tinted rectangle
    box(x, y, w, h, { seed = 1 } = {}) {
      return [tag(rc.rectangle(x, y, w, h, { ...tint, seed }), 'tint')]
    },

    // a database: tinted body, hatched lid
    cylinder(x, y, w, h, { seed = 1, ry = RY } = {}) {
      const rx = w / 2
      const top = y + ry
      const bottom = y + h - ry
      const body = `M${x} ${top} L${x} ${bottom} A${rx} ${ry} 0 0 0 ${x + w} ${bottom} L${x + w} ${top} A${rx} ${ry} 0 0 1 ${x} ${top} Z`
      return [
        tag(rc.path(body, { ...tint, seed }), 'tint'),
        tag(rc.ellipse(x + rx, top, w, ry * 2, { ...hatch, seed: seed + 1 }), 'hatch'),
      ]
    },

    // a table: tinted, a hatched header row, then rows and columns ruled
    table(x, y, w, h, { seed = 1, header = 30, rows = 5, cols = 3 } = {}) {
      const thin = { ...pen, strokeWidth: 1.4, roughness: 1 }
      const els = [
        tag(rc.rectangle(x, y, w, h, { ...tint, seed }), 'tint'),
        tag(rc.rectangle(x, y, w, header, { ...hatch, seed: seed + 1 }), 'hatch'),
      ]
      const rowH = (h - header) / rows
      for (let r = 1; r < rows; r++) {
        const ly = y + header + r * rowH
        els.push(tag(rc.line(x, ly, x + w, ly, { ...thin, seed: seed + 10 + r }), 'line'))
      }
      for (let c = 1; c < cols; c++) {
        const lx = x + (w * c) / cols
        els.push(tag(rc.line(lx, y + header, lx, y + h, { ...thin, seed: seed + 20 + c }), 'line'))
      }
      return els
    },

    // a line with a head at its end
    arrow(x1, y1, x2, y2, { seed = 1 } = {}) {
      const angle = Math.atan2(y2 - y1, x2 - x1)
      const wing = (da) => [x2 - 16 * Math.cos(angle + da), y2 - 16 * Math.sin(angle + da)]
      return [
        tag(rc.line(x1, y1, x2, y2, { ...pen, seed }), 'arrow'),
        tag(rc.linearPath([wing(0.5), [x2, y2], wing(-0.5)], { ...pen, seed: seed + 1 }), 'arrow'),
      ]
    },
  }
}
