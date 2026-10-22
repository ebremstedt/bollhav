// Whether the page list is open as a drawer, on screens too narrow for the
// sidebar column.
export const menu = $state({ open: false })

// the home page's sea: how many balls (0..1 of the full pile) and how stormy.
// The sliders in the bottom bar set it; the page's sea follows
export const sea = $state({ density: 0.6, storm: 0.5 })
