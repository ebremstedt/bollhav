// The whole site is static: every route is prerendered, and URLs end with a
// slash so each page lands in its own folder, docs/CINT/index.html, which
// nginx serves without configuration.
export const prerender = true
export const trailingSlash = 'always'
