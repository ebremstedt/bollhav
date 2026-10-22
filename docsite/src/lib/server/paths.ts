import { resolve } from 'node:path'

// vite runs with the site folder as its working directory, in the build and
// on a developer's machine alike
export const site = process.cwd()
export const contentDir = resolve(site, 'content')
// the repo, for resolving a page's links to code; and the pages' folder in it
export const repoRoot = resolve(site, '..')
export const pagesDir = 'docs'
export const github = 'https://github.com/ebremstedt/bollhav'
