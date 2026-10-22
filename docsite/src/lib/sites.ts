import { dev } from '$app/environment'

// the deck; locally, the local one (deck/ runs on 5180, see its vite config)
export const deck = dev ? 'http://localhost:5180/' : 'https://bollhav.dev/'
export const github = 'https://github.com/ebremstedt/bollhav'
