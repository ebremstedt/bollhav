import { allPages, groups, refOf } from '$lib/server/content'

export function load() {
  const pages = allPages()
  return { nav: groups(pages), order: pages.map(refOf) }
}
