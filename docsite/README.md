# docsite

The bollhav docs at [bollhav.dev/docs](https://bollhav.dev/docs/): every
Markdown file in [docs/](../docs/) as one site. SvelteKit builds it, App
Platform serves it as the `bollhav-docs` component (see `.do/app.yaml`).

## The rule

A page exists because the file exists. Every `.md` in `docs/` is a page at
its path, titled by its first heading, and the sidebar is built from the
folders: one group per folder, a folder's `README.md` as its overview at the
folder's own URL. The groups come in the order `content.ts` lists them;
pages within a group are alphabetical, overview first. Nothing is
registered anywhere.

A link to another page stays a link. A link to any other file in the repo
goes to GitHub. A link to nothing fails the build. Images next to the pages
are served beside them.

MkDocs callouts (`!!! note "Title"`, folding `???`), fence titles
(`title="models.py"`) and mermaid fences render.

## Build

The pages are imported by the site (`src/lib/server/content.ts`), so vite
watches them. `npm run build` copies the images into `static/`, prerenders
one folder per page into `build/`, and lets Pagefind index them. `BASE_PATH=/docs` at build time puts the site under that prefix,
which is how App Platform builds it.

## Locally

```bash
cd docsite && npm install && npm run dev
```

Save a page and the browser reloads it. Search needs the built site served
statically; `npm run preview` serves everything but the search index.
