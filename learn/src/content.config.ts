import { defineCollection, z } from "astro:content";
import { glob } from "astro/loaders";

// One Markdown file per concept in src/content/concepts/.
// Frontmatter: title + short body (the card front); the Markdown body is the
// expanded explanation.
const concepts = defineCollection({
  loader: glob({ pattern: "**/*.md", base: "./src/content/concepts" }),
  schema: z.object({
    title: z.string(),
    body: z.string(),
  }),
});

export const collections = { concepts };
