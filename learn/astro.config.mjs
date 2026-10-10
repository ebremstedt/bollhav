import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  // Served at bollhav.dev/learn: one App Platform app hosts docs, learn and
  // lab (see .do/app.yaml), which sets BASE_PATH=/learn at build time so every
  // asset URL gets the prefix. Unset (local dev, `npm run dev`) it is the root.
  site: "https://bollhav.dev",
  base: process.env.BASE_PATH || "/",
  vite: { plugins: [tailwindcss()] },
});
