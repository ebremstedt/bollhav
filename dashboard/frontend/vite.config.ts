import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

// Proxy /api/* to the FastAPI backend during `pnpm dev` so the frontend
// can hit the backend on the same origin. The backend serves the built
// `dist/` in production, so there's no proxy needed once you `pnpm build`.
export default defineConfig({
  plugins: [svelte()],
  server: {
    port: 5174,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:5173",
        changeOrigin: true,
      },
    },
  },
});
