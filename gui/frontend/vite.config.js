import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

// Proxy the bollhav lineage JSON endpoints to the FastAPI app so the
// frontend can fetch them same-origin (no CORS). In Docker these are set to
// the backend service; locally they default to localhost.
const API = process.env.VITE_API_TARGET || "http://127.0.0.1:8137";
const HOST = process.env.VITE_HOST || "127.0.0.1";

export default defineConfig({
  plugins: [svelte()],
  server: {
    host: HOST,
    port: 5173,
    strictPort: true,
    proxy: {
      "/graph": API,
      "/tree": API,
      "/lineage": API,
      "/state": API,
      "/models": API,
      "/model": API,
      "/downstreams": API,
      "/errors": API,
    },
  },
});
