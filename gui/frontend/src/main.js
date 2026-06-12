import { mount } from "svelte";
import "@xyflow/svelte/dist/style.css";
import "./theme.css";
import App from "./App.svelte";

export default mount(App, { target: document.getElementById("app") });
