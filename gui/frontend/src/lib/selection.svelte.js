// Shared runes state: the selected model full_name (or null) and which side-
// panel tab to show ("state" | "errors"). Set by clicking a model node or its
// state / errors buttons; the right side panel reads it to load runs + errors.
export const selection = $state({ name: null, tab: "state" });

// The model whose metadata is shown in the LEFT panel (or null). Set by
// clicking a node's ⓘ badge; MetaPanel reads it and fetches /model/{name}.
export const info = $state({ name: null });
