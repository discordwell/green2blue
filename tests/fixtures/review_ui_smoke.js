// Headless smoke test for the embedded review UI logic in green2blue/review.py.
//
// Run by tests/test_review_ui.py (skipped when node is unavailable):
//   node review_ui_smoke.js <payload.json> <review_ui.js>
//
// The payload comes from ReviewSession.payload() for the standard test export
// (3 messages in 3 conversations, wizard workflow context attached). The DOM
// stub is just rich enough for the UI script; assertions cover boot, derived
// state, selection, the debounced search filter, and the workflow lifecycle.
const fs = require("fs");
const assert = require("assert");

const [, , payloadPath, uiScriptPath] = process.argv;
const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
const alerts = [];
const applyCalls = [];

function makeEl(tag) {
  return {
    tag,
    children: [],
    className: "",
    textContent: "",
    innerHTML: "",
    hidden: false,
    disabled: false,
    type: "",
    checked: false,
    indeterminate: false,
    value: "",
    href: "",
    download: "",
    listeners: {},
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    addEventListener(name, fn) {
      (this.listeners[name] = this.listeners[name] || []).push(fn);
    },
    remove() {},
    fire(name, event) {
      (this.listeners[name] || []).forEach((fn) => fn(event || { target: this }));
    },
    click() {
      this.fire("click", {});
    },
  };
}

const elements = new Map();
globalThis.document = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, makeEl(`#${id}`));
    return elements.get(id);
  },
  createElement: (tag) => makeEl(tag),
  createTextNode: (text) => ({ text }),
  body: makeEl("body"),
};
globalThis.alert = (msg) => alerts.push(String(msg));
globalThis.URL = { createObjectURL: () => "blob:x", revokeObjectURL: () => {} };
globalThis.fetch = async (url, opts) => {
  if (url === "/api/data") {
    return { ok: true, json: async () => JSON.parse(JSON.stringify(payload)) };
  }
  if (url === "/api/apply") {
    const body = JSON.parse(opts.body);
    applyCalls.push(body);
    const action =
      body.action === "cancel" ? "cancel" : body.action === "continue_full" ? "full" : "filtered";
    return { ok: true, json: async () => ({ status: "ok", action }) };
  }
  throw new Error(`unexpected fetch ${url}`);
};

let code = fs.readFileSync(uiScriptPath, "utf8");
// Expose the UI's top-level state object so assertions can reach it.
code = code.replace("const state = {", "globalThis.state = {");
eval(code);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

(async () => {
  await sleep(20); // let boot() finish

  assert.ok(state.data, "boot loaded data");
  assert.ok(state.derived, "render stored derived state");
  assert.strictEqual(state.derived.visibleConversations.length, 3, "three conversations visible");
  assert.ok(state.activeConversationId, "an active conversation was chosen");
  assert.strictEqual(
    state.activeConversationId,
    state.derived.visibleConversations[0].id,
    "active conversation matches the first visible row"
  );

  // Wizard note rendered without innerHTML (message data must stay inert).
  const note = elements.get("workflowNote");
  assert.strictEqual(note.hidden, false, "workflow note shown");
  assert.strictEqual(note.innerHTML, "", "workflow note does not use innerHTML");
  assert.ok(note.children.length >= 4, "workflow note built from DOM nodes");

  // Select all filtered messages.
  elements.get("selectAllFiltered").click();
  assert.strictEqual(state.selectedMessageIds.size, 3, "select-all selected every message");
  assert.strictEqual(state.derived.selectedConversations, 3, "selection spans 3 conversations");
  assert.strictEqual(
    state.derived.selectedCountByConversation.get(state.activeConversationId),
    1,
    "per-conversation selected counts tracked"
  );

  // Search filter (debounced 150ms).
  const search = elements.get("searchFilter");
  search.fire("input", { target: { value: "hello" } });
  assert.strictEqual(state.filters.query, "hello", "query updated immediately");
  await sleep(250);
  assert.strictEqual(state.derived.filteredMessages.length, 2, "search narrowed messages");
  assert.strictEqual(state.derived.visibleConversations.length, 2, "search narrowed conversations");
  search.fire("input", { target: { value: "" } });
  await sleep(250);
  assert.strictEqual(state.derived.filteredMessages.length, 3, "clearing search restores all");

  // Clear selection via hero chip button.
  elements.get("clearSelection").click();
  assert.strictEqual(state.selectedMessageIds.size, 0, "clear selection works");

  // Continue with full export: completes the workflow and locks the UI.
  elements.get("continueFull").click();
  await sleep(20);
  assert.deepStrictEqual(applyCalls, [{ action: "continue_full", selected_ids: [] }]);
  assert.strictEqual(state.workflowDoneAction, "full", "workflow recorded as done");
  assert.strictEqual(elements.get("exportSelected").disabled, true, "export disabled after done");
  assert.strictEqual(elements.get("continueFull").disabled, true, "continue disabled after done");
  assert.strictEqual(elements.get("cancelWorkflow").disabled, true, "cancel disabled after done");
  assert.match(
    elements.get("exportTitle").textContent,
    /Done/,
    "export bar shows completion message"
  );

  // Further clicks must not re-submit.
  elements.get("cancelWorkflow").click();
  await sleep(20);
  assert.strictEqual(applyCalls.length, 1, "no double submit after completion");
  assert.strictEqual(alerts.length, 0, `no alerts fired: ${alerts}`);

  console.log("JS SMOKE TEST OK");
})().catch((error) => {
  console.error("JS SMOKE TEST FAILED:", error && error.message ? error.message : error);
  process.exit(1);
});
