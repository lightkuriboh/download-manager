import assert from "node:assert/strict";
import test from "node:test";
import { actionsFor, downloadUrl } from "../dist/downloads.js";

test("preserves browser request URLs and rejects unsupported schemes and credentials", () => {
  assert.equal(downloadUrl(" https://example.org/file?token=a "), "https://example.org/file?token=a");
  for (const url of ["file:///tmp/file", "javascript:alert(1)", "https://user:pass@example.org"]) {
    assert.throws(() => downloadUrl(url));
  }
});

test("only offers controls the browser can perform", () => {
  assert.deepEqual(actionsFor({ state: "in_progress", paused: false, canResume: false }), ["pause", "cancel"]);
  assert.deepEqual(actionsFor({ state: "in_progress", paused: true, canResume: false }), ["cancel"]);
  assert.deepEqual(actionsFor({ state: "interrupted", paused: false, canResume: true }), ["resume"]);
  assert.deepEqual(actionsFor({ state: "complete", paused: false, canResume: false }), []);
});
