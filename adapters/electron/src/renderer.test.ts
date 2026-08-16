import assert from "node:assert/strict";
import test from "node:test";

import { ELECTRON_LOG_CHANNEL } from "./main.js";
import { createRendererLogger } from "./renderer.js";

test("renderer logger batches, redacts and infers subsystems", () => {
  const sent: unknown[][] = [];
  const logger = createRendererLogger(
    { send(channel, entries) { assert.equal(channel, ELECTRON_LOG_CHANNEL); sent.push(entries); } },
    { appSessionId: "app-1" },
  );
  const circular: Record<string, unknown> = { apiToken: "secret" };
  circular.self = circular;
  logger.log("info", "console.message", "[Player] Started", circular);
  logger.flush();

  const event = sent.flat()[0] as Record<string, any>;
  assert.equal(typeof event.timestamp, "number");
  assert.equal(event.subsystem, "player");
  assert.equal(event.fields.apiToken, "[REDACTED]");
  assert.equal(event.fields.self, "[Circular]");
});
