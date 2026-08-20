import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { ElectronJsonlWriter } from "./main.js";

function event(fields: Record<string, unknown> = {}) {
  return {
    schema_version: 1 as const,
    timestamp: Date.now(),
    level: "info" as const,
    source: "test",
    subsystem: "logging",
    event: "test.event",
    message: "test",
    app_session_id: "app-1",
    fields,
  };
}

test("main writer serializes bigint fields", async () => {
  const root = await mkdtemp(join(tmpdir(), "dee-bugee-electron-"));
  const path = join(root, "events.jsonl");
  try {
    const writer = new ElectronJsonlWriter(path);
    await writer.writeBatch([event({ count: 42n })]);
    const parsed = JSON.parse((await readFile(path, "utf8")).trim());
    assert.equal(parsed.fields.count, "42");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("main writer recovers after one filesystem failure", async () => {
  const root = await mkdtemp(join(tmpdir(), "dee-bugee-electron-"));
  const path = join(root, "events.jsonl");
  try {
    await mkdir(path);
    const writer = new ElectronJsonlWriter(path);
    await assert.rejects(writer.writeBatch([event()]));

    await rm(path, { recursive: true });
    await writer.writeBatch([event({ attempt: 2 })]);

    const parsed = JSON.parse((await readFile(path, "utf8")).trim());
    assert.equal(parsed.fields.attempt, 2);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
