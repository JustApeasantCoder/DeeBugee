import { appendFile, mkdir, rename, rm, stat } from "node:fs/promises";
import { dirname } from "node:path";

import { type LogEventV1, isLogEvent } from "./schema.js";

export const ELECTRON_LOG_CHANNEL = "debug-logging-toolkit:batch";

export interface IpcMainLike {
  on(channel: string, listener: (event: unknown, entries: unknown) => void): void;
}

export interface WriterOptions {
  rotationBytes?: number;
  archiveCount?: number;
  maximumBatchEvents?: number;
  maximumBatchBytes?: number;
}

export class ElectronJsonlWriter {
  readonly path: string;
  private readonly rotationBytes: number;
  private readonly archiveCount: number;
  private readonly maximumBatchEvents: number;
  private readonly maximumBatchBytes: number;
  private pending: Promise<void> = Promise.resolve();
  private rejectedEvents = 0;

  constructor(path: string, options: WriterOptions = {}) {
    this.path = path;
    this.rotationBytes = options.rotationBytes ?? 50 * 1024 * 1024;
    this.archiveCount = options.archiveCount ?? 4;
    this.maximumBatchEvents = options.maximumBatchEvents ?? 512;
    this.maximumBatchBytes = options.maximumBatchBytes ?? 2 * 1024 * 1024;
  }

  writeBatch(input: unknown): Promise<void> {
    const supplied = Array.isArray(input) ? input : [];
    const entries = supplied.slice(0, this.maximumBatchEvents);
    this.rejectedEvents += supplied.length - entries.length;
    const valid = entries.filter(isLogEvent);
    this.rejectedEvents += entries.length - valid.length;

    let payload = valid.map((event) => JSON.stringify(event)).join("\n");
    if (payload.length > 0) payload += "\n";
    if (Buffer.byteLength(payload) > this.maximumBatchBytes) {
      this.rejectedEvents += valid.length;
      return Promise.resolve();
    }

    this.pending = this.pending.then(async () => {
      await mkdir(dirname(this.path), { recursive: true });
      if (this.rejectedEvents > 0) {
        const rejected = this.rejectedEvents;
        const template = valid.at(-1);
        if (template) {
          this.rejectedEvents = 0;
          const overflow: LogEventV1 = {
            schema_version: 1,
            timestamp: Date.now(),
            level: "warn",
            source: "electron_main",
            subsystem: "logging",
            event: "logger.events_rejected",
            message: `Rejected ${rejected} invalid or oversized renderer log events`,
            app_session_id: template.app_session_id,
            fields: { count: rejected },
          };
          payload = `${JSON.stringify(overflow)}\n${payload}`;
        }
      }
      if (!payload) return;
      await this.rotateIfNeeded(Buffer.byteLength(payload));
      await appendFile(this.path, payload, "utf8");
    });
    return this.pending;
  }

  flush(): Promise<void> {
    return this.pending;
  }

  private async rotateIfNeeded(incomingBytes: number): Promise<void> {
    const currentBytes = await stat(this.path).then((value) => value.size, () => 0);
    if (currentBytes === 0 || currentBytes + incomingBytes <= this.rotationBytes) return;

    if (this.archiveCount === 0) {
      await rm(this.path, { force: true });
      return;
    }
    await rm(`${this.path}.${this.archiveCount}`, { force: true });
    for (let generation = this.archiveCount - 1; generation >= 1; generation -= 1) {
      await rename(`${this.path}.${generation}`, `${this.path}.${generation + 1}`).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error;
      });
    }
    await rename(this.path, `${this.path}.1`).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

export function installElectronLogging(
  ipcMain: IpcMainLike,
  path: string,
  options: WriterOptions = {},
): ElectronJsonlWriter {
  const writer = new ElectronJsonlWriter(path, options);
  ipcMain.on(ELECTRON_LOG_CHANNEL, (_event, entries) => {
    void writer.writeBatch(entries);
  });
  return writer;
}
