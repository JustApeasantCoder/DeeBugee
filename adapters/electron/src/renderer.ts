import {
  type LogEventV1,
  type LogLevel,
  createEvent,
} from "./schema.js";
import { ELECTRON_LOG_CHANNEL } from "./main.js";

export interface IpcRendererLike {
  send(channel: string, entries: LogEventV1[]): void;
}
export interface RendererLoggerOptions {
  appSessionId: string;
  source?: string;
  flushIntervalMs?: number;
  maximumQueueEvents?: number;
  maximumBatchEvents?: number;
}

export interface RendererLogger {
  log(
    level: LogLevel,
    event: string,
    message: string,
    fields?: Record<string, unknown>,
    subsystem?: string,
  ): void;
  flush(): void;
  captureConsole(): () => void;
}

export function createRendererLogger(
  ipcRenderer: IpcRendererLike,
  options: RendererLoggerOptions,
): RendererLogger {
  const queue: LogEventV1[] = [];
  const source = options.source ?? "electron_renderer";
  const maximumQueueEvents = options.maximumQueueEvents ?? 10_000;
  const maximumBatchEvents = options.maximumBatchEvents ?? 256;
  let dropped = 0;

  const flush = (): void => {
    if (dropped > 0 && queue.length < maximumQueueEvents) {
      queue.push(createEvent({
        level: "warn",
        source,
        subsystem: "logging",
        event: "logger.events_dropped",
        message: `Dropped ${dropped} renderer log events because the queue was full`,
        appSessionId: options.appSessionId,
        fields: { count: dropped },
      }));
      dropped = 0;
    }
    while (queue.length > 0) {
      ipcRenderer.send(ELECTRON_LOG_CHANNEL, queue.splice(0, maximumBatchEvents));
    }
  };

  const logger: RendererLogger = {
    log(level, event, message, fields = {}, subsystem = inferSubsystem(message)) {
      if (queue.length >= maximumQueueEvents) {
        dropped += 1;
        return;
      }
      queue.push(createEvent({
        level,
        source,
        subsystem,
        event,
        message,
        appSessionId: options.appSessionId,
        fields: safeFields(fields),
      }));
    },
    flush,
    captureConsole() {
      const methods = ["debug", "log", "info", "warn", "error"] as const;
      const originals = new Map<string, (...args: unknown[]) => void>();
      for (const method of methods) {
        const original = console[method].bind(console) as (...args: unknown[]) => void;
        originals.set(method, original);
        console[method] = ((...args: unknown[]) => {
          original(...args);
          const message = args.map(formatValue).join(" ");
          logger.log(consoleLevel(method), "console.message", message, { arguments: args });
        }) as typeof console[typeof method];
      }
      return () => {
        for (const method of methods) {
          const original = originals.get(method);
          if (original) console[method] = original as typeof console[typeof method];
        }
      };
    },
  };

  const timer = globalThis.setInterval(flush, options.flushIntervalMs ?? 100);
  if (typeof timer === "object" && "unref" in timer) timer.unref();
  globalThis.addEventListener?.("pagehide", flush);
  return logger;
}

function consoleLevel(method: "debug" | "log" | "info" | "warn" | "error"): LogLevel {
  if (method === "log") return "info";
  return method;
}

function inferSubsystem(message: string): string {
  const match = /^\[([^\]]+)]/.exec(message);
  return match?.[1]?.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_") || "renderer";
}

function safeFields(fields: Record<string, unknown>): Record<string, unknown> {
  return JSON.parse(JSON.stringify(fields, circularReplacer())) as Record<string, unknown>;
}

function circularReplacer(): (key: string, value: unknown) => unknown {
  const seen = new WeakSet<object>();
  return (key, value) => {
    if (isSensitiveKey(key)) return "[REDACTED]";
    if (value instanceof Error) {
      return { name: value.name, message: value.message, stack: value.stack };
    }
    if (value && typeof value === "object") {
      if (seen.has(value)) return "[Circular]";
      seen.add(value);
    }
    return value;
  };
}

function isSensitiveKey(key: string): boolean {
  return /token|secret|api.?key|authorization|cookie|passkey|signature|magnet/i.test(key);
}

function formatValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Error) return `${value.name}: ${value.message}`;
  try {
    return JSON.stringify(value, circularReplacer());
  } catch {
    return String(value);
  }
}
