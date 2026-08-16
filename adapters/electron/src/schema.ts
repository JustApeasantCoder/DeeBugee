import { randomUUID } from "node:crypto";

export const SCHEMA_VERSION = 1 as const;
export type LogLevel = "trace" | "debug" | "info" | "warn" | "error" | "fatal";
export type LogIdentifier = string | number;

export interface LogEventV1 {
  schema_version: 1;
  timestamp: number | string;
  level: LogLevel;
  source: string;
  subsystem: string;
  target?: string;
  event: string;
  message: string;
  app_session_id: LogIdentifier;
  playback_session_id?: LogIdentifier;
  request_id?: LogIdentifier;
  session_id?: LogIdentifier;
  provider?: string;
  duration_ms?: number;
  error_kind?: string;
  status?: unknown;
  fields?: Record<string, unknown>;
}

export interface EventInput {
  level: LogLevel;
  source: string;
  subsystem: string;
  event: string;
  message: string;
  appSessionId: string;
  fields?: Record<string, unknown>;
}

export function newAppSessionId(): string {
  return randomUUID();
}

export function createEvent(input: EventInput): LogEventV1 {
  return {
    schema_version: SCHEMA_VERSION,
    timestamp: Date.now(),
    level: input.level,
    source: input.source,
    subsystem: input.subsystem,
    event: input.event,
    message: input.message,
    app_session_id: input.appSessionId,
    ...(input.fields && Object.keys(input.fields).length > 0 ? { fields: input.fields } : {}),
  };
}

export function isLogEvent(value: unknown): value is LogEventV1 {
  if (!value || typeof value !== "object") return false;
  const event = value as Partial<LogEventV1>;
  return event.schema_version === SCHEMA_VERSION
    && isTimestamp(event.timestamp)
    && isLevel(event.level)
    && nonEmpty(event.source)
    && nonEmpty(event.subsystem)
    && nonEmpty(event.event)
    && typeof event.message === "string"
    && isIdentifier(event.app_session_id);
}

function isTimestamp(value: unknown): value is number | string {
  return (typeof value === "number" && Number.isFinite(value))
    || (typeof value === "string" && !Number.isNaN(Date.parse(value)));
}

function isIdentifier(value: unknown): value is LogIdentifier {
  return (typeof value === "number" && Number.isFinite(value)) || nonEmpty(value);
}

function isLevel(value: unknown): value is LogLevel {
  return typeof value === "string"
    && ["trace", "debug", "info", "warn", "error", "fatal"].includes(value);
}

function nonEmpty(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}
