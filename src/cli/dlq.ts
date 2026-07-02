import type { EventEnvelope } from "../core";
import type { DlqReplayOptions } from "../core";

// ── Parsed command shapes ─────────────────────────────────────────────────────

/** DLQ topic suffix — the CLI operates on `<topic>.dlq`. */
export const DLQ_SUFFIX = ".dlq";

/** `ls` — list DLQ topics with message counts. */
export interface LsCommand {
  command: "ls";
  brokers: string[];
  /** Optional topic-name prefix filter (matched against the base topic). */
  prefix?: string;
}

/** `peek` — print the first N messages of `<topic>.dlq`. */
export interface PeekCommand {
  command: "peek";
  brokers: string[];
  /** Base topic name — the CLI reads from `<topic>.dlq`. */
  topic: string;
  /** Maximum number of messages to print. Default: 10. */
  limit: number;
}

/** `replay` — re-publish `<topic>.dlq` messages via `KafkaClient.replayDlq`. */
export interface ReplayCommand {
  command: "replay";
  brokers: string[];
  /** Base topic name — the CLI replays `<topic>.dlq`. */
  topic: string;
  /** Override destination topic (default: read from `x-dlq-original-topic`). */
  target?: string;
  /** Log what would be replayed without publishing. */
  dryRun: boolean;
  /**
   * `true`  → full replay of all DLQ messages on every call (ephemeral group).
   * `false` → incremental — only messages since the previous replay (stable group).
   */
  fromBeginning: boolean;
}

/** `help` — print usage. */
export interface HelpCommand {
  command: "help";
}

/** Discriminated union of every parsed CLI invocation. */
export type ParsedCommand = LsCommand | PeekCommand | ReplayCommand | HelpCommand;

/** Thrown by `parseArgs` when the argv is invalid. Carries the usage text. */
export class DlqUsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DlqUsageError";
  }
}

// ── Usage text ────────────────────────────────────────────────────────────────

export const USAGE = `kafka-client-dlq — dead-letter queue operations

Usage:
  kafka-client-dlq ls     --brokers <b1,b2> [--prefix <name>]
  kafka-client-dlq peek   --brokers <b1,b2> --topic <name> [--limit <n>]
  kafka-client-dlq replay --brokers <b1,b2> --topic <name> [--target <t>] [--dry-run] [--from-beginning | --incremental]

Commands:
  ls       List DLQ topics (ending in .dlq) with per-topic message counts.
  peek     Print up to N messages from <topic>.dlq (offset, x-dlq-* headers, value).
  replay   Re-publish <topic>.dlq messages to their original topic (or --target).

Options:
  --brokers <list>    Comma-separated broker addresses (required).       e.g. localhost:9092
  --prefix <name>     ls: only show DLQ topics whose base name starts with <name>.
  --topic <name>      peek/replay: base topic name (the CLI uses <name>.dlq).
  --limit <n>         peek: max messages to print (default 10).
  --target <t>        replay: override destination topic.
  --dry-run           replay: log without publishing.
  --from-beginning    replay: full replay every call (default).
  --incremental       replay: only messages added since the previous replay.
  -h, --help          Show this help.

Examples:
  kafka-client-dlq ls --brokers localhost:9092
  kafka-client-dlq ls --brokers localhost:9092 --prefix orders
  kafka-client-dlq peek --brokers localhost:9092 --topic orders.created --limit 5
  kafka-client-dlq replay --brokers localhost:9092 --topic orders.created --dry-run
  kafka-client-dlq replay --brokers localhost:9092 --topic orders.created --target orders.manual --incremental
`;

// ── Argument parsing ────────────────────────────────────────────────────────────

/** Flags that take a value (`--flag value`). */
const VALUE_FLAGS = new Set([
  "--brokers",
  "--prefix",
  "--topic",
  "--limit",
  "--target",
]);
/** Boolean flags (no value). */
const BOOL_FLAGS = new Set(["--dry-run", "--from-beginning", "--incremental"]);

interface RawFlags {
  values: Record<string, string>;
  bools: Set<string>;
}

/** Split the flag portion of argv into value flags and boolean flags. */
function parseFlags(args: string[]): RawFlags {
  const values: Record<string, string> = {};
  const bools = new Set<string>();

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith("--")) {
      throw new DlqUsageError(`Unexpected argument: "${arg}"`);
    }
    if (VALUE_FLAGS.has(arg)) {
      const value = args[i + 1];
      if (value === undefined || value.startsWith("--")) {
        throw new DlqUsageError(`Flag "${arg}" requires a value.`);
      }
      values[arg] = value;
      i++; // consume the value
    } else if (BOOL_FLAGS.has(arg)) {
      bools.add(arg);
    } else {
      throw new DlqUsageError(`Unknown flag: "${arg}"`);
    }
  }

  return { values, bools };
}

/** Parse comma-separated brokers, requiring at least one non-empty entry. */
function requireBrokers(flags: RawFlags): string[] {
  const raw = flags.values["--brokers"];
  if (raw === undefined) {
    throw new DlqUsageError("Missing required flag: --brokers");
  }
  const brokers = raw
    .split(",")
    .map((b) => b.trim())
    .filter((b) => b.length > 0);
  if (brokers.length === 0) {
    throw new DlqUsageError("--brokers must list at least one broker address.");
  }
  return brokers;
}

function requireTopic(flags: RawFlags): string {
  const topic = flags.values["--topic"];
  if (topic === undefined || topic.length === 0) {
    throw new DlqUsageError("Missing required flag: --topic");
  }
  return topic;
}

/**
 * Parse the process argv (already sliced past `node <script>`) into a
 * `ParsedCommand`. Pure and synchronous — no I/O — so it is fully unit-testable.
 *
 * @throws {DlqUsageError} on unknown commands, missing/invalid flags.
 */
export function parseArgs(argv: string[]): ParsedCommand {
  const [command, ...rest] = argv;

  if (
    command === undefined ||
    command === "-h" ||
    command === "--help" ||
    command === "help"
  ) {
    return { command: "help" };
  }

  switch (command) {
    case "ls": {
      const flags = parseFlags(rest);
      const brokers = requireBrokers(flags);
      const prefix = flags.values["--prefix"];
      return { command: "ls", brokers, prefix };
    }
    case "peek": {
      const flags = parseFlags(rest);
      const brokers = requireBrokers(flags);
      const topic = requireTopic(flags);
      const limit = parseLimit(flags.values["--limit"]);
      return { command: "peek", brokers, topic, limit };
    }
    case "replay": {
      const flags = parseFlags(rest);
      const brokers = requireBrokers(flags);
      const topic = requireTopic(flags);
      const target = flags.values["--target"];
      const dryRun = flags.bools.has("--dry-run");
      if (
        flags.bools.has("--from-beginning") &&
        flags.bools.has("--incremental")
      ) {
        throw new DlqUsageError(
          "--from-beginning and --incremental are mutually exclusive.",
        );
      }
      // Default is full replay (fromBeginning: true); --incremental opts out.
      const fromBeginning = !flags.bools.has("--incremental");
      return { command: "replay", brokers, topic, target, dryRun, fromBeginning };
    }
    default:
      throw new DlqUsageError(`Unknown command: "${command}"`);
  }
}

function parseLimit(raw: string | undefined): number {
  if (raw === undefined) return 10;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new DlqUsageError(`--limit must be a positive integer, got "${raw}".`);
  }
  return n;
}

// ── Runtime deps (injected — tests pass a fake) ──────────────────────────────────

/** Per-partition low/high watermarks for a topic. */
export interface PartitionWatermarks {
  partition: number;
  low: string;
  high: string;
}

/**
 * Minimal client surface the CLI needs. The production factory backs this with
 * a real `KafkaClient` plus its transport admin; tests inject a fake.
 */
export interface DlqCliClient {
  /** List all topics visible to the broker (via `checkStatus`). */
  listTopics(): Promise<string[]>;
  /** Per-partition low/high watermarks for a single topic. */
  fetchTopicOffsets(topic: string): Promise<PartitionWatermarks[]>;
  /** Read up to `limit` messages from `dlqTopic`, from the earliest offset. */
  peekMessages(
    dlqTopic: string,
    limit: number,
  ): Promise<Array<EventEnvelope<unknown>>>;
  /** Delegate to `KafkaClient.replayDlq`. */
  replayDlq(
    topic: string,
    options: DlqReplayOptions,
  ): Promise<{ replayed: number; skipped: number }>;
  /** Release all connections. */
  close(): Promise<void>;
}

/** Injected dependencies for `runDlqCommand`. */
export interface RunDeps {
  /** Build a connected client for the given brokers. */
  createClient(brokers: string[]): DlqCliClient | Promise<DlqCliClient>;
  /** Sink for human-readable output. Defaults are wired by the bin entrypoint. */
  out: (line: string) => void;
}

// ── Result shapes (returned so the entrypoint / tests can assert) ────────────────

export interface DlqTopicCount {
  /** The `<name>.dlq` topic. */
  dlqTopic: string;
  /** The base topic (`dlqTopic` without the `.dlq` suffix). */
  baseTopic: string;
  /** Sum over partitions of (high − low). */
  count: number;
}

export type RunResult =
  | { command: "ls"; topics: DlqTopicCount[] }
  | { command: "peek"; printed: number }
  | { command: "replay"; replayed: number; skipped: number; dryRun: boolean }
  | { command: "help" };

// ── Command runner ───────────────────────────────────────────────────────────

/** Sum (high − low) across a topic's partitions. Negative widths clamp to 0. */
export function countFromWatermarks(watermarks: PartitionWatermarks[]): number {
  let total = 0;
  for (const { low, high } of watermarks) {
    const width = Number(high) - Number(low);
    total += width > 0 ? width : 0;
  }
  return total;
}

/** Truncate a value for display, appending an ellipsis marker when cut. */
export function truncate(value: string, max = 200): string {
  if (value.length <= max) return value;
  return `${value.slice(0, max)}… (${value.length} chars)`;
}

/**
 * Execute a parsed command against injected deps.
 * Returns a structured result; all human-readable output goes through `deps.out`.
 * The client is always closed in a `finally` block.
 */
export async function runDlqCommand(
  cmd: ParsedCommand,
  deps: RunDeps,
): Promise<RunResult> {
  if (cmd.command === "help") {
    deps.out(USAGE);
    return { command: "help" };
  }

  const client = await deps.createClient(cmd.brokers);
  try {
    switch (cmd.command) {
      case "ls":
        return await runLs(cmd, client, deps);
      case "peek":
        return await runPeek(cmd, client, deps);
      case "replay":
        return await runReplay(cmd, client, deps);
    }
  } finally {
    await client.close();
  }
}

async function runLs(
  cmd: LsCommand,
  client: DlqCliClient,
  deps: RunDeps,
): Promise<RunResult> {
  const allTopics = await client.listTopics();
  let dlqTopics = allTopics.filter((t) => t.endsWith(DLQ_SUFFIX));
  if (cmd.prefix) {
    const prefix = cmd.prefix;
    dlqTopics = dlqTopics.filter((t) =>
      t.slice(0, -DLQ_SUFFIX.length).startsWith(prefix),
    );
  }
  dlqTopics.sort();

  const counts: DlqTopicCount[] = [];
  for (const dlqTopic of dlqTopics) {
    const watermarks = await client.fetchTopicOffsets(dlqTopic);
    counts.push({
      dlqTopic,
      baseTopic: dlqTopic.slice(0, -DLQ_SUFFIX.length),
      count: countFromWatermarks(watermarks),
    });
  }

  if (counts.length === 0) {
    deps.out(
      cmd.prefix
        ? `No DLQ topics found matching prefix "${cmd.prefix}".`
        : "No DLQ topics found.",
    );
  } else {
    const width = Math.max(...counts.map((c) => c.dlqTopic.length));
    deps.out(`${"TOPIC".padEnd(width)}  MESSAGES`);
    for (const c of counts) {
      deps.out(`${c.dlqTopic.padEnd(width)}  ${c.count}`);
    }
    const total = counts.reduce((s, c) => s + c.count, 0);
    deps.out(`${counts.length} DLQ topic(s), ${total} message(s) total.`);
  }

  return { command: "ls", topics: counts };
}

async function runPeek(
  cmd: PeekCommand,
  client: DlqCliClient,
  deps: RunDeps,
): Promise<RunResult> {
  const dlqTopic = `${cmd.topic}${DLQ_SUFFIX}`;
  const messages = await client.peekMessages(dlqTopic, cmd.limit);

  if (messages.length === 0) {
    deps.out(`No messages in ${dlqTopic}.`);
    return { command: "peek", printed: 0 };
  }

  deps.out(`Peeking up to ${cmd.limit} message(s) from ${dlqTopic}:`);
  let printed = 0;
  for (const env of messages) {
    if (printed >= cmd.limit) break;
    deps.out("");
    deps.out(
      `─ offset ${env.offset} · partition ${env.partition} · ${env.timestamp}`,
    );
    const dlqHeaders = Object.entries(env.headers)
      .filter(([k]) => k.startsWith("x-dlq-"))
      .sort(([a], [b]) => a.localeCompare(b));
    for (const [k, v] of dlqHeaders) {
      deps.out(`    ${k}: ${truncate(String(v), 500)}`);
    }
    deps.out(`    value: ${truncate(JSON.stringify(env.payload))}`);
    printed++;
  }

  deps.out("");
  deps.out(`Printed ${printed} message(s).`);
  return { command: "peek", printed };
}

async function runReplay(
  cmd: ReplayCommand,
  client: DlqCliClient,
  deps: RunDeps,
): Promise<RunResult> {
  const options: DlqReplayOptions = {
    dryRun: cmd.dryRun,
    fromBeginning: cmd.fromBeginning,
  };
  if (cmd.target !== undefined) options.targetTopic = cmd.target;

  const mode = cmd.fromBeginning ? "full" : "incremental";
  const targetDesc = cmd.target ? ` → ${cmd.target}` : " → original topic";
  deps.out(
    `Replaying ${cmd.topic}${DLQ_SUFFIX}${targetDesc} (${mode}${
      cmd.dryRun ? ", dry-run" : ""
    })…`,
  );

  const { replayed, skipped } = await client.replayDlq(cmd.topic, options);

  deps.out(
    cmd.dryRun
      ? `Dry-run: ${replayed} message(s) would be replayed, ${skipped} skipped.`
      : `Replayed ${replayed} message(s), ${skipped} skipped.`,
  );
  return { command: "replay", replayed, skipped, dryRun: cmd.dryRun };
}
