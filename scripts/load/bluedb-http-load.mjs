#!/usr/bin/env node

import { writeFile } from "node:fs/promises";

const DEFAULT_STEPS = [1, 2, 4, 8, 16, 32, 64, 128];

function usage() {
  return `Usage:
  BLUEDB_TOKEN=... node scripts/load/bluedb-http-load.mjs \\
    --base-url http://host:8080 \\
    --profiles write,read,mixed \\
    --steps 1,2,4,8,16,32,64 \\
    --duration-seconds 20 \\
    --seed-rows 2000 \\
    --markdown-report /tmp/bluedb-load.md \\
    --json-report /tmp/bluedb-load.json

Options:
  --base-url URL              Required. Public BlueDB endpoint.
  --token TOKEN               Bearer token. Defaults to BLUEDB_TOKEN.
  --tenant TENANT             Optional X-Bluedb-Tenant header.
  --table NAME                Table name. Defaults to load_<timestamp>.
  --profiles LIST            Comma list: write,read,mixed. Default: write,read,mixed.
  --mixed-write-ratio N       Write probability in mixed profile. Default: 0.50.
  --steps LIST                Comma concurrency levels. Default: ${DEFAULT_STEPS.join(",")}.
  --duration-seconds N        Measured seconds per step. Default: 15.
  --warmup-seconds N          Warmup seconds per step, excluded from metrics. Default: 2.
  --seed-rows N               Rows to seed before read/mixed profiles. Default: 1000.
  --seed-concurrency N        Seed write concurrency. Default: 32.
  --read-surface sql|tables   Read operation surface. Default: sql.
  --timeout-ms N              Per-request timeout. Default: 10000.
  --max-error-rate N          Knee threshold. Default: 0.01.
  --max-p99-ms N              Knee threshold. Default: 2000.
  --min-throughput-gain N     Knee threshold vs previous step. Default: 0.10.
  --stop-at-knee true|false   Stop each profile when knee is detected. Default: true.
  --keep-table                Do not drop the load-test table at the end.
  --json-report PATH          Write machine-readable report.
  --markdown-report PATH      Write Markdown report.
`;
}

function parseArgs(argv) {
  const out = {
    profiles: ["write", "read", "mixed"],
    steps: DEFAULT_STEPS,
    durationSeconds: 15,
    warmupSeconds: 2,
    seedRows: 1000,
    seedConcurrency: 32,
    readSurface: "sql",
    timeoutMs: 10_000,
    mixedWriteRatio: 0.5,
    maxErrorRate: 0.01,
    maxP99Ms: 2_000,
    minThroughputGain: 0.10,
    stopAtKnee: true,
    keepTable: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = () => {
      i += 1;
      if (i >= argv.length) throw new Error(`missing value for ${arg}`);
      return argv[i];
    };
    switch (arg) {
      case "--help":
      case "-h":
        console.log(usage());
        process.exit(0);
      case "--base-url":
        out.baseUrl = next().replace(/\/+$/, "");
        break;
      case "--token":
        out.token = next();
        break;
      case "--tenant":
        out.tenant = next();
        break;
      case "--table":
        out.table = next();
        break;
      case "--profiles":
        out.profiles = next().split(",").map((s) => s.trim()).filter(Boolean);
        break;
      case "--steps":
        out.steps = next().split(",").map((s) => Number.parseInt(s, 10));
        break;
      case "--duration-seconds":
        out.durationSeconds = Number.parseFloat(next());
        break;
      case "--warmup-seconds":
        out.warmupSeconds = Number.parseFloat(next());
        break;
      case "--seed-rows":
        out.seedRows = Number.parseInt(next(), 10);
        break;
      case "--seed-concurrency":
        out.seedConcurrency = Number.parseInt(next(), 10);
        break;
      case "--read-surface":
        out.readSurface = next();
        break;
      case "--timeout-ms":
        out.timeoutMs = Number.parseInt(next(), 10);
        break;
      case "--mixed-write-ratio":
        out.mixedWriteRatio = Number.parseFloat(next());
        break;
      case "--max-error-rate":
        out.maxErrorRate = Number.parseFloat(next());
        break;
      case "--max-p99-ms":
        out.maxP99Ms = Number.parseFloat(next());
        break;
      case "--min-throughput-gain":
        out.minThroughputGain = Number.parseFloat(next());
        break;
      case "--stop-at-knee":
        out.stopAtKnee = next() !== "false";
        break;
      case "--keep-table":
        out.keepTable = true;
        break;
      case "--json-report":
        out.jsonReport = next();
        break;
      case "--markdown-report":
        out.markdownReport = next();
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  out.token = out.token ?? process.env.BLUEDB_TOKEN;
  out.table = out.table ?? `load_${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14)}`;

  if (!out.baseUrl) throw new Error("--base-url is required");
  if (!out.token) throw new Error("--token or BLUEDB_TOKEN is required");
  if (!out.steps.every((n) => Number.isInteger(n) && n > 0)) {
    throw new Error("--steps must contain positive integers");
  }
  if (!["sql", "tables"].includes(out.readSurface)) {
    throw new Error("--read-surface must be sql or tables");
  }
  for (const profile of out.profiles) {
    if (!["write", "read", "mixed"].includes(profile)) {
      throw new Error(`unsupported profile: ${profile}`);
    }
  }
  return out;
}

function headers(cfg, json = false) {
  const h = {
    authorization: `Bearer ${cfg.token}`,
  };
  if (json) h["content-type"] = "application/json";
  if (cfg.tenant) h["x-bluedb-tenant"] = cfg.tenant;
  return h;
}

async function request(cfg, method, path, body = undefined) {
  const started = performance.now();
  let status = 0;
  let bytes = 0;
  let ok = false;
  let error = "";
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), cfg.timeoutMs);
    const response = await fetch(`${cfg.baseUrl}${path}`, {
      method,
      headers: headers(cfg, body !== undefined),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
    });
    clearTimeout(timer);
    status = response.status;
    const text = await response.text();
    bytes = Buffer.byteLength(text);
    ok = status >= 200 && status < 300;
    if (!ok) error = text.slice(0, 200);
    return { ok, status, bytes, latencyMs: performance.now() - started, error };
  } catch (e) {
    error = e?.name === "AbortError" ? "timeout" : String(e?.message ?? e);
    return { ok: false, status, bytes, latencyMs: performance.now() - started, error };
  }
}

async function createTable(cfg) {
  const body = {
    name: cfg.table,
    columns: [
      { name: "id", type: "INTEGER", primaryKey: true },
      { name: "worker", type: "INTEGER", nullable: false },
      { name: "seq", type: "INTEGER", nullable: false },
      { name: "bucket", type: "INTEGER", nullable: false },
      { name: "payload", type: "TEXT", nullable: false },
    ],
    indexes: [
      { name: `${cfg.table}_bucket`, columns: ["bucket"] },
    ],
  };
  const res = await request(cfg, "POST", "/schema/tables", body);
  if (!res.ok) throw new Error(`create table failed: HTTP ${res.status} ${res.error}`);
}

async function dropTable(cfg) {
  const res = await request(cfg, "DELETE", `/schema/tables/${cfg.table}`);
  if (!res.ok && res.status !== 404) {
    throw new Error(`drop table failed: HTTP ${res.status} ${res.error}`);
  }
}

function writeBody(id, worker, seq) {
  return {
    id,
    worker,
    seq,
    bucket: id % 128,
    payload: `payload-${id}-abcdefghijklmnopqrstuvwxyz0123456789`,
  };
}

async function seedRows(cfg, idState) {
  if (cfg.seedRows <= 0) return;
  const started = performance.now();
  let done = 0;
  let errors = 0;
  async function worker(workerId) {
    for (;;) {
      const seq = done;
      if (seq >= cfg.seedRows) return;
      done += 1;
      const id = idState.next++;
      const res = await request(cfg, "POST", `/tables/${cfg.table}`, writeBody(id, workerId, seq));
      if (!res.ok) errors += 1;
    }
  }
  const workers = Array.from({ length: Math.min(cfg.seedConcurrency, cfg.seedRows) }, (_, i) =>
    worker(i),
  );
  await Promise.all(workers);
  if (errors > 0) throw new Error(`seed failed: ${errors} failed writes`);
  const elapsed = (performance.now() - started) / 1000;
  console.log(`seeded ${cfg.seedRows} rows in ${elapsed.toFixed(2)}s (${(cfg.seedRows / elapsed).toFixed(1)} rows/s)`);
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

function summarize(profile, concurrency, durationSeconds, samples, previous) {
  const latencies = samples.map((s) => s.latencyMs).sort((a, b) => a - b);
  const successes = samples.filter((s) => s.ok).length;
  const errors = samples.length - successes;
  const reads = samples.filter((s) => s.op === "read" && s.ok).length;
  const writes = samples.filter((s) => s.op === "write" && s.ok).length;
  const totalBytes = samples.reduce((sum, s) => sum + s.bytes, 0);
  const statuses = {};
  for (const s of samples) statuses[s.status] = (statuses[s.status] ?? 0) + 1;
  const rps = samples.length / durationSeconds;
  const successRps = successes / durationSeconds;
  const errorRate = samples.length === 0 ? 1 : errors / samples.length;
  const gain = previous && previous.successRps > 0
    ? (successRps - previous.successRps) / previous.successRps
    : null;
  const p99 = percentile(latencies, 0.99);
  const kneeReasons = [];
  if (errorRate > previous?.cfg?.maxErrorRate) kneeReasons.push(`error-rate>${previous.cfg.maxErrorRate}`);
  if (p99 > previous?.cfg?.maxP99Ms) kneeReasons.push(`p99>${previous.cfg.maxP99Ms}ms`);
  if (gain !== null && gain < previous.cfg.minThroughputGain && p99 > previous.p99 * 1.25) {
    kneeReasons.push(`throughput-gain<${previous.cfg.minThroughputGain}`);
  }
  return {
    profile,
    concurrency,
    requests: samples.length,
    successes,
    errors,
    errorRate,
    rps,
    successRps,
    readRps: reads / durationSeconds,
    writeRps: writes / durationSeconds,
    mbps: (totalBytes / durationSeconds) / (1024 * 1024),
    avgMs: latencies.reduce((a, b) => a + b, 0) / Math.max(1, latencies.length),
    p50: percentile(latencies, 0.50),
    p95: percentile(latencies, 0.95),
    p99,
    maxMs: latencies.at(-1) ?? 0,
    statuses,
    throughputGain: gain,
    knee: kneeReasons.length > 0,
    kneeReasons,
  };
}

async function runStep(cfg, profile, concurrency, idState) {
  const warmupUntil = performance.now() + cfg.warmupSeconds * 1000;
  const end = warmupUntil + cfg.durationSeconds * 1000;
  const samples = [];
  const seededMaxId = cfg.seedRows;

  async function worker(workerId) {
    let seq = 0;
    while (performance.now() < end) {
      const measured = performance.now() >= warmupUntil;
      const doWrite =
        profile === "write" || (profile === "mixed" && Math.random() < cfg.mixedWriteRatio);
      let res;
      if (doWrite) {
        const id = idState.next++;
        res = await request(cfg, "POST", `/tables/${cfg.table}`, writeBody(id, workerId, seq));
        res.op = "write";
      } else {
        const id = 1 + Math.floor(Math.random() * Math.max(1, seededMaxId));
        if (cfg.readSurface === "sql") {
          res = await request(cfg, "POST", "/sql", {
            sql: `SELECT id, payload FROM ${cfg.table} WHERE id = $1;`,
            params: [id],
          });
        } else {
          res = await request(cfg, "GET", `/tables/${cfg.table}?id=eq.${id}&select=id,payload`);
        }
        res.op = "read";
      }
      seq += 1;
      if (measured) samples.push(res);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
  return samples;
}

async function runProfile(cfg, profile, idState) {
  const rows = [];
  let previous = null;
  for (const concurrency of cfg.steps) {
    const samples = await runStep(cfg, profile, concurrency, idState);
    const summary = summarize(profile, concurrency, cfg.durationSeconds, samples, {
      ...(previous ?? {}),
      cfg,
    });
    rows.push(summary);
    previous = summary;
    console.log(formatRow(summary));
    if (cfg.stopAtKnee && summary.knee) break;
  }
  return rows;
}

function formatMs(n) {
  return n.toFixed(n >= 100 ? 0 : n >= 10 ? 1 : 2);
}

function formatRow(r) {
  const knee = r.knee ? ` knee=${r.kneeReasons.join("+")}` : "";
  return [
    r.profile.padEnd(5),
    `c=${String(r.concurrency).padStart(3)}`,
    `ok/s=${r.successRps.toFixed(1).padStart(8)}`,
    `read/s=${r.readRps.toFixed(1).padStart(8)}`,
    `write/s=${r.writeRps.toFixed(1).padStart(8)}`,
    `err=${(r.errorRate * 100).toFixed(2).padStart(6)}%`,
    `p50=${formatMs(r.p50).padStart(6)}ms`,
    `p95=${formatMs(r.p95).padStart(6)}ms`,
    `p99=${formatMs(r.p99).padStart(6)}ms`,
    knee,
  ].join(" ");
}

function markdownReport(report) {
  const lines = [];
  lines.push(`# BlueDB External Load Test`);
  lines.push("");
  lines.push(`- Base URL: \`${report.baseUrl}\``);
  lines.push(`- Table: \`${report.table}\``);
  lines.push(`- Duration per step: ${report.durationSeconds}s measured + ${report.warmupSeconds}s warmup`);
  lines.push(`- Seed rows: ${report.seedRows}`);
  lines.push(`- Read surface: \`${report.readSurface}\``);
  lines.push(`- Started: ${report.startedAt}`);
  lines.push(`- Finished: ${report.finishedAt}`);
  lines.push("");
  for (const [profile, rows] of Object.entries(report.profiles)) {
    lines.push(`## ${profile}`);
    lines.push("");
    lines.push("| concurrency | ok/s | read/s | write/s | err % | p50 ms | p95 ms | p99 ms | max ms | knee |");
    lines.push("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");
    for (const r of rows) {
      lines.push(`| ${r.concurrency} | ${r.successRps.toFixed(1)} | ${r.readRps.toFixed(1)} | ${r.writeRps.toFixed(1)} | ${(r.errorRate * 100).toFixed(2)} | ${formatMs(r.p50)} | ${formatMs(r.p95)} | ${formatMs(r.p99)} | ${formatMs(r.maxMs)} | ${r.knee ? r.kneeReasons.join(", ") : ""} |`);
    }
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  const cfg = parseArgs(process.argv.slice(2));
  const startedAt = new Date().toISOString();
  const idState = { next: 1 };
  const report = {
    baseUrl: cfg.baseUrl,
    table: cfg.table,
    profilesRequested: cfg.profiles,
    steps: cfg.steps,
    durationSeconds: cfg.durationSeconds,
    warmupSeconds: cfg.warmupSeconds,
    seedRows: cfg.seedRows,
    readSurface: cfg.readSurface,
    startedAt,
    profiles: {},
  };

  console.log(`base=${cfg.baseUrl} table=${cfg.table} profiles=${cfg.profiles.join(",")} steps=${cfg.steps.join(",")}`);
  await createTable(cfg);
  try {
    if (cfg.profiles.some((p) => p === "read" || p === "mixed")) {
      await seedRows(cfg, idState);
    }
    for (const profile of cfg.profiles) {
      console.log(`\nprofile=${profile}`);
      report.profiles[profile] = await runProfile(cfg, profile, idState);
    }
  } finally {
    if (!cfg.keepTable) {
      await dropTable(cfg);
    }
  }

  report.finishedAt = new Date().toISOString();
  if (cfg.jsonReport) {
    await writeFile(cfg.jsonReport, `${JSON.stringify(report, null, 2)}\n`);
  }
  if (cfg.markdownReport) {
    await writeFile(cfg.markdownReport, markdownReport(report));
  }
}

main().catch((e) => {
  console.error(e?.stack ?? e);
  process.exit(1);
});
