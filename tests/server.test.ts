import assert from "node:assert/strict";
import test from "node:test";
import Database from "better-sqlite3";
import type { BbPluginApi } from "@get-bb/plugin-sdk";
import plugin from "../src/server.ts";
import type { Recap, RecapSettings } from "../src/server.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

async function createHarness(options: {
  timeline?: (args: { threadId: string; beforeAnchorSeq?: string }) => Promise<unknown>;
  wait?: (threadId: string) => Promise<void>;
  output?: (threadId: string) => Promise<string | null>;
  archive?: (threadId: string) => Promise<void>;
  stop?: (threadId: string) => Promise<void>;
  database?: Database.Database;
  listThreads?: (args: { parentThreadId?: string }) => Promise<unknown[]>;
  settings?: Partial<RecapSettings>;
  legacySettings?: boolean;
  provider?: { id: string; capabilities?: { experimental_supportsDisposableSessions?: unknown }; serviceTiers?: Array<{ id: string }> };
  spawnResponse?: (index: number) => Promise<{ id: string; experimental_providerSessionLifetime?: unknown }>;
  environmentId?: string;
  modelSelection?: { providerId: string; model: string; reasoningLevel: string; serviceTier?: string };
} = {}) {
  const db = options.database ?? new Database(":memory:");
  const settings: RecapSettings = { auto: true, autoCleanup: true, disposableWorkers: false, afterSeconds: 86_400, minTurns: 1, maxConcurrent: 1, displayMode: "Compact banner", prompt: "Return a recap.", ...options.settings };
  const { disposableWorkers, ...legacySettings } = settings;
  const events = new Map<string, (payload: unknown) => void>();
  const spawned: Array<Parameters<BbPluginApi["sdk"]["threads"]["spawn"]>[0] & { prompt: string; experimental_providerSessionLifetime?: "persistent" | "disposable" }> = [];
  const modelQueries: unknown[] = [];
  const stored = new Map<string, unknown>();
  const warnings: string[] = [];
  let dispose!: () => Promise<void>;
  let handlers!: {
    recap_generate: (input: { threadId: string; automatic: boolean }) => Promise<{ recap: Recap | null; reason: string | null; generated: boolean }>;
    recap_get: (input: { threadId: string }) => Promise<{ recap: Recap | null; generating: boolean }>;
    recap_settings_get: () => Promise<RecapSettings>;
    recap_settings_set: (input: RecapSettings) => Promise<RecapSettings>;
    recap_display_mode_set: (input: { displayMode: RecapSettings["displayMode"] }) => Promise<{ displayMode: RecapSettings["displayMode"] }>;
  };
  const thread = (id: string) => ({ id, projectId: "p", environmentId: options.environmentId ?? null, providerId: "p", status: "idle", visibility: "visible" as const, originPluginId: null, originKind: null, parentThreadId: null as string | null });
  const bb = {
    pluginId: "bb-recap",
    storage: {
      database: () => db,
      migrate: (_: unknown, statements: string[]) => statements.forEach((statement) => db.exec(statement)),
      kv: { get: async (key: string) => key === "settings" ? options.legacySettings ? legacySettings : settings : options.modelSelection ?? { providerId: "p", model: "m", reasoningLevel: "low" }, set: async (key: string, value: unknown) => { stored.set(key, value); } },
    },
    sdk: {
      subscribe: () => () => {},
      threads: {
        get: async ({ threadId }: { threadId: string }) => thread(threadId),
        list: async (args: { parentThreadId?: string }) => (options.listThreads ?? (async () => []))(args),
        timeline: options.timeline ?? (async ({ threadId }: { threadId: string }) => ({
          rows: [{ id: `${threadId}-user`, kind: "conversation", role: "user", threadId, text: "Work" }],
          timelinePage: { hasOlderRows: false, olderCursor: null },
        })),
        spawn: async (input: (typeof spawned)[number]) => { spawned.push(input); return options.spawnResponse ? await options.spawnResponse(spawned.length) : { id: String(spawned.length) }; },
        wait: async ({ threadId }: { threadId: string }) => { await options.wait?.(threadId); return {}; },
        output: async ({ threadId }: { threadId: string }) => ({ output: options.output ? await options.output(threadId) : "We did work." }),
        archive: async ({ threadId }: { threadId: string }) => { await options.archive?.(threadId); },
        stop: async ({ threadId }: { threadId: string }) => { await options.stop?.(threadId); },
      },
      providers: { models: async (query: unknown) => { modelQueries.push(query); return { models: [{ id: "m", model: "m", supportedReasoningEfforts: [{ reasoningEffort: "low" }], defaultReasoningEffort: "low" }], providers: [options.provider ?? { id: "p" }] }; } },
    },
    rpc: { register: (_: unknown, registered: typeof handlers) => { handlers = registered; } },
    cli: { register: () => {} }, events: { on: (name: string, callback: (payload: unknown) => void) => { events.set(name, callback); } },
    realtime: { publish: () => {} }, log: { info: () => {}, warn: (message: string) => { warnings.push(message); } }, onDispose: (callback: typeof dispose) => { dispose = callback; },
  } as unknown as BbPluginApi;
  await plugin(bb);
  return {
    db,
    settings,
    handlers,
    spawned,
    modelQueries,
    stored,
    warnings,
    activate: (id: string) => events.get("thread.active")?.({ thread: { ...thread(id), status: "running" } }),
    emit: (name: string, payload: { thread: Record<string, unknown> }) => events.get(name)?.(payload),
    close: async () => { await dispose(); db.close(); },
  };
}

test("default settings retain persistent workers on hosts without the new capability", async () => {
  const harness = await createHarness({ legacySettings: true });
  try {
    assert.equal((await harness.handlers.recap_settings_get()).disposableWorkers, false);
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.equal(generated.recap?.summary, "We did work.");
    assert.equal(harness.spawned[0].experimental_providerSessionLifetime, undefined);
  } finally { await harness.close(); }
});

test("enabled disposable workers retain recaps and preserve execution selection", async () => {
  const db = new Database(":memory:");
  const cleanup: string[] = [];
  const observeRetention = (action: string) => async (id: string) => {
    assert.deepEqual(db.prepare("SELECT summary FROM recaps").get(), { summary: "We did work." });
    cleanup.push(`${action}:${id}`);
  };
  const harness = await createHarness({
    database: db,
    archive: observeRetention("archive"),
    stop: observeRetention("stop"),
    settings: { disposableWorkers: true },
    provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: true }, serviceTiers: [{ id: "fast" }] },
    environmentId: "workspace",
    modelSelection: { providerId: "p", model: "m", reasoningLevel: "low", serviceTier: "fast" },
    spawnResponse: async (index) => ({ id: String(index), experimental_providerSessionLifetime: "disposable" }),
  });
  try {
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.equal(generated.recap?.summary, "We did work.");
    const worker = harness.spawned[0];
    assert.equal(worker.experimental_providerSessionLifetime, "disposable");
    assert.equal(worker.providerId, "p");
    assert.equal(worker.model, "m");
    assert.equal(worker.reasoningLevel, "low");
    assert.equal(worker.serviceTier, "fast");
    assert.deepEqual(worker.environment, { type: "reuse", environmentId: "workspace" });
    assert.equal(worker.permissionMode, "accept-edits");
    assert.equal(worker.visibility, "hidden");
    assert.match(worker.prompt ?? "", /Return a recap\./);
    assert.equal(harness.modelQueries.length, 1);
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
    assert.deepEqual(harness.db.prepare("SELECT summary FROM recaps").get(), { summary: "We did work." });
  } finally { await harness.close(); }
});

for (const capability of ["absent", undefined, false, "true"]) {
  test(`unsupported disposable capability ${String(capability)} allocates no worker`, async () => {
    const harness = await createHarness({ settings: { disposableWorkers: true }, provider: capability === "absent" ? { id: "p" } : { id: "p", capabilities: { experimental_supportsDisposableSessions: capability } } });
    try {
      const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
      assert.equal(generated.reason, "disposable_workers_unsupported");
      assert.equal(generated.generated, false);
      assert.equal(harness.spawned.length, 0);
      assert.deepEqual(harness.db.prepare("SELECT count(*) AS count FROM recaps").get(), { count: 0 });
    } finally { await harness.close(); }
  });
}

test("a disposable create rejection is actionable without persistent fallback", async () => {
  const harness = await createHarness({
    settings: { disposableWorkers: true },
    provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: true } },
    spawnResponse: async () => { throw Object.assign(new Error("unsupported"), { code: "disposable_sessions_unsupported", status: 400 }); },
  });
  try {
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.equal(generated.reason, "disposable_workers_unsupported");
    assert.equal(generated.generated, false);
    assert.equal(harness.spawned.length, 1);
    assert.equal(harness.spawned[0].experimental_providerSessionLifetime, "disposable");
  } finally { await harness.close(); }
});

for (const echo of [undefined, "persistent", true]) {
  test(`a disposable worker with response lifetime ${String(echo)} is released without accepting output`, async () => {
    const cleanup: string[] = [];
    let outputReads = 0;
    const harness = await createHarness({
      settings: { disposableWorkers: true },
      provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: true } },
      spawnResponse: async (index) => ({ id: String(index), experimental_providerSessionLifetime: echo }),
      output: async () => { outputReads += 1; return "Incorrectly accepted."; },
      archive: async (id) => { cleanup.push(`archive:${id}`); },
      stop: async (id) => { cleanup.push(`stop:${id}`); },
    });
    try {
      const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
      assert.equal(generated.reason, "disposable_workers_unsupported");
      assert.equal(generated.generated, false);
      assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
      assert.equal(outputReads, 0);
      assert.deepEqual(harness.db.prepare("SELECT count(*) AS count FROM recaps").get(), { count: 0 });
    } finally { await harness.close(); }
  });
}

test("an in-flight worker keeps its requested disposable lifetime when settings change", async () => {
  const spawned = deferred();
  const release = deferred();
  const cleanup: string[] = [];
  const harness = await createHarness({
    settings: { disposableWorkers: true },
    provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: true } },
    spawnResponse: async (index) => { spawned.resolve(); await release.promise; return { id: String(index) }; },
    archive: async (id) => { cleanup.push(`archive:${id}`); },
    stop: async (id) => { cleanup.push(`stop:${id}`); },
  });
  try {
    const pending = harness.handlers.recap_generate({ threadId: "source", automatic: false });
    await spawned.promise;
    await harness.handlers.recap_settings_set({ ...harness.settings, disposableWorkers: false });
    release.resolve();
    const generated = await pending;
    assert.equal(generated.reason, "disposable_workers_unsupported");
    assert.equal(harness.spawned[0].experimental_providerSessionLifetime, "disposable");
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
  } finally { release.resolve(); await harness.close(); }
});

for (const failure of ["capability", "create", "echo"] as const) {
  test(`automatic recaps do not retry unsupported disposable ${failure}`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const harness = await createHarness({
      settings: { disposableWorkers: true, afterSeconds: 0 },
      provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: failure !== "capability" } },
      spawnResponse: async (index) => {
        if (failure === "create") throw Object.assign(new Error("unsupported"), { code: "disposable_sessions_unsupported" });
        return { id: String(index) };
      },
    });
    try {
      harness.emit("thread.idle", { thread: idleParent() });
      await new Promise(setImmediate);
      t.mock.timers.tick(0);
      await new Promise(setImmediate);
      assert.equal(harness.modelQueries.length, 1);
      t.mock.timers.tick(90_000);
      await new Promise(setImmediate);
      assert.equal(harness.modelQueries.length, 1);
      assert.deepEqual(harness.db.prepare("SELECT count(*) AS count FROM recaps").get(), { count: 0 });
    } finally { await harness.close(); }
  });
}

test("saves the disposable worker setting through the settings RPC and KV", async () => {
  const harness = await createHarness();
  try {
    const saved = await harness.handlers.recap_settings_set({ ...harness.settings, disposableWorkers: true });
    assert.equal(saved.disposableWorkers, true);
    assert.equal((await harness.handlers.recap_settings_get()).disposableWorkers, true);
    assert.deepEqual(harness.stored.get("settings"), saved);
  } finally { await harness.close(); }
});

test("retains the recap before archiving or stopping its worker", async () => {
  const db = new Database(":memory:");
  const cleanup: Array<{ action: string; threadId: string; summary: string | null }> = [];
  const observe = (action: string) => async (threadId: string) => {
    const row = db.prepare("SELECT summary FROM recaps WHERE thread_id = ?").get("source") as { summary: string } | undefined;
    cleanup.push({ action, threadId, summary: row?.summary ?? null });
  };
  const harness = await createHarness({ database: db, archive: observe("archive"), stop: observe("stop") });
  try {
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.equal(generated.generated, true);
    assert.equal(generated.recap?.summary, "We did work.");
    assert.deepEqual(cleanup, [
      { action: "archive", threadId: "1", summary: "We did work." },
      { action: "stop", threadId: "1", summary: "We did work." },
    ]);
  } finally {
    await harness.close();
  }
});

for (const stage of ["wait timeout", "output", "validation"] as const) {
  test(`releases the worker without reporting success after ${stage} fails`, async () => {
    const failure = new Error(stage);
    const cleanup: string[] = [];
    let reads = 0;
    const harness = await createHarness({
      wait: async () => { if (stage === "wait timeout") throw failure; },
      output: async () => {
        if (stage === "output") throw failure;
        return "We did work.";
      },
      timeline: async () => {
        reads += 1;
        if (stage === "validation" && reads > 1) throw failure;
        return {
          rows: [{ id: "source-user", kind: "conversation", role: "user", threadId: "source", text: "Work" }],
          timelinePage: { hasOlderRows: false, olderCursor: null },
        };
      },
      archive: async (threadId) => { cleanup.push(`archive:${threadId}`); },
      stop: async (threadId) => { cleanup.push(`stop:${threadId}`); },
    });
    try {
      await assert.rejects(
        harness.handlers.recap_generate({ threadId: "source", automatic: false }),
        (error) => error === failure,
      );
      assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
      assert.deepEqual(await harness.handlers.recap_get({ threadId: "source" }), { recap: null, generating: false });
    } finally {
      await harness.close();
    }
  });
}

test("releases the worker when the recap cannot be saved", async () => {
  const cleanup: string[] = [];
  const harness = await createHarness({
    archive: async (threadId) => { cleanup.push(`archive:${threadId}`); },
    stop: async (threadId) => { cleanup.push(`stop:${threadId}`); },
  });
  harness.db.exec("CREATE TRIGGER reject_recap BEFORE INSERT ON recaps BEGIN SELECT RAISE(ABORT, 'recap save failed'); END");
  try {
    await assert.rejects(
      harness.handlers.recap_generate({ threadId: "source", automatic: false }),
      /recap save failed/,
    );
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
    assert.deepEqual(await harness.handlers.recap_get({ threadId: "source" }), { recap: null, generating: false });
  } finally {
    await harness.close();
  }
});

for (const failedAction of ["archive", "stop"] as const) {
  test(`transient ${failedAction} failure retries only the unconfirmed operation on the same worker`, async () => {
    const db = new Database(":memory:");
    const cleanup: string[] = [];
    const attempts = { archive: 0, stop: 0 };
    const perform = (action: "archive" | "stop") => async (threadId: string) => {
      assert.deepEqual(db.prepare("SELECT summary FROM recaps").get(), { summary: "We did work." });
      cleanup.push(`${action}:${threadId}`);
      attempts[action] += 1;
      if (action === failedAction && attempts[action] === 1) throw new Error("transient cleanup failure");
    };
    const harness = await createHarness({ database: db, archive: perform("archive"), stop: perform("stop") });
    try {
      const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
      assert.equal(generated.recap?.summary, "We did work.");
      assert.deepEqual(cleanup, ["archive:1", "stop:1", `${failedAction}:1`]);
      assert.deepEqual(harness.warnings, []);
      assert.equal(harness.spawned.length, 1);
    } finally { await harness.close(); }
  });
}

for (const failedAction of ["archive", "stop", "both"] as const) {
  test(`persistent ${failedAction} cleanup failure retains the recap and warns once with confirmed statuses`, async () => {
    const cleanup: string[] = [];
    const perform = (action: "archive" | "stop") => async (threadId: string) => {
      cleanup.push(`${action}:${threadId}`);
      if (failedAction === action || failedAction === "both") throw new Error("synthetic-secret-cleanup-error");
    };
    const harness = await createHarness({
      archive: perform("archive"),
      stop: perform("stop"),
      spawnResponse: async () => ({ id: "thr_cleanup_worker" }),
    });
    try {
      const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
      assert.equal(generated.generated, true);
      assert.equal(generated.recap?.summary, "We did work.");
      assert.deepEqual(cleanup, [
        "archive:thr_cleanup_worker",
        "stop:thr_cleanup_worker",
        ...(failedAction !== "stop" ? ["archive:thr_cleanup_worker"] : []),
        ...(failedAction !== "archive" ? ["stop:thr_cleanup_worker"] : []),
      ]);
      assert.deepEqual(harness.warnings, [
        `Recap worker cleanup incomplete for thr_cleanup_worker. Archive ${failedAction === "stop" ? "confirmed" : "unconfirmed"}; stop ${failedAction === "archive" ? "confirmed" : "unconfirmed"}. Any saved recap remains available.`,
      ]);
      assert.equal((await harness.handlers.recap_get({ threadId: "source" })).recap?.id, generated.recap.id);
      assert.deepEqual(harness.db.prepare("SELECT summary FROM recaps").get(), { summary: "We did work." });
    } finally { await harness.close(); }
  });
}

test("cleanup failures do not mask the original generation failure", async () => {
  const failure = new Error("generation failed");
  const cleanup: string[] = [];
  const harness = await createHarness({
    wait: async () => { throw failure; },
    archive: async (id) => { cleanup.push(`archive:${id}`); throw new Error("synthetic-secret-archive-error"); },
    stop: async (id) => { cleanup.push(`stop:${id}`); throw new Error("synthetic-secret-stop-error"); },
  });
  try {
    await assert.rejects(harness.handlers.recap_generate({ threadId: "source", automatic: false }), (error) => error === failure);
    assert.deepEqual(cleanup, ["archive:1", "stop:1", "archive:1", "stop:1"]);
    assert.deepEqual(harness.warnings, ["Recap worker cleanup incomplete for 1. Archive unconfirmed; stop unconfirmed. Any saved recap remains available."]);
    assert.deepEqual(await harness.handlers.recap_get({ threadId: "source" }), { recap: null, generating: false });
    assert.deepEqual(harness.db.prepare("SELECT count(*) AS count FROM recaps").get(), { count: 0 });
  } finally { await harness.close(); }
});

test("automatic generation does not regenerate a saved recap after cleanup alone fails", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const cleanup: string[] = [];
  const harness = await createHarness({
    settings: { disposableWorkers: true, afterSeconds: 0 },
    provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: true } },
    spawnResponse: async (index) => ({ id: String(index), experimental_providerSessionLifetime: "disposable" }),
    archive: async (id) => { cleanup.push(`archive:${id}`); throw new Error("archive failed"); },
    stop: async (id) => { cleanup.push(`stop:${id}`); throw new Error("stop failed"); },
  });
  try {
    harness.emit("thread.idle", { thread: idleParent() });
    await new Promise(setImmediate);
    t.mock.timers.tick(0);
    await new Promise(setImmediate);
    assert.deepEqual(cleanup, ["archive:1", "stop:1", "archive:1", "stop:1"]);
    assert.equal((await harness.handlers.recap_get({ threadId: "parent" })).recap?.summary, "We did work.");
    t.mock.timers.tick(90_000);
    await new Promise(setImmediate);
    const duplicate = await harness.handlers.recap_generate({ threadId: "parent", automatic: true });
    assert.equal(duplicate.reason, "already_exists");
    assert.equal(harness.spawned.length, 1);
    assert.equal(harness.warnings.length, 1);
    assert.deepEqual(harness.db.prepare("SELECT count(*) AS count FROM recaps").get(), { count: 1 });
  } finally { await harness.close(); }
});

test("an aborted generation releases its worker without saving the output", async () => {
  const started = deferred();
  const release = deferred();
  const cleanup: string[] = [];
  const harness = await createHarness({
    wait: async () => { started.resolve(); await release.promise; },
    archive: async (threadId) => { cleanup.push(`archive:${threadId}`); },
    stop: async (threadId) => { cleanup.push(`stop:${threadId}`); },
  });
  try {
    const pending = harness.handlers.recap_generate({ threadId: "source", automatic: false });
    await started.promise;
    harness.activate("source");
    release.resolve();
    const generated = await pending;
    assert.equal(generated.generated, false);
    assert.equal(generated.reason, "aborted");
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
    assert.equal((await harness.handlers.recap_get({ threadId: "source" })).recap, null);
  } finally {
    release.resolve();
    await harness.close();
  }
});

test("stale worker output is released without saving a recap", async () => {
  let cursor = "before";
  const cleanup: string[] = [];
  const harness = await createHarness({
    timeline: async () => ({
      rows: [{ id: cursor, kind: "conversation", role: "user", threadId: "source", text: "Work" }],
      timelinePage: { hasOlderRows: false, olderCursor: null },
    }),
    output: async () => { cursor = "after"; return "Stale work."; },
    archive: async (threadId) => { cleanup.push(`archive:${threadId}`); },
    stop: async (threadId) => { cleanup.push(`stop:${threadId}`); },
  });
  try {
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.equal(generated.generated, false);
    assert.equal(generated.reason, "stale");
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
    assert.equal((await harness.handlers.recap_get({ threadId: "source" })).recap, null);
  } finally {
    await harness.close();
  }
});

test("empty worker output is released without reporting success", async () => {
  const cleanup: string[] = [];
  const harness = await createHarness({
    output: async () => null,
    archive: async (threadId) => { cleanup.push(`archive:${threadId}`); },
    stop: async (threadId) => { cleanup.push(`stop:${threadId}`); },
  });
  try {
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.equal(generated.generated, false);
    assert.equal(generated.reason, "empty_model_response");
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
    assert.equal((await harness.handlers.recap_get({ threadId: "source" })).recap, null);
  } finally {
    await harness.close();
  }
});

test("an automatic retry uses a fresh disposable worker and retains the recap once", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const cleanup: string[] = [];
  const harness = await createHarness({
    settings: { disposableWorkers: true },
    provider: { id: "p", capabilities: { experimental_supportsDisposableSessions: true } },
    spawnResponse: async (index) => ({ id: String(index), experimental_providerSessionLifetime: "disposable" }),
    wait: async (threadId) => { if (threadId === "1") throw new Error("worker timed out"); },
    archive: async (threadId) => { cleanup.push(`archive:${threadId}`); },
    stop: async (threadId) => { cleanup.push(`stop:${threadId}`); },
  });
  try {
    await harness.handlers.recap_settings_set({ ...harness.settings, afterSeconds: 0 });
    harness.emit("thread.idle", { thread: idleParent() });
    await new Promise(setImmediate);
    t.mock.timers.tick(0);
    await new Promise(setImmediate);
    assert.equal(harness.spawned.length, 1);
    assert.deepEqual(cleanup, ["archive:1", "stop:1"]);
    assert.equal((await harness.handlers.recap_get({ threadId: "parent" })).recap, null);

    t.mock.timers.tick(90_000);
    await new Promise(setImmediate);
    assert.equal(harness.spawned.length, 2);
    assert.deepEqual(harness.spawned.map((worker) => worker.experimental_providerSessionLifetime), ["disposable", "disposable"]);
    assert.deepEqual(cleanup, ["archive:1", "stop:1", "archive:2", "stop:2"]);
    const retained = (await harness.handlers.recap_get({ threadId: "parent" })).recap;
    assert.equal(retained?.summary, "We did work.");
    const duplicate = await harness.handlers.recap_generate({ threadId: "parent", automatic: true });
    assert.equal(duplicate.reason, "already_exists");
    assert.equal(harness.spawned.length, 2);
    assert.deepEqual(harness.db.prepare("SELECT count(*) AS count FROM recaps").get(), { count: 1 });
  } finally {
    await harness.close();
  }
});

test("saves Expanded banner and reads it back", async () => {
  const harness = await createHarness();
  try {
    const saved = await harness.handlers.recap_display_mode_set({ displayMode: "Expanded banner" });
    assert.equal(saved.displayMode, "Expanded banner");
    assert.equal((await harness.handlers.recap_settings_get()).displayMode, "Expanded banner");
  } finally {
    await harness.close();
  }
});

test("disabling automatic recaps prevents queued requests from spawning", async () => {
  const started = deferred();
  const release = deferred();
  const harness = await createHarness({ wait: async (id) => { if (id === "1") { started.resolve(); await release.promise; } } });
  try {
    const first = harness.handlers.recap_generate({ threadId: "A", automatic: true });
    await started.promise;
    const queued = harness.handlers.recap_generate({ threadId: "B", automatic: true });
    assert.equal((await harness.handlers.recap_get({ threadId: "B" })).generating, true);
    await harness.handlers.recap_settings_set({ ...harness.settings, auto: false });
    release.resolve();
    await first;
    const stopped = await queued;
    assert.equal(harness.spawned.length, 1);
    assert.equal(stopped.reason, "automatic_disabled");
    const manual = await harness.handlers.recap_generate({ threadId: "B", automatic: false });
    assert.ok(manual.recap);
    assert.equal(harness.spawned.length, 2);
  } finally {
    release.resolve();
    await harness.close();
  }
});

test("pagination cannot skip new requests when older pages drop out of the window", async () => {
  let pageCount = 21;
  const harness = await createHarness({ timeline: async ({ beforeAnchorSeq }) => {
    const page = beforeAnchorSeq === undefined ? pageCount : Number(beforeAnchorSeq) - 1;
    return {
      rows: Array.from({ length: 50 }, (_, index) => ({ id: `page-${page}-${index}`, kind: "conversation", threadId: "source", role: index === 0 && page !== 2 ? "user" : "assistant", text: index === 0 && page !== 2 ? `New request ${page}` : `Activity ${page}.${index}` })),
      timelinePage: { hasOlderRows: page > 1, olderCursor: page > 1 ? { anchorId: `page-${page}`, anchorSeq: page } : null },
    };
  } });
  try {
    await harness.handlers.recap_settings_set({ ...harness.settings, auto: false });
    await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    pageCount = 23;
    harness.activate("source");
    await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.match(harness.spawned[1].prompt, /<previous-recap>/);
    assert.match(harness.spawned[1].prompt, /New request 22/);
    assert.match(harness.spawned[1].prompt, /New request 23/);
    assert.doesNotMatch(harness.spawned[1].prompt, /New request 21/);
    // Counts can stay the same in successive windows; the row ID still moves.
    await harness.handlers.recap_settings_set({ ...harness.settings, auto: true });
    pageCount = 25;
    harness.activate("source");
    await harness.handlers.recap_generate({ threadId: "source", automatic: true });
    assert.equal(harness.spawned.length, 3);
    assert.match(harness.spawned[2].prompt, /New request 24/);
    assert.match(harness.spawned[2].prompt, /New request 25/);
  } finally {
    await harness.close();
  }
});

test("cursor migration preserves recaps from an existing database", async () => {
  const db = new Database(":memory:");
  db.exec(`CREATE TABLE recaps (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, summary TEXT NOT NULL, automatic INTEGER NOT NULL, generated_at INTEGER NOT NULL, turns INTEGER NOT NULL, model TEXT NOT NULL, suppressed INTEGER NOT NULL DEFAULT 0)`);
  db.prepare("INSERT INTO recaps VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run("legacy", "source", "Earlier decisions", 0, 100, 4, "p/m", 0);
  const harness = await createHarness({ database: db });
  try {
    assert.equal((db.prepare("SELECT last_user_row_id FROM recaps WHERE id = 'legacy'").get() as { last_user_row_id: null }).last_user_row_id, null);
    await harness.handlers.recap_settings_set({ ...harness.settings, auto: false });
    const generated = await harness.handlers.recap_generate({ threadId: "source", automatic: false });
    assert.match(harness.spawned[0].prompt, /Earlier decisions/);
    assert.equal(generated.recap?.lastUserRowId, "source-user");
  } finally {
    await harness.close();
  }
});

test("does not submit a transcript if its thread is hidden during model lookup", async () => {
  let visibility: "visible" | "hidden" = "visible";
  let notifyThreadChanged: (() => void) | undefined;
  let startModelLookup!: () => void;
  let finishModelLookup!: () => void;
  const modelLookupStarted = new Promise<void>((resolve) => {
    startModelLookup = resolve;
  });
  const modelLookupGate = new Promise<void>((resolve) => {
    finishModelLookup = resolve;
  });
  const db = new Database(":memory:");
  const disposers: Array<() => void | Promise<void>> = [];
  const spawned: unknown[] = [];
  let handlers: any;
  const thread = (threadId: string) => ({
    id: threadId,
    projectId: "test-project",
    environmentId: null,
    providerId: "test-provider",
    status: "idle",
    visibility,
    originPluginId: null,
  });
  const bb = {
    pluginId: "bb-recap",
    storage: {
      database: () => db,
      migrate: (_database: Database.Database, statements: string[]) => {
        for (const statement of statements) db.exec(statement);
      },
      kv: {
        get: async () => undefined,
        set: async () => undefined,
      },
    },
    sdk: {
      subscribe: ({ callback }: any) => {
        notifyThreadChanged = () =>
          callback({
            entity: "thread",
            type: "changed",
            id: "source-thread",
            changes: ["title-changed"],
          });
        return () => {
          notifyThreadChanged = undefined;
        };
      },
      threads: {
        get: async ({ threadId }: { threadId: string }) => thread(threadId),
        list: async () => [],
        timeline: async () => ({
          rows: [
            {
              kind: "conversation",
              role: "user",
              threadId: "source-thread",
              text: "Private discussion",
            },
          ],
          timelinePage: { hasOlderRows: false, olderCursor: null },
        }),
        spawn: async () => {
          spawned.push(true);
          return { id: "recap-worker" };
        },
        wait: async () => ({}),
        output: async () => ({ output: "We discussed a private topic." }),
        archive: async () => ({ ok: true }),
        stop: async () => ({ ok: true }),
      },
      system: {
        executionOptions: async () => ({
          models: [
            {
              isDefault: true,
              routeProviderId: "test-provider",
              model: "test-model",
              defaultReasoningEffort: "low",
            },
          ],
          providers: [{ id: "test-provider", available: true }],
        }),
      },
      providers: {
        models: async () => {
          startModelLookup();
          await modelLookupGate;
          return {
            models: [
              {
                id: "test-model",
                model: "test-model",
                isDefault: true,
                supportedReasoningEfforts: [{ reasoningEffort: "low" }],
                defaultReasoningEffort: "low",
              },
            ],
            providers: [{ id: "test-provider", serviceTiers: [] }],
          };
        },
      },
    },
    rpc: {
      register: (_contract: unknown, registeredHandlers: unknown) => {
        handlers = registeredHandlers;
      },
    },
    cli: { register: () => undefined },
    events: { on: () => undefined },
    realtime: { publish: () => undefined },
    log: { info: () => undefined, warn: () => undefined },
    onDispose: (dispose: () => void | Promise<void>) => {
      disposers.push(dispose);
    },
  } as unknown as BbPluginApi;

  await plugin(bb);
  try {
    const generation = handlers.recap_generate({
      threadId: "source-thread",
      automatic: false,
    });
    await modelLookupStarted;
    visibility = "hidden";
    notifyThreadChanged?.();
    await new Promise<void>((resolve) => setImmediate(resolve));
    finishModelLookup();

    const result = await generation;
    assert.ok(["hidden_thread", "aborted"].includes(result.reason));
    assert.equal(result.generated, false);
    assert.equal(spawned.length, 0);
  } finally {
    for (const dispose of disposers.reverse()) await dispose();
    db.close();
  }
});

function listedChild(overrides: Record<string, unknown> = {}) {
  return {
    id: "child",
    status: "active",
    visibility: "hidden",
    originKind: null,
    originPluginId: null,
    parentThreadId: "parent",
    archivedAt: null,
    deletedAt: null,
    ...overrides,
  };
}

async function waitFor(predicate: () => boolean | Promise<boolean>) {
  const started = Date.now();
  while (Date.now() - started < 1_000) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail("timed out waiting for recap state");
}

test("a running child thread keeps its parent active", async () => {
  let children = [listedChild()];
  const harness = await createHarness({
    listThreads: async ({ parentThreadId }) =>
      children.filter((child) => child.parentThreadId === parentThreadId),
  });
  try {
    await assert.rejects(
      harness.handlers.recap_generate({ threadId: "parent", automatic: false }),
      /Running child threads keep it active/,
    );
    assert.equal(harness.spawned.length, 0);

    children = [listedChild({ status: "idle" })];
    assert.ok((await harness.handlers.recap_generate({ threadId: "parent", automatic: false })).recap);

    children = [listedChild({ id: "worker", status: "active", originPluginId: "bb-recap" })];
    harness.activate("parent");
    assert.ok((await harness.handlers.recap_generate({ threadId: "parent", automatic: false })).recap);

    children = [listedChild({ status: "active", originKind: "fork" })];
    harness.activate("parent");
    assert.ok((await harness.handlers.recap_generate({ threadId: "parent", automatic: false })).recap);

    children = [listedChild({ status: "active", archivedAt: 1 })];
    harness.activate("parent");
    assert.ok((await harness.handlers.recap_generate({ threadId: "parent", automatic: false })).recap);

    children = [
      listedChild({ id: "mid", status: "idle" }),
      listedChild({ id: "leaf", status: "active", parentThreadId: "mid" }),
    ];
    harness.activate("parent");
    await assert.rejects(
      harness.handlers.recap_generate({ threadId: "parent", automatic: false }),
      /Running child threads keep it active/,
    );
  } finally {
    await harness.close();
  }
});

function idleParent() {
  return {
    id: "parent",
    projectId: "p",
    environmentId: null,
    providerId: "p",
    status: "idle",
    visibility: "visible" as const,
    originPluginId: null,
    originKind: null,
    parentThreadId: null,
  };
}

test("automatic recaps wait until running child threads finish", async () => {
  let children = [listedChild({ status: "active" })];
  const harness = await createHarness({
    listThreads: async ({ parentThreadId }) =>
      children.filter((child) => child.parentThreadId === parentThreadId),
  });
  try {
    await harness.handlers.recap_settings_set({
      ...harness.settings,
      afterSeconds: 0,
      auto: true,
      minTurns: 1,
    });
    harness.emit("thread.idle", { thread: idleParent() });
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal(harness.spawned.length, 0);

    children = [];
    harness.emit("thread.idle", {
      thread: listedChild({ status: "idle", visibility: "hidden" }),
    });
    await waitFor(() => harness.spawned.length === 1);
  } finally {
    await harness.close();
  }
});

test("a child that starts running cancels an in-flight recap until it finishes", async () => {
  let children: Array<ReturnType<typeof listedChild>> = [];
  const started = deferred();
  const release = deferred();
  const harness = await createHarness({
    listThreads: async ({ parentThreadId }) =>
      children.filter((child) => child.parentThreadId === parentThreadId),
    wait: async (id) => {
      if (id !== "1") return;
      started.resolve();
      await release.promise;
    },
  });
  try {
    await harness.handlers.recap_settings_set({
      ...harness.settings,
      afterSeconds: 0,
      auto: true,
      minTurns: 1,
    });
    harness.emit("thread.idle", { thread: idleParent() });
    await started.promise;
    children = [listedChild({ status: "active" })];
    harness.emit("thread.active", { thread: listedChild({ status: "active" }) });
    release.resolve();
    await waitFor(async () => !(await harness.handlers.recap_get({ threadId: "parent" })).generating);
    assert.equal((await harness.handlers.recap_get({ threadId: "parent" })).recap, null);

    children = [];
    harness.emit("thread.idle", {
      thread: listedChild({ status: "idle", visibility: "hidden" }),
    });
    await waitFor(() => harness.spawned.length === 2);
    await waitFor(async () => (await harness.handlers.recap_get({ threadId: "parent" })).recap !== null);
  } finally {
    release.resolve();
    await harness.close();
  }
});
