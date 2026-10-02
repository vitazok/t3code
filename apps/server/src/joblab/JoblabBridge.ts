// @effect-diagnostics nodeBuiltinImport:off - Snapshot verification requires native O_NOFOLLOW descriptors and inode metadata at the filesystem boundary.
import { readFile, readdir, lstat, realpath, open, writeFile, mkdir } from "node:fs/promises";
import { constants } from "node:fs";
import { resolve, relative, dirname } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import {
  CommandId,
  ProjectId,
  ThreadId,
  MessageId,
  ProviderInstanceId,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as ProjectService from "../project/ProjectService.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EventStore from "../persistence/Services/OrchestrationEventStore.ts";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import * as Stream from "effect/Stream";

export class BridgeError extends Schema.TaggedError<BridgeError>()("JoblabBridgeError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return "Company workspace request could not be completed safely.";
  }
}
const config = () => {
  const runId = process.env.JOBLAB_RUN_ID,
    workspace = process.env.JOBLAB_WORKSPACE,
    serviceUrl = process.env.JOBLAB_SERVICE_URL,
    uiToken = process.env.JOBLAB_UI_TOKEN,
    bridgeToken = process.env.JOBLAB_BRIDGE_TOKEN,
    threadId = process.env.JOBLAB_THREAD_ID,
    projectId = process.env.JOBLAB_PROJECT_ID;
  if (!runId || !workspace || !serviceUrl || !uiToken || !bridgeToken || !threadId || !projectId)
    throw new Error("No bound assessment runtime.");
  return {
    runId,
    workspace,
    serviceUrl,
    uiToken,
    bridgeToken,
    threadId,
    projectId,
    mode: process.env.JOBLAB_MODE ?? "isolated",
  };
};
const hash = (data: Buffer) => createHash("sha256").update(data).digest("hex");
async function safeFile(root: string, path: string) {
  if (
    !path ||
    path.startsWith("/") ||
    path.includes("\\") ||
    path.split("/").some((p) => p === ".." || p === "." || !p)
  )
    throw new Error("Unsafe path.");
  let full = await realpath(root);
  for (const segment of path.split("/")) {
    full = resolve(full, segment);
    if ((await lstat(full)).isSymbolicLink()) throw new Error("Symlink rejected.");
  }
  if (relative(await realpath(root), await realpath(full)).startsWith(".."))
    throw new Error("Workspace escape.");
  return full;
}
async function snapshot(root: string, files: { path: string; sha256: string }[]) {
  const result = [];
  let size = 0;
  const seen = new Set<string>();
  for (const f of files) {
    if (!f.path.startsWith("outputs/") || seen.has(f.path) || !/^[a-f0-9]{64}$/.test(f.sha256))
      throw new Error("Invalid output selection.");
    seen.add(f.path);
    const path = await safeFile(root, f.path),
      fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const before = await fd.stat();
      if (!before.isFile() || before.nlink !== 1 || before.size > 10 * 1024 * 1024)
        throw new Error("Invalid file.");
      const bytes = await fd.readFile(),
        after = await fd.stat();
      size += bytes.length;
      if (
        size > 30 * 1024 * 1024 ||
        before.mtimeMs !== after.mtimeMs ||
        before.ctimeMs !== after.ctimeMs ||
        hash(bytes) !== f.sha256 ||
        hash(await readFile(await safeFile(root, f.path))) !== f.sha256
      )
        throw new Error("File changed.");
      result.push({
        path: f.path,
        sha256: f.sha256,
        size: bytes.length,
        bytes: bytes.toString("base64"),
      });
    } finally {
      await fd.close();
    }
  }
  return { files: result };
}
async function outputs(
  root: string,
  prefix = "outputs",
): Promise<{ path: string; sha256: string; size: number }[]> {
  const result = [];
  for (const entry of await readdir(resolve(root, prefix), { withFileTypes: true })) {
    if (entry.isSymbolicLink() || entry.name.startsWith(".")) continue;
    const path = `${prefix}/${entry.name}`;
    if (entry.isDirectory()) result.push(...(await outputs(root, path)));
    else if (entry.isFile()) {
      const bytes = await readFile(await safeFile(root, path));
      result.push({ path, sha256: hash(bytes), size: bytes.length });
    }
  }
  return result;
}
export class JoblabBridge extends Context.Service<
  JoblabBridge,
  {
    readonly binding: Effect.Effect<unknown, BridgeError>;
    readonly proxy: (
      path: string,
      method: string,
      body?: unknown,
    ) => Effect.Effect<{ status: number; body: unknown }, BridgeError>;
    readonly files: Effect.Effect<unknown, BridgeError>;
    readonly snapshot: (body: unknown) => Effect.Effect<unknown, BridgeError>;
    readonly assistantReview: Effect.Effect<unknown, BridgeError>;
    readonly transcript: Effect.Effect<void, BridgeError>;
  }
>()("t3/joblab/JoblabBridge") {}
const make = Effect.gen(function* () {
  const projects = yield* ProjectService.ProjectService;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const eventStore = yield* EventStore.OrchestrationEventStore;
  const attempt = <A>(operation: string, run: () => Promise<A>) =>
    Effect.tryPromise({ try: run, catch: (cause) => new BridgeError({ operation, cause }) });
  const binding = Effect.gen(function* () {
    const c = yield* attempt("config", async () => config());
    const project = yield* projects
      .getById(ProjectId.make(c.projectId))
      .pipe(Effect.mapError((cause) => new BridgeError({ operation: "project", cause })));
    if (Option.isNone(project))
      yield* projects
        .bootstrap({
          commandId: CommandId.make(`joblab-project-${c.runId}`),
          projectId: ProjectId.make(c.projectId),
          title: "Prairie Table Foods",
          workspaceRoot: c.workspace,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-6.1-sol",
          },
        })
        .pipe(Effect.mapError((cause) => new BridgeError({ operation: "project", cause })));
    yield* orchestrator
      .dispatch(
        yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
          type: "thread.create",
          commandId: `joblab-thread-${c.runId}`,
          threadId: c.threadId,
          projectId: c.projectId,
          title: "Assessment",
          modelSelection: { instanceId: "codex", model: "gpt-6.1-sol" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "server",
        }).pipe(Effect.mapError((cause) => new BridgeError({ operation: "command", cause }))),
      )
      .pipe(Effect.mapError((cause) => new BridgeError({ operation: "thread", cause })));
    return { runId: c.runId, threadId: c.threadId, projectId: c.projectId, mode: c.mode };
  });
  const client = yield* HttpClient.HttpClient;
  const request = (url: string, credential: string, method: "GET" | "POST", body?: unknown) =>
    Effect.gen(function* () {
      let request = HttpClientRequest.make(method)(url).pipe(
        HttpClientRequest.setHeaders({ Authorization: `Bearer ${credential}` }),
      );
      if (body !== undefined)
        request = yield* HttpClientRequest.bodyJson(request, body).pipe(
          Effect.mapError((cause) => new BridgeError({ operation: "encode", cause })),
        );
      const response = yield* client.execute(request).pipe(
        Effect.timeout("30 seconds"),
        Effect.mapError((cause) => new BridgeError({ operation: "http", cause })),
      );
      const data = yield* response.json.pipe(
        Effect.mapError((cause) => new BridgeError({ operation: "http-json", cause })),
      );
      return { status: response.status, body: data };
    });
  const proxy = (path: string, method: string, body?: unknown) =>
    Effect.gen(function* () {
      const c = yield* attempt("config", async () => config());
      if (
        !["status", "command", "download"].includes(path) &&
        !/^updates(?:\?since=\d{1,10})?$/.test(path)
      )
        return yield* new BridgeError({
          operation: "proxy",
          cause: "Candidate operation required.",
        });
      const response = yield* request(
        `${c.serviceUrl}/api/${path}`,
        c.uiToken,
        method === "POST" ? "POST" : "GET",
        body,
      );
      const data = response.body as {
        records?: { path: string; body: string; sha256: string }[];
        updates?: { path: string; body: string; sha256: string }[];
      };
      yield* attempt("attachments", async () => {
        for (const r of data.records ?? data.updates ?? []) {
          if (!/^updates\/UPD-\d+\.md$/.test(r.path) || hash(Buffer.from(r.body)) !== r.sha256)
            throw new Error("Unsafe released attachment.");
          const updateRoot = await safeFile(c.workspace, "updates");
          const full = resolve(updateRoot, r.path.split("/")[1]!);
          await writeFile(full, r.body, { flag: "wx", mode: 0o444 }).catch((error) => {
            if (error.code !== "EEXIST") throw error;
          });
        }
      });
      return response;
    });
  let transcriptCursor = 0;
  const transcript = Effect.gen(function* () {
    const c = yield* attempt("config", async () => config());
    const events = yield* eventStore
      .readAgentEvents({
        threadId: ThreadId.make(c.threadId),
        afterSequence: transcriptCursor,
        limit: 1000,
      })
      .pipe(
        Stream.runCollect,
        Effect.mapError((cause) => new BridgeError({ operation: "transcript", cause })),
      );
    for (let i = 0; i < events.length; i += 100) {
      const batch = events
        .slice(i, i + 100)
        .map((e) => ({ sourceId: String(e.sequence), event: e }));
      const response = yield* request(
        `${c.serviceUrl}/api/bridge/transcript`,
        c.bridgeToken,
        "POST",
        { events: batch },
      );
      if (response.status >= 400)
        return yield* new BridgeError({ operation: "transcript", cause: "Capture failed." });
      transcriptCursor = Math.max(transcriptCursor, ...batch.map((e) => Number(e.sourceId)));
    }
  });
  if (process.env.JOBLAB_RUN_ID) yield* binding;
  if (process.env.JOBLAB_RUN_ID)
    yield* Effect.all([transcript, proxy("status", "GET")], { concurrency: 1 }).pipe(
      Effect.catch((error) => Effect.logWarning("Assessment transcript capture will retry", error)),
      Effect.andThen(Effect.sleep("2 seconds")),
      Effect.forever,
      Effect.forkScoped,
    );
  const assistantReview = Effect.gen(function* () {
    const c = yield* attempt("config", async () => config());
    return yield* orchestrator
      .dispatch(
        yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
          type: "message.dispatch",
          commandId: randomUUID(),
          threadId: c.threadId,
          messageId: randomUUID(),
          text: "Review newly released company updates and colleague replies using get_status, read_inbox and get_records. Revalidate the operating plan and explain any needed decisions before booking.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        }).pipe(Effect.mapError((cause) => new BridgeError({ operation: "command", cause }))),
      )
      .pipe(Effect.mapError((cause) => new BridgeError({ operation: "assistant-review", cause })));
  });
  return JoblabBridge.of({
    binding,
    proxy,
    files: attempt("files", async () => ({ files: await outputs(config().workspace) })),
    snapshot: (body) =>
      attempt("snapshot", async () => {
        const x = body as { files: { path: string; sha256: string }[] };
        if (!Array.isArray(x?.files) || !x.files.length || x.files.length > 30)
          throw new Error("Invalid selection.");
        return snapshot(config().workspace, x.files);
      }),
    assistantReview,
    transcript,
  });
});
export const layer = Layer.effect(JoblabBridge, make);
