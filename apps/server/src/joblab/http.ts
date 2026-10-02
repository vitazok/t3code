import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as JoblabBridge from "./JoblabBridge.ts";
import { timingSafeEqual } from "node:crypto";
const authorized = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  return yield* auth.authenticateHttpRequest(request);
});
const result = (effect: Effect.Effect<unknown, JoblabBridge.BridgeError, never>) =>
  effect.pipe(
    Effect.map((data) => HttpServerResponse.jsonUnsafe(data)),
    Effect.catch(() =>
      Effect.succeed(
        HttpServerResponse.jsonUnsafe(
          { error: "Company request could not be completed." },
          { status: 400 },
        ),
      ),
    ),
  );
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const bridge = yield* JoblabBridge.JoblabBridge;
    const router = yield* HttpRouter.HttpRouter;
    yield* router.add(
      "GET",
      "/api/joblab/binding",
      Effect.gen(function* () {
        yield* authorized;
        return yield* result(bridge.binding);
      }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 401 })))),
    );
    for (const path of ["status", "updates"])
      yield* router.add(
        "GET",
        `/api/joblab/${path}`,
        Effect.gen(function* () {
          yield* authorized;
          const request = yield* HttpServerRequest.HttpServerRequest;
          const since = new URL(request.url, "http://local").searchParams.get("since") ?? "0";
          if (path === "updates" && !/^\d{1,10}$/.test(since))
            return HttpServerResponse.empty({ status: 400 });
          const data = yield* bridge.proxy(
            path === "updates" ? `updates?since=${since}` : path,
            "GET",
          );
          return HttpServerResponse.jsonUnsafe(data.body, { status: data.status });
        }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 401 })))),
      );
    yield* router.add(
      "POST",
      "/api/joblab/command",
      Effect.gen(function* () {
        yield* authorized;
        const request = yield* HttpServerRequest.HttpServerRequest;
        const body = yield* request.json;
        const data = yield* bridge.proxy("command", "POST", body);
        return HttpServerResponse.jsonUnsafe(data.body, { status: data.status });
      }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 400 })))),
    );
    yield* router.add(
      "GET",
      "/api/joblab/files",
      Effect.gen(function* () {
        yield* authorized;
        return yield* result(bridge.files);
      }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 401 })))),
    );
    yield* router.add(
      "POST",
      "/api/joblab/assistant-review",
      Effect.gen(function* () {
        yield* authorized;
        return yield* result(bridge.assistantReview);
      }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 401 })))),
    );
    yield* router.add(
      "POST",
      "/api/joblab/internal/snapshot",
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const provided = request.headers.authorization?.replace(/^Bearer /, "") ?? "",
          expected = process.env.JOBLAB_BRIDGE_TOKEN ?? "";
        if (
          !expected ||
          provided.length !== expected.length ||
          !timingSafeEqual(Buffer.from(provided), Buffer.from(expected))
        )
          return HttpServerResponse.empty({ status: 401 });
        return yield* result(bridge.snapshot(yield* request.json));
      }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 400 })))),
    );
  }),
).pipe(Layer.provide(JoblabBridge.layer));
