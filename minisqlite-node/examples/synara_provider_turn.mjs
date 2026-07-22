// Synara provider-turn vertical slice, ported from examples/synara_provider_turn.rs.
//
// Happy path: thread created -> turn requested -> projection "queued" ->
// provider job enqueued (one atomic outbox commit) -> worker claims ->
// provider effect (simulated) -> completion event + projection "idle" +
// job ack (one atomic commit).
//
// Failure drill: the claim commits but the worker "crashes" before
// acknowledging. The store is reopened and recoverClaim reconstructs the
// original lease tokens, so the turn completes exactly once.
//
// Runs under both Node and Bun:
//   node examples/synara_provider_turn.mjs
//   bun examples/synara_provider_turn.mjs

import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store, newId } from "../index.js";

const THREADS = "threads";
const QUEUE = "provider-command";
const nowMs = () => Date.now();
const buf = (s) => Buffer.from(s, "utf8");

function threadStatus(store, threadId) {
  const value = store.projectionGet(THREADS, buf(threadId));
  return value ? value.toString("utf8") : "<absent>";
}

// Steps 1-4: thread created, then turn requested + projection "queued" +
// provider job enqueued in one atomic outbox commit.
function requestTurn(store, threadId) {
  const stream = `thread:${threadId}`;
  store.commit({
    committedAtMs: nowMs(),
    expectedStreamVersions: [{ streamId: stream, version: 0 }],
    events: [
      {
        streamId: stream,
        eventType: "thread.created",
        occurredAtMs: nowMs(),
        payload: buf("{}"),
      },
    ],
    projectionPatches: [
      {
        projection: THREADS,
        expectedVersion: store.projectionVersion(THREADS),
        puts: [{ key: buf(threadId), value: buf('{"status":"idle"}') }],
      },
    ],
  });
  console.log(`[commit] thread.created            threads/${threadId} -> idle`);

  const jobId = newId();
  store.commit({
    committedAtMs: nowMs(),
    expectedStreamVersions: [{ streamId: stream, version: 1 }],
    events: [
      {
        streamId: stream,
        eventType: "thread.turn-requested",
        occurredAtMs: nowMs(),
        payload: buf('{"turn":1}'),
      },
    ],
    projectionPatches: [
      {
        projection: THREADS,
        expectedVersion: store.projectionVersion(THREADS),
        puts: [{ key: buf(threadId), value: buf('{"status":"queued"}') }],
      },
    ],
    enqueueJobs: [
      {
        jobId,
        queue: QUEUE,
        partitionKey: stream,
        payload: buf('{"turn":1,"provider":"simulated"}'),
      },
    ],
  });
  console.log(`[commit] thread.turn-requested     threads/${threadId} -> queued, job enqueued`);
  return jobId;
}

// Worker heartbeat: durably extend the lease so no other worker can claim the job.
function heartbeat(store, job) {
  const now = nowMs();
  const receipt = store.extendLease(job.jobId, job.leaseToken, now + 60_000, now);
  console.log(
    `[lease]  heartbeat extended lease for ${job.partitionKey} to now+60s (attempt ${receipt.attempt})`,
  );
}

// Worker protocol step 4: turn-started event + projection "running", one commit.
function startTurn(store, threadId) {
  const stream = `thread:${threadId}`;
  store.commit({
    committedAtMs: nowMs(),
    events: [
      {
        streamId: stream,
        eventType: "thread.turn-started",
        occurredAtMs: nowMs(),
        payload: buf('{"turn":1}'),
      },
    ],
    projectionPatches: [
      {
        projection: THREADS,
        expectedVersion: store.projectionVersion(THREADS),
        puts: [{ key: buf(threadId), value: buf('{"status":"running"}') }],
      },
    ],
  });
  console.log(`[commit] thread.turn-started       threads/${threadId} -> running`);
}

// Step 6: the external provider effect, simulated.
function callProvider(job) {
  console.log(`[effect] provider call for ${job.partitionKey} attempt ${job.attempt} (simulated)`);
}

// Steps 7-9: completion event + projection "idle" + job ack, atomically.
function completeTurn(store, threadId, job) {
  const stream = `thread:${threadId}`;
  const transactionId = newId();
  const batch = {
    transactionId,
    committedAtMs: nowMs(),
    events: [
      {
        streamId: stream,
        eventType: "thread.turn-completed",
        occurredAtMs: nowMs(),
        payload: buf('{"turn":1}'),
      },
    ],
    projectionPatches: [
      {
        projection: THREADS,
        expectedVersion: store.projectionVersion(THREADS),
        puts: [{ key: buf(threadId), value: buf('{"status":"idle"}') }],
      },
    ],
    ackJobs: [{ jobId: job.jobId, leaseToken: job.leaseToken }],
  };
  try {
    store.commit(batch);
  } catch (e) {
    // The ack outcome is unknown; a blind retry could double-ack. Resolve with
    // recoverTransaction: committed means done, absent means the commit never
    // landed and the same batch is safe to resubmit.
    if (e.code !== "CommitIndeterminate") throw e;
    const recovery = store.recoverTransaction(transactionId);
    if (recovery.kind === "absent") {
      console.log("[commit] indeterminate ack resolved: absent; resubmitting");
      store.commit(batch);
    } else {
      console.log("[commit] indeterminate ack resolved: committed; nothing to retry");
    }
  }
  console.log(`[commit] thread.turn-completed     threads/${threadId} -> idle, job acked`);
}

// Step 5: worker claims one job from the provider-command queue.
function claimOne(store, workerId) {
  for (;;) {
    let outcome;
    try {
      outcome = store.claimJobs({
        queue: QUEUE,
        workerId,
        nowMs: nowMs(),
        leaseMs: 30_000,
        limit: 1,
      });
    } catch (e) {
      // No executable data here -- recover before doing anything.
      if (e.code !== "ClaimIndeterminate") throw e;
      const transactionId = /transactionId=([0-9a-f]{32})/.exec(e.message)[1];
      console.log(`[claim]  indeterminate; recovering ${transactionId}`);
      const recovery = store.recoverClaim(transactionId, nowMs());
      if (recovery.kind !== "committed") continue; // maintenance-only or never leased
      for (const stale of recovery.staleJobs) {
        console.log(`[claim]  stale job ${stale}: do not execute; surfaces as Uncertain`);
      }
      if (recovery.jobs.length === 0) continue; // every receipt job went stale
      return { transactionId: recovery.transactionId, job: recovery.jobs[0] };
    }
    if (outcome.kind === "maintenanceCommitted") continue; // progress made; poll again
    if (outcome.kind === "noop") throw new Error("queue unexpectedly empty");
    const job = outcome.jobs[0];
    console.log(`[claim]  ${workerId} leased job on ${job.partitionKey} (attempt ${job.attempt})`);
    return { transactionId: outcome.transactionId, job };
  }
}

export function main() {
  const dir = mkdtempSync(join(tmpdir(), "minisqlite-node-"));
  const db = join(dir, "synara-control-plane.db");

  console.log("=== happy path: one provider turn ===");
  {
    const store = Store.open(db);
    requestTurn(store, "t-100");
    const { job } = claimOne(store, "worker-1");
    heartbeat(store, job);
    startTurn(store, "t-100");
    callProvider(job);
    completeTurn(store, "t-100", job);
    const status = threadStatus(store, "t-100");
    console.log(`[state]  threads/t-100 = ${status}`);
    assert.equal(status, '{"status":"idle"}');
    store.close();
  }

  console.log("");
  console.log("=== failure drill: worker crash after claim, before ack ===");
  let claimTx;
  {
    const store = Store.open(db);
    requestTurn(store, "t-200");
    const { transactionId, job } = claimOne(store, "worker-2");
    console.log(
      `[crash]  worker-2 dies before acking job on ${job.partitionKey} (claim tx ${transactionId})`,
    );
    claimTx = transactionId;
    store.close(); // simulated process crash
  }

  // A recovering process reopens the store. It knows only the claim
  // transaction id (e.g. from its WAL/journal) -- no payloads, no lease tokens.
  const store = Store.open(db);
  console.log(`[reopen] store reopened; recovering claim ${claimTx}`);
  const recovery = store.recoverClaim(claimTx, nowMs());
  assert.equal(recovery.kind, "committed");
  for (const stale of recovery.staleJobs) {
    console.log(`[recover] stale job ${stale}: not executable; surfaces as Uncertain`);
  }
  assert.equal(recovery.jobs.length, 1);
  console.log(
    `[recover] claim receipt found: ${recovery.jobs.length} job(s), original lease tokens restored`,
  );
  for (const job of recovery.jobs) {
    heartbeat(store, job);
    startTurn(store, "t-200");
    callProvider(job); // exactly once, under the recovered lease
    completeTurn(store, "t-200", job);
  }
  const status = threadStatus(store, "t-200");
  console.log(`[state]  threads/t-200 = ${status}`);
  assert.equal(status, '{"status":"idle"}');

  const succeeded = store.jobs(QUEUE, "succeeded", 10);
  const events = store.eventsAfter(0, 100);
  console.log(
    `[state]  ${succeeded.length} succeeded job(s) on queue ${QUEUE}; ${events.length} event(s) total`,
  );
  assert.equal(succeeded.length, 2);
  assert.equal(events.length, 8);
  store.close();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}
