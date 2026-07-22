// Runs the ported Synara provider-turn vertical slice (happy path plus the
// crash-before-ack recovery drill) under Bun. The example asserts internally.
import { test } from "bun:test";
import { main } from "../examples/synara_provider_turn.mjs";

test("synara provider turn: happy path and crash-before-ack recovery", () => {
  main();
});
