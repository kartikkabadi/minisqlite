# PR rules

Rules for all pull requests in the control-plane kernel work. See
[ROADMAP.md](./ROADMAP.md) for the recommended PR sequence.

## Rules

1. **One invariant or subsystem per PR.** Each PR introduces or changes exactly
   one primary invariant or one subsystem (e.g. event append, projection
   patches, job retry). If a PR description needs the word "and" to explain
   what it does, split it.

2. **Tests land in the same PR.** Any behavior change ships with the tests
   that pin it. No follow-up-PR promises for test coverage.

3. **Keep PRs small.** Aim for roughly **1500 changed production lines or
   fewer** where practical. Generated files, lockfiles, and test fixtures do
   not count against the budget, but reviewability always wins.

4. **No opportunistic refactors.** Do not bundle unrelated cleanup, renames,
   or restructuring into a PR. Refactors get their own PR with their own
   justification.

5. **Update the relevant ADR or contract document.** A PR that changes
   documented behavior, a public contract, or an architectural decision must
   update the corresponding doc in the same PR (or add a superseding ADR).

6. **Identify migration implications.** Every schema or on-disk format change
   must state its migration implications (forward-only during alpha) and
   include the migration in the same PR.

7. **Benchmark evidence when performance-sensitive.** PRs touching commit,
   claim, read, or scheduling paths must include benchmark evidence with a
   documented environment.

8. **Independent review.** The implementing agent or author may not be the
   sole reviewer.

9. **Merge commits preferred.** Merge PRs with a merge commit (no squash by
   default, no rebase-merge) so the granular commit history within each PR is
   preserved on `main`.

## Pre-merge checklist

Immediately before merging, verify that:

- every unresolved review thread has been re-read and resolved in code or
  explicitly rejected with reasoning (not just marked outdated);
- the final diff has been inspected end to end;
- CI has been rerun and is green at the exact head commit, not an earlier one;
- no scope has silently crept in beyond the PR's stated invariant/subsystem;
- the branch is still based on the expected parent.
