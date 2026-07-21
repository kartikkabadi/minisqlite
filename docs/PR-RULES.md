# PR rules

Rules for all pull requests in the control-plane kernel work. See
[ROADMAP.md](./ROADMAP.md) for the recommended PR sequence.

## Rules

1. **One invariant or subsystem per PR.** Each PR introduces or changes exactly
   one invariant or one subsystem (e.g. event append, projection rebuild, job
   retry). If a PR description needs the word "and" to explain what it does,
   split it.

2. **Tests land in the same PR.** Any behavior change ships with the tests
   that pin it. No follow-up-PR promises for test coverage.

3. **Keep PRs small.** Aim for roughly **1500 changed production lines or
   fewer** where practical. Generated files, lockfiles, and test fixtures do
   not count against the budget, but reviewability always wins.

4. **Merge commits preferred.** Merge PRs with a merge commit (no squash, no
   rebase-merge) so the granular commit history within each PR is preserved on
   `main`.

5. **Recheck before merge.** Immediately before merging, re-verify that:
   - all review threads are resolved (not just marked outdated), and
   - CI is green on the latest commit, not an earlier one.
