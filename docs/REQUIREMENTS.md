# MiniSQLite Requirements Audit

| Requirement | Source | Why it exists | What would let us delete it? |
| --- | --- | --- | --- |
| Ordered event sequence | Synara-shaped control plane | Deterministic replay and consumer cursors | Delete only if order is proven irrelevant |
| Atomic batches | User-visible state and resulting work must agree | Prevent impossible half-state | Cannot delete |
| SQL | Existing repository ancestry | No customer requirement | Delete immediately |
| B+ tree | Generic database analogy | No measured access need | Delete |
| Query planner | Generic SQL architecture | No arbitrary query workload | Delete |
| Multi-process writers | Generic database expectation | First use-case has one owner process | Delete |
| Distributed replication | Future speculation | No first user requires it | Delete |
| Persistent projections | Faster startup assumption | No measured replay bottleneck yet | Keep simple replayed maps; add snapshots only after measurement |
| Zero dependencies | Aesthetic preference | May improve auditability | Keep a low dependency budget, not an absolute dogma |
| One primary data file | Portability and supportability | Useful for local apps | Keep; permit a lock file and temporary recovery files |
| Automatic job execution | Framework ambition | Application already owns workers | Delete; store and lease jobs only |
