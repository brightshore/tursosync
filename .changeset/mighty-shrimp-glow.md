---
type: feat
"TursoSync"
---

Transactions now take the write lock when they begin. BeginTransaction() and BeginTransactionAsync() send BEGIN IMMEDIATE instead of BEGIN, matching Microsoft.Data.Sqlite, so a transaction that reads before it writes no longer fails with a stale snapshot when another writer commits first. This changes behaviour: a contended BEGIN now waits for the busy timeout and can fail busy there, and other writers, pulls and cold opens on a synced database wait for the open transaction, even one that only reads. Use the new BeginTransaction(deferred: true) for read-only transactions; IsolationLevel.ReadUncommitted stays deferred. Closing a connection with a transaction still open now rolls it back rather than returning it to the pool holding its locks, and a rollback waits out a busy database instead of failing during a checkpoint.