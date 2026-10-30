---
type: feat
"TursoSync"
---

A sync request now gives up when the server stops answering, after one minute by default, instead of after `HttpClient`'s 100 seconds. Set it with the new `Http Timeout` connection-string key (milliseconds; `TursoSyncConfig.HttpTimeoutMs`), or `-1` to wait forever. The timeout also applies to each read of a response body, so a download that stops arriving fails too, and it never cuts a configured `Long Poll Timeout` short. This matters beyond the failed request: a push or pull holds the sync database until it ends, and opening a new connection waits for it.

Opening a connection no longer fails with "database is busy" just because it first had to wait for a push or pull to finish. The wait used to count against the connection's `Busy Timeout`, so after a long push the first lock collision failed at once. The busy timeout now covers lock contention only.