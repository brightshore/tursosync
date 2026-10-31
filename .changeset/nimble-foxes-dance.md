---
type: fix
"TursoSync"
---

Split sync pushes into batches of 1,000 operations by default, preserving transactions and resuming from the last acknowledged batch after a failed request. Configure the threshold with Push Operations Threshold; zero retains an unlimited batch.