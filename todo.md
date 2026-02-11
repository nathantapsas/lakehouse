
# Todo:

- Link rows to file manifest, probably by new primary key value.
- Search file by regex rather than glob (remove _partial)

- Implement the connection to postgres form sqlmesh. This is where we can store gold scd tables.
- Speed up the upsert discovered files method in ledger store, it's way too slow.

- Implement a clean up / consolidate parquet method, maybe in ledger store, or separate script.
- Do we need spec hash in manifest table? Should we instead write a script to mark a file for reprocessing?