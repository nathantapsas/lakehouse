
# Todo:

- Link rows to file manifest, probably by new primary key value.
- Search file by regex rather than glob (remove _partial)

- Implement the connection to postgres form sqlmesh. This is where we can store gold scd tables.
- Speed up the upsert discovered files method in ledger store, it's way too slow.

- Implement a clean up / consolidate parquet method, maybe in ledger store, or separate script.
- Do we need spec hash in manifest table? Should we instead write a script to mark a file for reprocessing?

- Add security added date to openview query

- Some position costs are negative and some positive, how does this make sense?
- Figure out the business key columns for transaction table

- CRS table's "client code" is actually "client id"??? maybe join with client table to fix?
- Get statuses for client and account table, why do we have nulls in account status?

- Calculate the mapping between as_of_date and __data_snapshot_date. The transactions table will now be 'process_date = Toady Minus 1 Business day", rather than >=, this will allow an easy mapping.

- Transaction table, 26-01-28:
    "880-0024-5"	"01/27/26"	"5,168.04"	"01/27/26"	"91324M109"	"A/C 077-2310-Q NOT FOUND 
    PRICE 13.3541 PRO INCLUDED"	"0."	"C"	"QRRX"	"6,308.10"	"09:36:19"	"shawal"	"202601"	""	"387.00"	"JE26014400"	"0"	"01/28/26"	"5"	"0000034579"	"01/27/26"	"NO"	"5168.04"	"YES"	"MBY - BUY-MANAGED ACCT"


- Run explain analyze on some real data with muliple __data_snapshot_dates to make sure that we are not transforming the entire bronze table before filtering.