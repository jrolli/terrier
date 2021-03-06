# Design Doc: Replication

## Overview

- Built on top of the [thesis](https://gustavoangulo.github.io/thesis.pdf) Gus wrote, with some important tweaks:
  - To support synchronous replication, it is necessary to sometimes send the Oldest Active Transaction (OAT) serialized on the primary.
  - Uses the `Messenger` instead of the `ITP`. 

## Replication

### Semantics

#### Overview

NoisePage has the following semantics, loosely modeled off SingleStore:

Durability: do commits wait for logs to be written to disk?
- DISABLE: Logs are not written to disk. This also disables replication.
- SYNC: Commits must wait for logs to be written to disk.
- ASYNC: Commits do not need to wait for logs to be written to disk.

Replication: do logs get replicated across nodes?
- DISABLE: Logs are not replicated.
- SYNC: Commits must wait for logs to be replicated (and applied^).
- ASYNC: Commits do not need to wait for logs to replicated. But logs will be replicated, eventually.

^ Note that this is a stronger requirement than SingleStore.

#### Comparison to Postgres streaming replication

##### Granularity

By default, Postgres streaming replication has two types of log shipping:
- File-based: ship at the granularity of 16 MB WAL segments
- Record-based: ship at the granularity of records

NoisePage is currently kind of file-based, but the "file size" is whatever is in the `LogSerializerTask`.

##### Monitoring

- Postgres has
  - Primary: `pg_current_wal_lsn`, `sent_lsn`, determines what has been sent so far
  - Replica: `pg_last_wal_receive_lsn`, determines what has been received so far
- We don't have any LSNs in NoisePage. The best we can do is txn start time, which is not helpful here. For example,
  - First txn has ID 500.
  - Second txn has ID 9999999.
  - No easy way of tracking progress.

##### Sync/async

Sync
- Postgres controls this with the `synchronous_commit` setting, roughly in order of decreasing strength:
  - `remote_apply`: replicas must have flushed AND applied the data.
  - `on`: default setting, all `synchronous_standby_names` servers must have flushed the data.
  - `local`: only wait for txn to be flushed to local disk.
  - `remote_write`: replicas must have written the data (but not necessarily fsync).
  - `off`: see async below.

Async
- Postgres ships WAL records after txn commit.
- Postgres hot standbys are eventually consistent.

[Cybertec](https://www.cybertec-postgresql.com/en/the-synchronous_commit-parameter/) has published some pgbench numbers (tps).
- Async `off`: 6211
- Async `on`: 4256
- Sync `remote_write`: 3720
- Sync `on`: 3329
- Sync `remote_apply`: 3055

NoisePage currently is closest to `remote_apply` when synchronous replication is specified. It isn't exactly apples-to-apples, but:
- To support `on` and `remote_write`, modify `RecoveryManager` and/or `ReplicationLogProvider` to ACK as soon as the txn is seen, without waiting for the txn to be applied. Switch between replica SYNC and ASYNC durability to get the desired behavior.
- To support `local`, is to run the primary with SYNC durability ASYNC replication.

### Lifecycle

1. A transaction in the system generates various records, such as `RedoRecord`, `CommitRecord`, etc.
2. The records generated by these transactions eventually end up in the `LogSerializerTask` as a batch of records.
   - The batch of records records may have associated `CommitCallback` objects.
   - A `CommitCallback` is what wakes up the network thread to return the result of a query, see `TrafficCop::CommitCallback`.
   - Replay can be a bit complicated:
     - In NoisePage, records are guaranteed to be ordered within a transaction.
     - In NoisePage, records are NOT guaranteed to be ordered across different transactions.
     - For this reason, it is not safe to naively replay records -- see the thesis by Gus for details.
     - At a high level, wait until it is "safe" to replay a transaction by using the `OldestActiveTransaction` (OAT) time: a txn is safe to replay if it is less than OAT.
     - What does it mean for a transaction to be active?
       - Before: a transaction is active until it commits.
       - At time of writing: a transaction is active until it is serialized. This can hang synchronous replication without the `NotifyOATMsg` mechanism below.
3. On the primary, the `LogSerializerTask` hands the batch of records to the `PrimaryReplicationManager`.
4. Then, depending on the replication mode:
  - ASYNC: The commit callbacks are invoked immediately. The batch of records is sent to the replicas.  
  - SYNC: The commit callbacks are stored until the relevant `TxnAppliedMsg` are received from the replicas. The batch of records is sent to the replicas.
5. On the replicas, the `Messenger` thread will pick up the batch of records while polling on all the custom serverloop callbacks (SLCs).
6. The `Messenger` forwards the batch of records to the `ReplicaReplicationManager`'s `EventLoop`, which forwards the batch of records to the `ReplicationLogProvider`.
7. The `ReplicationLogProvider` performs its own buffering until the batch of records has the right batch number, etc.
8. When the `ReplicationLogProvider` is ready to provide logs, the condition in `ReplicationLogProvider::WaitForEvent` returns accordingly. That condition was being polled by the replica's dedicated `RecoveryManager` thread.
9. The `RecoveryManager` processes each record in the batch by **deferring** the record, until the `RecoveryManager` sees an OAT. There are two ways this could happen:
  - Each `CommitRecord` has an `OAT` piggybacking on it as a performance optimization.
  - The primary's `LogSerializerTask` may independently send `NotifyOATMsg` to the replicas when it realizes that there are no transactions left.
10. When the `RecoveryManager` sees an OAT, the `RecoveryManager` applies deferred transactions up to the OAT, sending a `TxnAppliedMsg` back to the primary for each transaction that was applied.

### Implementation

Note that read-only transactions are not replicated across the network as their commit records are not serialized.  
This has some implications for bookkeeping in the `ReplicationManager` code.

#### Message types

- `RecordsBatchMsg` : a batch of records that should be applied on the replica.
  - Sent from Primary -> Replica.
  - Created in the `LogSerializerTask` when a batch of records is ready to be handed off (e.g., to disk).
  - Used in the replica's `RecoveryManager` where each record's application is deferred until the OAT is seen.
- `NotifyOATMsg` : the latest Oldest Active Transaction time on the primary.
  - Sent from Primary -> Replica.
  - Created in the `LogSerializerTask` when there are no transactions left.
  - Used by the replica's `RecoveryManager` to fire off deferred transactions.
  - This is crucial for synchronous replication, which may otherwise have the last couple of records be stuck in limbo.
- `TxnAppliedMsg` : a transaction which has been applied on the replica.
  - Sent from Replica -> Primary.
  - Created in the `RecoveryManager`
  - Used by the primary's `ReplicationManager` to invoke commit callbacks for transactions that have been applied on all replicas.

## Replication (gone wrong)

Outdated but possibly useful meeting notes from an old buggy implementation of replication.

- Issues with the old implementation.
    - Problem: Conflating "I've received batch X of logs" with "I've applied batch X of logs."
        - Pathological case: long-running txn 1  
          Batch 1 = txn 1 starts, (other random transactions have log records).   
          All subsequent batches: txn 1 never commits but has one record in every batch.   
          This pollutes every batch and prevents all batches from being acknowledged.
    - Problem: We cannot acknowledge a log record until the log record has been applied by both disk and replication. 
        - Because of the deferred applications, it is difficult to design bookkeeping at a per-batch level of what logs have or have not been applied.  
          For example, suppose batch 42 contained [T1 record 2/?, T2 record 2/2, T3 record 3/?].  
          How would you maintain that bookkeeping?  
    - Problem: Who invokes commit callbacks?
        - Previously, an experimental branch had a CommitCallbackManager.  
        - Previously: "a commit callback should be invoked exactly once, by the subsystem that is last"  
        - Now: "every subsystem invokes the commit callback"  
          Commit callback has counting semaphore to wake up network thread.  
- Going forward, high-level plan for sync replication:
    - Commit callbacks will now be invoked by all "serializing" components (disk, replication). Previously tried to only invoke once.
    - Serializer has to hand the replication manager (batch of log records, (list of txn ids with their commit callbacks)).
    - Replicas send back the list of processed deferred transactions. Replication manager receives this and invokes commit callbacks.
    - Some engineering around the above.
    - Read-only txns may require replica OAT heartbeat to work.
- Things that need to exist
    0. Matt's commit callback counting semaphore thing
    1. Serializer has to hand replication manager (batch, (txn start,commit callbacks in batch))
    2. Replicas have to send back their list of deferred transactions that were processed, periodically.
       vec<txn_start_times>
    3. The replication manager on the primary receives that list
       Invokes the respective commit callbacks
- This should  
    Completely decouple batch "receive ack" from "applied ack".  
    Which would eliminate the current "everything has to happen in one batch" limitation.  
  