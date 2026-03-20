# Locking And Recovery

Pool-backed submissions use a remote lock directory so only one process updates
pool runtime state at a time.

## Lock Location

Each pool uses a directory under the remote home directory:

```text
$HOME/.aiida-flux-scheduler/locks/<pool-name>.lock
```

Pool names are sanitized before they are used in the path.

## Lock Metadata

After a lock is acquired, the scheduler writes:

```text
owner.json
```

inside the lock directory. The metadata currently includes:

- the pool name
- the UTC acquisition timestamp
- the acquisition epoch timestamp

## Stale-Lock Recovery

If lock acquisition fails because the directory already exists, the scheduler
checks the lock age. It prefers the timestamp recorded in `owner.json` and
falls back to the directory modification time for older lock directories.

When a lock is older than the scheduler threshold, the plugin removes the
metadata file, removes the lock directory, and retries acquisition.

Current default behavior:

- lock wait timeout: 60 seconds
- stale-lock threshold: `max(lock timeout, 300 seconds)`

These values are internal defaults in the current implementation and are not
yet exposed through the pool CLI.
