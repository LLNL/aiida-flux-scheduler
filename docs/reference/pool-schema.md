# Pool Schema

Pool definitions are stored as AiiDA `Group` records whose extras contain a
plugin-managed payload.

## Group Label

The group label is deterministic:

```text
aiida_flux_scheduler:pool:<user-email>:<computer-label>:<pool-name>
```

## Stored Payload

The plugin stores pool data under the extra key `aiida_flux_scheduler`.

Example payload:

```json
{
  "name": "production",
  "computer": "flux-cluster",
  "user": "user@example.org",
  "enabled": true,
  "timeout": "5m",
  "resources": {
    "num_machines": 4,
    "num_mpiprocs_per_machine": 32,
    "max_wallclock_seconds": 14400,
    "queue_name": "batch",
    "account": "my-project"
  },
  "runtime": {
    "current_flux_id": "f123456",
    "state": "active",
    "last_used_by_parent_pk": 1001,
    "last_validated_at": "2026-03-19T12:00:00+00:00",
    "remaining_seconds": 8200,
    "updated_at": "2026-03-19T12:00:00+00:00"
  }
}
```

## Runtime Fields

The `runtime` mapping is managed by the scheduler and may be empty for a newly
created pool. Fields currently used by the plugin include:

- `current_flux_id`
- `state`
- `last_used_by_parent_pk`
- `last_validated_at`
- `remaining_seconds`
- `updated_at`

Deleting a pool with runtime state still pointing at an allocation requires the
CLI `--force` flag.
