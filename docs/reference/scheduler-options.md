# Scheduler Options

The Flux scheduler plugin reads allocation settings from
`metadata.options.resources`.

## Supported Keys

| Key | Type | Required | Purpose |
| --- | --- | --- | --- |
| `num_machines` | integer | Yes for direct submission and pool creation | Number of nodes requested for the top-level Flux allocation. |
| `num_mpiprocs_per_machine` | integer | Yes for direct submission and pool creation | Number of MPI ranks per node used to compute the `flux alloc -n` value. |
| `max_wallclock_seconds` | integer | Yes in practice for direct submission and required for pool creation | Wallclock limit for the top-level Flux allocation. |
| `queue_name` | string | No | Flux queue or partition passed to `flux alloc -q`. |
| `account` | string | No | Flux account or bank passed to `flux alloc -B`. |
| `flux_pool` | string | No | Name of a stored pool definition to load instead of per-job allocation settings. |
| `num_mpi_procs_per_machine` | integer | Compatibility alias | Legacy spelling accepted when resolving allocation parameters. |

## Example

```python
builder.metadata.options.resources = {
    'num_machines': 2,
    'num_mpiprocs_per_machine': 64,
    'max_wallclock_seconds': 7200,
    'queue_name': 'batch',
    'account': 'my-project',
}
```

## Notes

- `flux_pool` changes the allocation source: the pool definition supplies the
  allocation resources, timeout, and enablement state.
- `num_cores_per_mpiproc` is still used when AiiDA writes the Flux batch
  header for the submitted job, even though it is not part of the pool CLI.
