# Pooled Allocations

Pools allow multiple submissions to share a named Flux allocation that is
tracked in the AiiDA database. A pool definition belongs to a specific user and
computer.

## Create A Pool

```bash
aiida-flux pool create production \
  --computer flux-cluster \
  --num-machines 4 \
  --num-mpiprocs-per-machine 32 \
  --max-wallclock-seconds 14400 \
  --queue-name batch \
  --account my-project
```

Pool records can also be listed, inspected, enabled, disabled, and deleted via
the CLI. See [CLI reference](cli.md).

## Select A Pool In Job Resources

Set `flux_pool` in the job resources:

```python
builder.metadata.options.resources = {
    'flux_pool': 'production',
}
```

When a pool is selected, the scheduler loads the pool definition from the AiiDA
group payload instead of taking allocation parameters directly from the job.

## Allocation Reuse Rules

For a pooled submission, the scheduler:

1. acquires a per-pool remote lock
2. checks the pool runtime state for `current_flux_id`
3. validates that the referenced Flux allocation still exists
4. reuses the allocation if the remaining walltime is sufficient
5. otherwise clears runtime state, cancels the stale allocation when possible,
   and starts a replacement allocation

Disabled pools fail at submission time. Missing pool definitions also fail at
submission time with a scheduler error.
