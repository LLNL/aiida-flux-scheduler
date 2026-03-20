from types import SimpleNamespace

from aiida.schedulers.datastructures import JobInfo, JobState

from aiida_flux_scheduler.flux import FluxJobResource, FluxScheduler
from aiida_flux_scheduler.pools import (
    create_or_update_pool_group,
    get_pool_runtime,
)


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.commands = []

    def exec_command_wait(self, command):
        self.commands.append(command)
        return self.responses.pop(0)


def test_validate_resources_accepts_flux_pool():
    resources = FluxJobResource.validate_resources(
        num_machines=2,
        num_mpiprocs_per_machine=4,
        max_wallclock_seconds='60',
        flux_pool='shared',
    )

    assert resources['num_machines'] == 2
    assert resources['num_mpiprocs_per_machine'] == 4
    assert resources['max_wallclock_seconds'] == 60
    assert resources['flux_pool'] == 'shared'


def test_parse_seconds_field_handles_float_and_blank():
    scheduler = FluxScheduler()

    assert scheduler._parse_seconds_field('10.9', 'time_used', '123') == 10
    assert scheduler._parse_seconds_field('', 'time_used', '123') is None
    assert scheduler._parse_seconds_field(None, 'time_used', '123') is None


def test_parse_joblist_output_parses_flux_time_fields():
    scheduler = FluxScheduler()
    stdout = (
        'HEADER\n'
        '123|R||user|2|64|node[1-2]|debug|120.0|30.0|1710000000.0|aiida-42|1709990000.0\n'
    )

    jobs = scheduler._parse_joblist_output(0, stdout, '')

    assert len(jobs) == 1
    job = jobs[0]
    assert job.job_id == '123'
    assert job.job_state == JobState.RUNNING
    assert job.title == 'aiida-42'
    assert job.requested_wallclock_time_seconds == 120
    assert job.wallclock_time_seconds == 30
    assert job.queue_name == 'debug'


def test_start_allocation_builds_expected_flux_alloc_command():
    transport = FakeTransport([(0, 'flux-123\n', '')])
    scheduler = FluxScheduler()
    scheduler._transport = transport

    flux_id = scheduler._start_allocation(
        {
            'resources': {
                'num_machines': 2,
                'num_mpi_procs_per_machine': 4,
                'queue_name': 'debug',
                'max_wallclock_seconds': 600,
                'account': 'project-a',
            },
            'job_name': 'aiida-pool-shared',
            'timeout': '10m',
        },
        parent_pk=42,
    )

    assert flux_id == 'flux-123'
    assert transport.commands == [
        "flux alloc --job-name=aiida-pool-shared --nodes=2 -n 8 -q debug "
        "-B project-a -t 600s -x --bg bash -c 'while true; do sleep 60; if "
        "[ $(flux jobs --since=-10m | wc -l) -gt 1 ]; then continue; else "
        "exit; fi; done'"
    ]


def test_check_allocation_matches_exact_title_only():
    scheduler = FluxScheduler()

    fuzzy = JobInfo()
    fuzzy.job_id = '111'
    fuzzy.title = 'prefix-aiida-42-suffix'
    fuzzy.requested_wallclock_time_seconds = 100
    fuzzy.wallclock_time_seconds = 10

    exact = JobInfo()
    exact.job_id = '222'
    exact.title = 'aiida-42'
    exact.requested_wallclock_time_seconds = 90
    exact.wallclock_time_seconds = 15

    scheduler.get_jobs = lambda: [fuzzy, exact]

    state = scheduler._check_allocation(42)

    assert state.active is True
    assert state.flux_id == '222'
    assert state.walltime == 75


def test_acquire_pool_lock_recovers_stale_lock(monkeypatch):
    transport = FakeTransport(
        [
            (0, '', ''),
            (1, '', 'exists'),
            (0, '', ''),
            (0, '', ''),
        ]
    )
    scheduler = FluxScheduler()
    scheduler._transport = transport

    written = []
    monkeypatch.setattr(scheduler, '_write_pool_lock_metadata', written.append)
    monkeypatch.setattr(scheduler, '_get_pool_lock_age_seconds', lambda pool_name: 600)
    monkeypatch.setattr('aiida_flux_scheduler.flux.time.sleep', lambda _: None)

    scheduler._acquire_pool_lock('shared', timeout_seconds=1, stale_lock_seconds=300)

    lock_path = scheduler._get_pool_lock_path('shared')
    metadata_path = scheduler._get_pool_lock_metadata_path('shared')
    assert transport.commands == [
        'mkdir -p "$HOME/.aiida-flux-scheduler/locks"',
        f'mkdir "{lock_path}"',
        f'rm -f "{metadata_path}" && rmdir "{lock_path}"',
        f'mkdir "{lock_path}"',
    ]
    assert written == ['shared']


def test_resolve_allocation_config_uses_pool_definition(
    aiida_profile_clean,
    aiida_computer_local,
):
    computer = aiida_computer_local(label='flux-config', configure=True)
    group, _ = create_or_update_pool_group(
        pool_name='shared',
        computer_label=computer.label,
        resources={
            'num_machines': 2,
            'num_mpiprocs_per_machine': 8,
            'max_wallclock_seconds': 600,
        },
        timeout='10m',
    )

    node = SimpleNamespace(
        user=group.user,
        computer=computer,
        get_metadata_inputs=lambda: {
            'metadata': {
                'options': {
                    'resources': {
                        'flux_pool': 'shared',
                    }
                }
            }
        },
    )

    config = FluxScheduler()._resolve_allocation_config(node)

    assert config['pool'] == 'shared'
    assert config['group'].pk == group.pk
    assert config['resources']['num_mpiprocs_per_machine'] == 8
    assert config['timeout'] == '10m'


def test_get_or_create_pooled_allocation_reuses_existing_runtime(
    aiida_profile_clean,
    aiida_computer_local,
    monkeypatch,
):
    computer = aiida_computer_local(label='flux-reuse', configure=True)
    group, _ = create_or_update_pool_group(
        pool_name='shared',
        computer_label=computer.label,
        resources={
            'num_machines': 2,
            'num_mpiprocs_per_machine': 8,
            'max_wallclock_seconds': 600,
        },
    )

    scheduler = FluxScheduler()
    scheduler.flux_values = {'t': 120}

    releases = []
    monkeypatch.setattr(scheduler, '_acquire_pool_lock', lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler, '_release_pool_lock', releases.append)
    monkeypatch.setattr(scheduler, '_start_allocation', lambda *args, **kwargs: 'new-id')

    job = JobInfo()
    job.job_id = 'old-id'
    job.requested_wallclock_time_seconds = 500
    job.wallclock_time_seconds = 100

    monkeypatch.setattr(scheduler, '_get_top_level_job', lambda flux_id: job)

    runtime = get_pool_runtime(group)
    runtime['current_flux_id'] = 'old-id'
    group.base.extras.set(
        'aiida_flux_scheduler',
        {
            **group.base.extras.get('aiida_flux_scheduler'),
            'runtime': runtime,
        },
    )

    node = SimpleNamespace(user=group.user, computer=computer)
    flux_id = scheduler._get_or_create_pooled_allocation(
        node,
        parent_pk=42,
        allocation_config={'pool': 'shared'},
    )

    assert flux_id == 'old-id'
    runtime = get_pool_runtime(group)
    assert runtime['current_flux_id'] == 'old-id'
    assert runtime['last_used_by_parent_pk'] == 42
    assert runtime['remaining_seconds'] == 400
    assert releases == ['shared']


def test_get_or_create_pooled_allocation_replaces_expired_runtime(
    aiida_profile_clean,
    aiida_computer_local,
    monkeypatch,
):
    computer = aiida_computer_local(label='flux-replace', configure=True)
    group, _ = create_or_update_pool_group(
        pool_name='shared',
        computer_label=computer.label,
        resources={
            'num_machines': 2,
            'num_mpiprocs_per_machine': 8,
            'max_wallclock_seconds': 600,
        },
    )

    runtime = get_pool_runtime(group)
    runtime['current_flux_id'] = 'old-id'
    group.base.extras.set(
        'aiida_flux_scheduler',
        {
            **group.base.extras.get('aiida_flux_scheduler'),
            'runtime': runtime,
        },
    )

    scheduler = FluxScheduler()
    scheduler.flux_values = {'t': 120}

    cancelled = []
    monkeypatch.setattr(scheduler, '_acquire_pool_lock', lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler, '_release_pool_lock', lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler, '_cancel_job_if_exists', cancelled.append)
    monkeypatch.setattr(scheduler, '_start_allocation', lambda *args, **kwargs: 'new-id')

    job = JobInfo()
    job.job_id = 'old-id'
    job.requested_wallclock_time_seconds = 150
    job.wallclock_time_seconds = 50

    monkeypatch.setattr(scheduler, '_get_top_level_job', lambda flux_id: job)

    node = SimpleNamespace(user=group.user, computer=computer)
    flux_id = scheduler._get_or_create_pooled_allocation(
        node,
        parent_pk=77,
        allocation_config={'pool': 'shared'},
    )

    assert flux_id == 'new-id'
    assert cancelled == ['old-id']
    runtime = get_pool_runtime(group)
    assert runtime['current_flux_id'] == 'new-id'
    assert runtime['last_used_by_parent_pk'] == 77
    assert runtime['state'] == 'active'
