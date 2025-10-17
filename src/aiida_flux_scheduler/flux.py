"""
Plugin for Flux.
"""

from typing import Any
from aiida.orm import load_node, CalcFunctionNode
from aiida.common.lang import type_check
from aiida.engine.processes.exit_code import ExitCode
from aiida.schedulers import Scheduler, SchedulerError
from aiida.schedulers.datastructures import JobInfo, JobState, JobTemplate, NodeNumberJobResource
from aiida.common.extendeddicts import AttributeDict
from aiida.common.escaping import escape_for_bash
import json
import datetime
from collections import defaultdict, namedtuple

_MAP_STATUS_FLUX = {
    'D': JobState.QUEUED,  # Depend
    'P': JobState.QUEUED,  # Priority
    'S': JobState.QUEUED,  # Scheduled
    'R': JobState.RUNNING, # Running
    'C': JobState.RUNNING, # Cleanup
    'CD': JobState.DONE,   # Completed
    'F': JobState.DONE,    # Failed
    'CA': JobState.DONE,   # Canceled
    'TO': JobState.DONE,   # Timeout
}

class FluxJobResource(NodeNumberJobResource):
    """
    Class for Flux job resources.
    """

    @classmethod
    def validate_resources(
        cls, 
        **kwargs: dict
    ) -> AttributeDict:
        """
        Validate the resouces against the job resource class of this scheduler.

        :param kwargs: dictionary of values to define the job resources.
        :return: attribute dictionary with the parsed parameters populated.
        """

        resources = super().validate_resources(**kwargs)

        return resources
    
class FluxScheduler(Scheduler):
    """
    Flux scheduler.
    """

    _FIELD_SEPARATOR="|"

    _features = {
        'can_query_by_user': False,
    }

    _job_resource_class = FluxJobResource

    fields = [
        ('id', 'job_id'),  # job or job step id
        ('status_abbrev', 'state_raw'),  # job state in compact form
        ('annotations', 'annotation'),  # reason for the job being in its current state
        ('username', 'username'),  # username
        ('nnodes', 'number_nodes'),  # number of nodes allocated
        ('ncores', 'number_cpus'),  # number of allocated cores (if already running)
        ('nodelist', 'allocated_machines'),  # list of allocated nodes when running, otherwise
        # reason within parenthesis
        ('queue', 'partition'),  # partition (queue) of the job
        ('duration', 'time_limit'),  # time limit in seconds
        ('runtime', 'time_used'),  # Time used by the job in days-hours:minutes:seconds
        ('t_run', 'dispatch_time'),  # actual or expected dispatch time (start time)
        ('name', 'job_name'),  # job name (title)
        ('t_submit', 'submission_time'),  # This is probably new, it exists in version
        # 14.03.7 and later
    ]

    def _get_joblist_command(
        self, 
        jobs: list[str] | None = None, 
        user: str | None = None,
        flux_id: str | None = None,
    ) -> str:
        """
        Command to report full information on an existing job.

        :param jobs: List of job ids.
        :param user: Username for the job queue.
        :param flux_id: Job ID of the active flux allocation to be used for the proxy command.
        :return comm: Command to retrieve full job information.
        """

        command = 'flux jobs {job_id} {user} {format}'

        fields = defaultdict(str, {})

        if flux_id:
            fields['flux_id'] = flux_id

        if jobs:
            command = 'flux proxy {flux_id} "' + command + '"'
            joblist = []
            if isinstance(jobs, str):
                joblist = jobs
            else:
                if not isinstance(jobs, (tuple, list)):
                    raise TypeError("If provided, the 'jobs' variable must be a string or a list of strings")
                joblist = ' '.join(jobs)
            fields['job_id'] = joblist

        if user:
            fields['user'] = f'-u {user}'

        fields['format'] = f"--format '{self._FIELD_SEPARATOR.join(f'{{{field[0]}}}' for field in self.fields)}'"
        comm = command.format_map(fields)

        self.logger.info(f'Checking joblist with {comm}')

        return comm
    
    def _get_detailed_job_info_command(
        self, 
        job_id: str
    ) -> dict[str, Any]:
        """
        Command to get detailed information on a job even after completion.

        :param job_id: Job id for the flux scheduler.
        :return comm: Command for detailed job info.
        """

        flux_id, child_id = job_id.split(':')

        return f"flux proxy {flux_id} flux job info {child_id} jobspec"
        
    def _get_submit_script_header(
        self, 
        job_tmpl: JobTemplate
    ) -> str:
        """
        Return the submit script header with the parameters from the job_tmpl.

        :param job_tmpl: JobTemplate instance with relevant parameters set.
        :return header: Job submission header as a string.
        """

        header = []

        if job_tmpl.job_name:
            header.append(f'#flux: --job-name={job_tmpl.job_name}')

        if job_tmpl.sched_output_path:
            header.append(f'#flux: --output={job_tmpl.sched_output_path}')

        if job_tmpl.sched_error_path:
            header.append(f'#flux: --error={job_tmpl.sched_error_path}')

        # When submitting to a flux instance you won't need to have a queue/bank
        #if job_tmpl.queue_name:
        #    header.append(f'#flux: -q {job_tmpl.queue_name}')

        #if job_tmpl.account:
        #    header.append(f'#flux: -B {job_tmpl.account}')
        
        if job_tmpl.priority:
            # Check that the specified value is within the appropriate range.
            # 0 - Hold
            # 16 - Default
            # 31 - Expedite
            priority = job_tmpl.priority
            if priority >= 31:
                priority = 31
            elif priority < 0:
                priority = 0
            header.append(f'#flux: --urgency={priority}')

        if not job_tmpl.job_resource:
            raise ValueError(
                'Job resources (number of nodes) are required for the Flux '
                'scheduler plugin.'
            )

        header.append(f'#flux: -N {job_tmpl.job_resource.num_machines}')
        if job_tmpl.job_resource.num_mpiprocs_per_machine:
            header.append(f'#flux: -n '
                f'{job_tmpl.job_resource.num_mpiprocs_per_machine * job_tmpl.job_resource.num_machines}'
            )
        
        if job_tmpl.job_resource.num_cores_per_mpiproc:
            header.append(
                f'#flux: -c {job_tmpl.job_resource.num_cores_per_mpiproc}'
            )

        if job_tmpl.max_wallclock_seconds is not None:
            try:
                tot_secs = int(job_tmpl.max_wallclock_seconds)
                if tot_secs <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError(
                    'max_wallclock_seconds must be ' "a positive integer (in seconds)! It is instead '{}'" ''.format(
                        (job_tmpl.max_wallclock_seconds)
                    )
                )

            # Check if total time is larger than day, hour, or minutes 
            # and convert to float.

            # Days
            if tot_secs > 86400:
                time = f'{tot_secs / 86400:.2f}d'
            # Hours
            elif tot_secs > 3600:
                time = f'{tot_secs / 3600:.2f}h'
            elif tot_secs > 60:
                time = f'{tot_secs / 60:.2f}m'
            else:
                time = tot_secs

            header.append(f'#flux: -t {time}')
        
        if isinstance(job_tmpl.custom_scheduler_commands, str) and job_tmpl.custom_scheduler_commands is not None:
            header.append(job_tmpl.custom_scheduler_commands)

        header = '\n'.join(header)         

        return header
    
    def _get_parent_pk(
            self, 
            pk: int
        ) -> int:
        """
        Return parent pk based on calculation pk retrieved.

        :param pk: AiiDA PK
        :return: The parent pk of the calculation pk.
        """

        parent = load_node(pk)
        while parent.caller:
            parent = parent.caller

        return parent.pk
    
    def _parse_pk(
        self,
        working_directory: str,
        submit_script: str,
    ) -> int:
        """
        Parse the AiiDA PK from the job submission script.

        :param working_directory: Path to the working directory on remote machine.
        :param submit_script: Name of the submission script.
        :returns: PK of the current calculation.
        """

        self.transport.chdir(working_directory)

        retval, stdout, stderr = self.transport.exec_command_wait(f'grep "#flux" {submit_script}')

        if retval != 0:
            self.logger.error(f'Error in _flux_allocation: {retval=}; {stdout=}; {stderr=}')
            raise SchedulerError(f'Error during submission, {retval=}\n{stdout=}\n{stderr=}')
        
        self.flux_values = {}
        items = stdout.strip().split('\n')
        for item in items:
            item = item.replace('#flux:', '').strip().strip('-')
            if '=' in item:
                item = item.split('=')
            else:
                item = item.split()
            self.flux_values[item[0]] = item[1]

        # Convert walltime to seconds
        walltime = self.flux_values['t']
        if 'd' in walltime:
            walltime = 86400 * float(walltime.strip('d'))
        elif 'h' in walltime:
            walltime = 3600 * float(walltime.strip('h'))
        elif 'm' in walltime:
            walltime = 60 * float(walltime.strip('m'))
        self.flux_values['t'] = walltime

        pk = int(self.flux_values['job-name'].split('-')[-1])

        return pk
        
    
    def _flux_allocation(
        self,
        pk: int
    ) -> int:
        """
        Start a flux allocation to submit jobs.

        :param pk: AiiDA PK of the current job submission.
        :returns: Flux job ID of the persistent allocation.
        """
        
        parent_pk = self._get_parent_pk(pk)

        # Based on the parent_pk, see if there is an active flux allocation.
        state = self._check_allocation(parent_pk)

        if not state.active:
            self.logger.info(f'No flux allocation found for aiida-{parent_pk}. Starting one now.')
            flux_id = self._start_allocation(pk, parent_pk)
        elif state.active:
            # Check if there is enough walltime left for the job.
            diff = state.walltime - self.flux_values['t']
            if diff < 0:
                self.logger.info(
                    'Current job exceeds remaining time. Killing allocation '
                    'and requesting a new allocation.'
                )
                # Kill allocation and start a new one
                kill_cmd = self._get_kill_command(state.flux_id)
                retval, stdout, stderr = self.transport.exec_command_wait(kill_cmd)
                if retval != 0:
                    self.logger.error(f'Error in _flux_allocation: {retval=}; {stdout=}; {stderr=}')
                    raise SchedulerError(f'Error during submission, {retval=}\n{stdout=}\n{stderr=}')
                flux_id = self._start_allocation(parent_pk)
            else:
                flux_id = state.flux_id

        self.logger.info(f'Flux instance for <{parent_pk}> is running with flux id: {flux_id}.')

        return flux_id

    def _check_allocation(
        self, 
        parent_pk: int
    ) -> namedtuple:
        """
        Given a parent_pk, check to see if Flux already has an active allocation.

        :param parent_pk: The parent PK of the job being submitted.
        :returns: State of the current parent allocation in Flux.
        """
        joblist = self.get_jobs()
        State = namedtuple(
            'State', 
            ['active', 'flux_id', 'walltime'], 
            defaults=[False, '', 0]
        )

        state = State()
        for job in joblist:
            if str(parent_pk) in job.title: 
                flux_id = job.job_id
                total = job.requested_wallclock_time_seconds
                used = job.wallclock_time_seconds
                remaining = float(total) - float(used)
                state = State(True, flux_id, remaining)

        return state

    def _start_allocation(
        self,
        pk,
        parent_pk
    ) -> str:
        """
        Start a flux allocation based on the job submission script in the working directory.

        :param pk: AiiDA pk of the current
        :param parent_pk: AiiDA pk of parent.
        :return: Job ID of the Flux instance.
        """
        node = load_node(pk)
        if (node, CalcFunctionNode):
            metadata = node.get_metadata_inputs()
            options = metadata.get('options', {})
            resources = options.get('resources', {})
            flux = options.get('persistent_resources', None)
            if flux is None:
                raise ValueError(
                    'Must specify `metadata.options.persistent_resources` to fully utilize the Flux scheduler.'
                )
        else:
            raise TypeError(f'{node} is not a recognized type for this scheduler.')
        
        keys = (
            'num_machines',
            'num_mpi_procs_per_machine',
            'queue_name',
            'max_wallclock_seconds',
            'account'
        )

        values = defaultdict(str, {})
        for resource in [resources, flux]:
            for key in keys:
                result = self.recursive_dict_search(key, resource)
                if result:
                    match key:
                        case 'num_machines':
                            values[key] = f'--nodes={result}'
                        case 'num_mpi_procs_per_machine':
                            values['num_tasks'] = f'-n {values["num_machines"] * result}'
                        case 'queue_name':
                            values[key] = f'-q {result}'
                        case 'max_wallclock_seconds':
                            values[key] = f'-t {int(result)}s'
                        case 'account':
                            values[key] = f'-B {result}'

        values['job_name'] = f'--job-name=aiida-{parent_pk}'

        timeout = flux.get('timeout', '5m')

        values['watcher'] = f"bash -c 'while true; do sleep 60; if [ $(flux jobs --since=-{timeout} | wc -l) -gt 1 ]; then continue; else exit; fi; done'"
            
        flux_submit = (
            'flux alloc {job_name} {num_machines} {num_tasks} '
            '{queue_name} {account} {max_wallclock_seconds} -x --bg {watcher}'
        )

        flux_submit = flux_submit.format_map(values)

        self.logger.info(f'Starting a flux allocation for parent workchain <{parent_pk}> with {flux_submit}')

        retval, stdout, stderr = self.transport.exec_command_wait(flux_submit)

        if retval != 0:
            self.logger.error(f'Error in _start_allocation {retval=}; {stdout=}; {stderr=}')

            raise SchedulerError(f'Error while starting a flux allocation, {retval=}\n{stdout=}\n{stderr=}')
        else:
            flux_id = stdout

        flux_id = flux_id.strip('\n')

        return flux_id

    def recursive_dict_search(
        self, 
        key, 
        dictionary: dict
    ):
        """
        Take a dictionary and look for the first instance of the key.
        """

        if key in dictionary:
            return dictionary[key]
        for value in dictionary.values():
            if isinstance(value, dict):
                result = self.recursive_dict_search(key, value)
                if result is not None:
                    return result
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        result = self.recursive_dict_search(key, item)
                        if result is not None:
                            return result
        return None


    def submit_from_script(self, working_directory: str, submit_script: str) -> str | ExitCode:
        """Submit the submission script to the scheduler.

        :return: return a string with the job ID in a valid format to be used for querying.
        """
        pk = self._parse_pk(working_directory, submit_script)
        flux_id = self._flux_allocation(pk)
        self.transport.chdir(working_directory)
        result = self.transport.exec_command_wait(
            self._get_submit_command(
                escape_for_bash(submit_script), 
                flux_id
            )
        )
        child_job_id = self._parse_submit_output(*result)

        total_job_id = f'{flux_id}:{child_job_id}'

        return total_job_id
    
    def get_jobs(
        self,
        jobs: list[str] | None = None,
        user: str | None = None,
        as_dict: bool = False,
    ) -> list[JobInfo] | dict[str, JobInfo]:
        """Return the list of currently active jobs.

        .. note:: typically, only either jobs or user can be specified. See 
            also comments in `_get_joblist_command`.

        :param list jobs: a list of jobs to check; only these are checked
        :param str user: a string with a user: only jobs of this user are checked
        :param list as_dict: if False (default), a list of JobInfo objects 
            is returned. If True, a dictionary is returned, having as key 
            the job_id and as value the JobInfo object.
        :return: list of active jobs
        """

        if jobs:
            joblist = []
            for job in jobs:
                flux_id, child_id = job.split(':')
                with self.transport:
                    retval, stdout, stderr = self.transport.exec_command_wait(
                        self._get_joblist_command(
                            jobs=[child_id], 
                            user=user,
                            flux_id=flux_id
                        )
                    )

                single_job = self._parse_joblist_output(retval, stdout, stderr)[0]
                single_job.job_id = f'{flux_id}:{single_job.job_id}'
                joblist.append(single_job)
        else:
            with self.transport:
                retval, stdout, stderr = self.transport.exec_command_wait(
                    self._get_joblist_command(
                        jobs=jobs, 
                        user=user
                    )
                )
            joblist = self._parse_joblist_output(retval, stdout, stderr)

        if as_dict:
            jobdict = {job.job_id: job for job in joblist}
            if None in jobdict:
                raise SchedulerError('Found at least one job without jobid')
            return jobdict

        return joblist

    def _get_submit_command(
        self, 
        submit_script: str,
        flux_id: str
    ) -> str:
        """
        Return the string to execute the submission script.

        :param submit_script: Path to the submission script relative to the 
            working directory.
        :return submit_command: Command used to submit the submission script.
        """

        submit_command = f"flux proxy {flux_id} flux batch {submit_script}"              

        self.logger.info(f'submitting with : {submit_command}')

        return submit_command
    
    def submit_job(
        self,
        working_directory: str,
        submit_script: str
    ) -> str | ExitCode:
        """
        Submit a job to the Flux scheduler.

        :param working_directory: Absolute filepath to working directory of the job to be submitted.
        :param submit_script: Name of the submission script relative to the working directory.
        """

        pk = self._parse_pk(working_directory, submit_script)
        parent_pk = self._get_parent_pk(pk)

        self.transport.chdir(working_directory)
        result = self.transport.exec_command_wait(
            self._get_submit_command(submit_script, parent_pk)
        )
        
        return self._parse_submit_output(*result)
        
    
    def _parse_submit_output(
        self, 
        retval: int, 
        stdout: str, 
        stderr: str
    ) -> str | ExitCode:
        """
        Parse the output from the submission command.

        :param retval: Return value of the submission command.
        :param stdout: Standard output.
        :param stderr: Error from standard output.
        :return job_id: Job ID from the submitted job.
        """

        if retval != 0:
            self.logger.error(f'Error in _parse_submit_output: {retval=}; {stdout=}; {stderr=}')

            raise SchedulerError(f'Error during submission, {retval=}\n{stdout=}\n{stderr=}')

        try:
            transport_string = f' for {self.transport}'
        except SchedulerError:
            transport_string = ''

        if stderr.strip():
            self.logger.warning(f'in _parse_submit_output{transport_string}: there was some text in stderr: {stderr}')

        stdout = stdout.strip('\n')
        if stdout:
            return stdout
        self.logger.error(f' in _parse_submit_output{transport_string}: unable to find the job id: {stdout}')
        raise SchedulerError(
            'Error during submission, cound not retrieve the jobID from flux output; see log for more info.'
        )
    
    def _parse_joblist_output(
        self, 
        retval: int, 
        stdout: str, 
        stderr: str
    ) -> list[JobInfo]:
        """
        Parse the output from the job queue as returned by the 
        _get_joblist_command command. The return is a list of lines, one for 
        each job.

        :param retval: Return value from the command.
        :param stdout: Standard output from command.
        :param stderr: Standard error from command.
        :return job_list: List of JobInfo instances for each submitted job.
        """

        num_fields = len(self.fields)

        # See discussion in _get_joblist_command on how we ensure that AiiDA can expect exit code 0 here.
        if retval != 0:
            raise SchedulerError(
                f"""flux jobs returned exit code {retval} (_parse_joblist_output function)
                stdout='{stdout.strip()}'
                stderr='{stderr.strip()}'"""
            )
        if stderr.strip():
            self.logger.warning(
                f"flux jobs returned exit code 0 (_parse_joblist_output function) but non-empty stderr='{stderr.strip()}'"
            )

        stdout_split = stdout.splitlines()[1:] # Remove the first line which has the header names.
        jobdata_raw = [line.split(self._FIELD_SEPARATOR,maxsplit=num_fields) for line in stdout_split]

        job_list = []

        for job in jobdata_raw:
            thisjob_dict = {k[1]: v for k, v in zip(self.fields, job)}

            this_job = JobInfo()
            try:
                this_job.job_id = thisjob_dict['job_id']
                this_job.annotation = thisjob_dict['annotation']
                job_state_raw = thisjob_dict['state_raw']
            except KeyError:
                self.logger.error(f"Wrong line length in flux output! '{job}'")
            
            try:
                job_state_string = _MAP_STATUS_FLUX[job_state_raw]
            except KeyError:
                self.logger.warning(F"Unrecognized job_state '{job_state_raw}' for job id {this_job.job_id}")
                job_state_string = JobState.UNDETERMINED
            this_job.job_state = job_state_string

            if len(job) < num_fields:
                self.logger.warning(f'Wrong line length in flux output! Skipping optional fields. Line: `{jobdata_raw}`')
                job_list.append(this_job)
                continue

            this_job.job_owner = thisjob_dict['username']

            try:
                this_job.num_machines = int(thisjob_dict['number_nodes'])
            except ValueError:
                self.logger.warning(
                    f"The number of allocated nodes is not an integer "
                    f"({thisjob_dict['number_nodes']}) for job id "
                    f"{this_job.job_id}!"
                )

            try:
                this_job.num_mpiprocs = int(thisjob_dict['number_cpus'])
            except ValueError:
                self.logger.warning(
                    f"The number of allocated cores is not an integer "
                    f"({thisjob_dict['number_cpus']}) for job id "
                    f"{this_job.job_id}!"
                )

            if this_job.job_state == JobState.RUNNING:
                this_job.allocated_machines_raw = thisjob_dict['allocated_machines']

            this_job.queue_name = thisjob_dict['partition']

            try:
                walltime = thisjob_dict['time_limit']
                this_job.requested_wallclock_time_seconds = walltime
            except ValueError:
                self.logger.warning(f'Error parsing the time limit for job id {this_job.job_id}')

            if this_job.job_state == JobState.RUNNING:
                try:
                    this_job.wallclock_time_seconds = thisjob_dict['time_used']
                except ValueError:
                    self.logger.warning(f'Error parsing time_used for job id {this_job.job_id}')

                try:
                    dispatch_time = float(thisjob_dict['dispatch_time'])
                    dispatch_time = datetime.datetime.fromtimestamp(dispatch_time)
                    this_job.dispatch_time = dispatch_time
                except ValueError:
                    self.logger.warning(f'Error parsing dispatch_time for job id {this_job.job_id}')

            try:
                submission_time = float(thisjob_dict['submission_time'])
                submission_time = datetime.datetime.fromtimestamp(submission_time)
                this_job.submission_time = submission_time
            except ValueError:
                self.logger.warning(f'Error parsing submission_time for job id {this_job.job_id}')

            this_job.title = thisjob_dict['job_name']

            job_list.append(this_job)

        return job_list
    
    def kill_job(
        self,
        jobid
    ) -> bool:
        """
        Function to kill a job on the Flux scheduler.

        :param jobid: Job ID within Flux
        :returns: True if executed correctly.
        """

        retval, stdout, stderr = self.transport.exec_command_wait(
            self._get_kill_command(jobid=jobid)
        )

        if retval != 0:
            self.logger.error(f'Error in kill_job {retval=}; {stdout=}; {stderr=}')

            raise RuntimeError(f'Error while kill Flux job, {retval=}\n{stdout=}\n{stderr=}')
        
        return True
    
    def _get_kill_command(
        self, 
        jobid: str
    ) -> str:
        """
        Return the command to kill the job with the specified jobid.

        :param jobid: Job ID of the job within Flux.
        :return comm: Command to kill a job within Flux.
        """

        return f"flux cancel {jobid}"
    
    def _parse_kill_output(
        self, 
        retval: int, 
        stdout: str, 
        stderr: str
    ) -> bool:
        """
        Parse the output returned from the kill command.

        :param retval: Return value from the kill command.
        :param stdout: Standard output from the kill command.
        :param stderr: Standard error from the kill command.
        :return: True if everything is ok, False otherwise.
        """

        if retval != 0:
            self.logger.error(
                f'Error in _parse_kill_output: retval={retval}; '
                f'stdout={stdout}; stderr={stderr}'
            )
            return False

        try:
            transport_string = f' for {self.transport}'
        except SchedulerError:
            transport_string = ''

        if stderr.strip():
            self.logger.warning(
                f'in _parse_kill_output{transport_string}: there was some '
                f'text in stderr: {stderr}'
            )

        if stdout.strip():
            self.logger.warning(
                f'in _parse_kill_output{transport_string}: there was some '
                f'text in stdout: {stdout}'
            )

        return 
    
    def parse_output(
        self, 
        detailed_job_info: dict[str, str | int] | None = None, 
        stdout: str | None = None, 
        stderr: str | None = None
    ) -> ExitCode | None:
        """
        Parse the output of the scheduler.

        :param detailed_job_info: dictionary with the ouput returned by the 
            `Scheduler.get_detailed_job_info` command. This should contain the
            keys `retval`, `stdout`, and `stderr` corresponding to the return
            value, stdout and stderr returned by the accounting command
            executed for a specfic job id.
        :param stdout: Standard output from the scheduler.
        :param stderr: Standard error from the scheduler.
        :return: Raise error otherwise None.
        """

        if detailed_job_info is not None:

            type_check(detailed_job_info, dict)

            try:
                detailed_stdout = json.loads(detailed_job_info['stdout'])
            except KeyError:
                raise ValueError(
                    'the `detailed_job_info` does not contain the '
                    'required key `stdout`.'
                )

            # The format of the detailed job info should be a dictionary.
            type_check(detailed_stdout, dict) 

            #data = dict(zip(fields, attributes))

            #if data['State'] == 'OUT_OF_MEMORY':
            #    return CalcJob.exit_codes.ERROR_SCHEDULER_OUT_OF_MEMORY

            #if data['State'] == 'TO':
            #    return CalcJob.exit_codes.ERROR_SCHEDULER_OUT_OF_WALLTIME

            #if data['State'] == 'F':
            #    return CalcJob.exit_codes.ERROR_SCHEDULER_NODE_FAILURE

        return None