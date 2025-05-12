# AiiDA Flux Scheduler

AiiDA plugin for the LLNL developed Flux scheduler.

A basic understanding of how to interact with AiiDA is required.

At this moment, the plugin works at the same level as the slurm scheduler plugin. AiiDA will use the plugin to submit individual jobs to the flux scheduler and retrieve the jobs when they are done. In the future, the hope is to keep the initial allocation open and have AiiDA interact with the instance through the `flux proxy` command to submit additional jobs in the workchain and reduce wait time between steps in the workflow.

# Basic installation

To get the latest developments and features it is recommended to check the 
git repository and install from source.

```bash
git clone https://github.com/LLNL/aiida-flux-scheduler.git
cd aiida-flux-scheduler
pip install -e .
```

# Release information

`LLNL-CODE-2005941`

AiiDA-Flux-Scheduler is provided under a standard MIT license.