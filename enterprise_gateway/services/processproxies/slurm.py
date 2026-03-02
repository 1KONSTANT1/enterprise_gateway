from __future__ import annotations

import asyncio
import json
import os
from socket import gethostbyname
from subprocess import STDOUT
from typing import Any

from ..kernels.remotemanager import RemoteKernelManager
from .processproxy import BaseProcessProxyABC, RemoteProcessProxy
from traitlets import List, Unicode, Integer, Bool
from traitlets.config import Configurable

poll_interval = float(os.getenv("EG_POLL_INTERVAL", "0.5"))
kernel_log_dir = os.getenv(
    "EG_KERNEL_LOG_DIR", "/tmp"  # noqa
)  # would prefer /var/log, but its only writable by root


class SlurmProxyConfig(Configurable):
    """Configuration for Slurm Process Proxy.
    
    This class provides configuration options for launching kernels
    on Slurm-managed clusters.
    """
    
    
    master_nodes = List(Unicode(), config=True,
                       help="""List of master nodes for Slurm.
These nodes will be used as the primary contact points
for Slurm job submission.
                       """)
    
    nfs_server = Unicode('0.0.0.0', config=True,
                             help="""  The IP address or hostname of the NFS server that contains user home directories
  or shared data volumes.
  This server will be mounted on Slurm worker nodes to provide access to user files,
  kernelspecs, and shared data across the cluster. The NFS export should be properly
  configured to allow access from all worker nodes in the Slurm partition.
  
  Example: '192.168.1.100' or 'nfs-server.cluster.local'

                             """)
    
    nfs_server_dest = Unicode('/data', config=True,
                             help="""NFS server user's directory path destination
The remote path on the NFS server that contains user directories or shared data.
This path corresponds to the NFS export that will be mounted on worker nodes.
 
This is typically the exported path on the NFS server, such as '/home' or '/exports/users'.
The kernel processes will access user data through this mount point.
  
Example: '/exports/users' or '/shared_data'
                             """)
    
    worker_mount_dir = Unicode('/mnt/users', config=True,
                             help="""The local mount point on each Slurm worker node where the NFS share will be mounted.
  
This directory must exist and be accessible on all worker nodes. The NFS export from
the server will be mounted here, making user files available to kernel processes.
Ensure this directory has appropriate permissions for the users running kernels.
  
Example: '/mnt/nfs/users' or '/shared'
                             """)
    enroot_image_path = Unicode('/mnt', config=True,
                             help=""" The file system path to Enroot container images stored on the Slurm worker nodes.
                             """)

class SlurmProcessProxy(RemoteProcessProxy):
    """
    Manages the lifecycle of kernels running on slurm cluster.
    """

    default_batch_job = """#!/bin/bash
{SBATCH_JOB_FLAGS}


{COMMAND}    
"""

    def __init__(self, kernel_manager: RemoteKernelManager, proxy_config: dict):
        """Initialize the proxy."""
        super().__init__(kernel_manager, proxy_config)
        self.kernel_log = None
        self.local_stdout = None
        
        self.slurm_sbatch_flags = proxy_config.get("sbatch_flags")

        #get slurm config from JEG config
        slurm_config = kernel_manager.config.get('SlurmProxyConfig', {})
        
        #get envs from config
        if slurm_config:
            self.slurm_master_hosts = slurm_config.get('master_nodes', [])
            self.nfs_server = slurm_config.get('nfs_server', "0.0.0.0")
            self.nfs_server_dest = slurm_config.get('nfs_server_dest', "/data")
            self.worker_mount_dir = slurm_config.get('worker_mount_dir', "/mnt/users")
            self.enroot_image_path = slurm_config.get('enroot_image_path', "/mnt")
            

    async def launch_process(
        self, kernel_cmd: str, **kwargs: dict[str, Any] | None
    ) -> SlurmProcessProxy:
        """
        Launches a kernel process on a SLURM cluster.
        """
        
        env_dict = kwargs.get("env")

        self.slurm_kernel_username = env_dict.get("KERNEL_USERNAME")


        await super().launch_process(kernel_cmd, **kwargs)


        self.assigned_host = self.slurm_master_hosts[0]
        self.ip = gethostbyname(self.assigned_host)  # convert to ip if host is provided
        self.assigned_ip = self.ip

        try:
            slurm_job = self._launch_remote_process(kernel_cmd, **kwargs)
            self.job_id= int(slurm_job)
        except Exception as e:
            error_message = "Failure occurred starting kernel on '{}'.  Returned result: {}".format(
                self.ip, e
            )
            self.log_and_raise(http_status_code=500, reason=error_message)

        self.log.info(
            "Kernel launched on '{}', slurm job id: {}, ID: {}, Log file: {}:{}, Command: '{}'.  ".format(
                self.assigned_host,
                self.job_id,
                self.kernel_id,
                self.assigned_host,
                self.kernel_log,
                kernel_cmd,
            )
        )
        await self.confirm_remote_startup()
        return self

    def _launch_remote_process(self, kernel_cmd: str, **kwargs: dict[str, Any] | None) -> str:
        """
        Launch the kernel as indicated by the argv stanza in the kernelspec.  Note that this method
        will bypass use of ssh if the remote host is also the local machine.
        """

        inputs = self._build_startup_command(kernel_cmd, **kwargs)
        self.log.debug(f"Invoking cmd: '{inputs}' on host: {self.assigned_host}")
        slurm_job = "bad_job"  # purposely initialize to bad int value

        result = self.rsh(self.ip, f"mkdir {self.worker_mount_dir}/{self.slurm_kernel_username} && sudo mount -t nfs {self.nfs_server}:{self.nfs_server_dest}/{self.slurm_kernel_username} {self.worker_mount_dir}/{self.slurm_kernel_username}; enroot create --name kernel_{self.slurm_kernel_username} {self.enroot_image_path};sbatch --parsable" , inputs)
        for line in result:
            slurm_job = line.strip()

        return slurm_job
    
    def poll(self) -> Any | None:
        """
        Determines if process proxy is still alive.
        """

        return self.send_signal(0)
    

    def _build_startup_command(self, argv_cmd: str, **kwargs: dict[str, Any] | None) -> str:
        """
        Builds the command to invoke by concatenating envs from kernelspec followed by the kernel argvs.

        We also force nohup, redirection to a file and place in background, then follow with an echo
        for the background pid.

        Note: We optimize for the local case and just return the existing command.
        """

        # Optimized case needs to also redirect the kernel output, so unconditionally compose kernel_log
        env_dict = kwargs["env"]
        kid = env_dict.get("KERNEL_ID")

        self.kernel_log = os.path.join(kernel_log_dir, f"kernel-{kid}.log")

        if BaseProcessProxyABC.ip_is_local(self.ip):  # We're local so just use what we're given
            cmd = argv_cmd
        else:  # Add additional envs, including those in kernelspec
            cmd = ""

            for key, value in env_dict.items():
                cmd += "export {}={};".format(key, json.dumps(value).replace("'", "''"))

            for key, value in self.kernel_manager.kernel_spec.env.items():
                cmd += "export {}={};".format(key, json.dumps(value).replace("'", "''"))

            for arg in argv_cmd:
                cmd += f" {arg}"
                
            #kernel runs in enroot container and is being mounted to nfs
            cmd = f"enroot start --mount {self.worker_mount_dir}/{self.slurm_kernel_username}:/work kernel_{self.slurm_kernel_username} /bin/bash -c \"" + cmd +"\""

            cmd += f" >> {self.kernel_log} 2>&1"  # return the process id    echo $!
        
        #set sbatch flags for script
        slurm_job_flags = ''
        for parameter, value in self.slurm_sbatch_flags.items():
            slurm_job_flags += f'#SBATCH --{parameter}={value}\n'
        slurm_job_flags += f'#SBATCH --job-name={self.slurm_kernel_username}-kernel\n'
        slurm_job_flags += f'#SBATCH --output=jupyter-kernel-%j.log\n'
        slurm_job_flags += f'#SBATCH --error=error-jupyter-kernel-%j.log\n'
        
        inputs = self.default_batch_job.format(COMMAND = cmd, SBATCH_JOB_FLAGS=slurm_job_flags)
        

        return inputs

    async def confirm_remote_startup(self) -> None:
        """Confirms the remote kernel has started by obtaining connection information from the remote host."""
        self.start_time = RemoteProcessProxy.get_current_time()
        i = 0
        ready_to_connect = False  # we're ready to connect when we have a connection file to use
        while not ready_to_connect:
            i += 1
            await self.handle_timeout()

            self.log.debug(
                "{}: Waiting to connect.  Host: '{}', KernelID: '{}'".format(
                    i, self.assigned_host, self.kernel_id
                )
            )

            if self.assigned_host:
                ready_to_connect = await self.receive_connection_info()

    async def handle_timeout(self) -> None:
        """Checks to see if the kernel launch timeout has been exceeded while awaiting connection info."""
        await asyncio.sleep(poll_interval)
        time_interval = RemoteProcessProxy.get_time_diff(
            self.start_time, RemoteProcessProxy.get_current_time()
        )
        try:
            job_state = self.rsh(self.ip, f"squeue -h -j {self.job_id} -o \"%T\" ")[0].strip()
        except Exception as e:
            self.log_and_raise(
                http_status_code=500,
                reason=f"Could not get status for the job {self.job_id}. "
                f"Error was: {str(e)}",
            )
        
        self.log.debug(f"Slurm job {self.job_id} is in {job_state} state")

        if job_state not in ["PENDING", "RUNNING"] and (time_interval > self.kernel_launch_timeout):
            reason = (
                "Waited too long ({}s) to get connection file.  Check Enterprise Gateway log and kernel "
                "log ({}:{}) for more information.".format(
                    self.kernel_launch_timeout, self.assigned_host, self.kernel_log
                )
            )
            timeout_message = f"KernelID: '{self.kernel_id}' launch timeout due to: {reason}"
            await asyncio.get_event_loop().run_in_executor(None, self.kill)
            self.log_and_raise(http_status_code=500, reason=timeout_message)
    
    def kill(self) -> bool|None:
        """Kill the proxy."""
        try:
            self.rsh(self.ip, f"scancel {self.job_id};enroot remove --force kernel_{self.slurm_kernel_username}")
        except Exception as e:
            self.log.warning(f"Error cancelling slurm job {self.job_id} and removing enroot image for user {self.slurm_kernel_username}. Error: {str(e)}")
            return False
            

    def cleanup(self) -> None:
        """Clean up the proxy."""
        self.kill()
        
        if self.local_stdout:
            self.local_stdout.close()
            self.local_stdout = None
        super().cleanup()
