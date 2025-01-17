import os
import time
import logging
import subprocess
import re
from abc import ABC, abstractmethod
from cgatcore.pipeline.cluster import (
    DRMAACluster, SGECluster, 
    SlurmCluster, TorqueCluster)
from cgatcore.pipeline.base_executor import BaseExecutor
import tempfile


class ClusterExecutorBase(ABC):
    """Abstract base class for cluster execution strategies."""
    
    @abstractmethod
    def submit_and_monitor(self, job_script, job_args, job_name):
        """Submit and monitor a cluster job."""
        pass


class DRMAAExecutorStrategy(ClusterExecutorBase):
    """DRMAA-based execution strategy using existing cluster.py functionality."""
    
    def __init__(self, session, queue_manager_cls, queue=None, **kwargs):
        self.session = session
        self.queue = queue
        self.cluster_manager = queue_manager_cls(session, **kwargs)
        self.logger = logging.getLogger(__name__)
        
    def submit_and_monitor(self, job_script, job_args, job_name):
        """Use existing DRMAA cluster implementation."""
        try:
            jt = self.session.createJobTemplate()
            jt.workingDirectory = os.path.dirname(job_script)
            jt.jobName = job_name
            jt.remoteCommand = job_script
            
            # Set queue if specified
            if self.queue:
                jt.nativeSpecification = f"-p {self.queue}"
            
            # Set memory and threads if specified
            if job_args.get('job_memory'):
                mem_spec = f"--mem={job_args['job_memory']}"
                if jt.nativeSpecification:
                    jt.nativeSpecification += f" {mem_spec}"
                else:
                    jt.nativeSpecification = mem_spec
                    
            if job_args.get('job_threads', 1) > 1:
                thread_spec = f"--cpus-per-task={job_args['job_threads']}"
                if jt.nativeSpecification:
                    jt.nativeSpecification += f" {thread_spec}"
                else:
                    jt.nativeSpecification = thread_spec
            
            # Submit the job
            jobid = self.session.runJob(jt)
            self.logger.info(f"Submitted job {jobid} to queue {self.queue}")
            
            # Monitor job status
            status = self._monitor_job(jobid)
            
            # Get resource usage
            if status == 'COMPLETED':
                resource_usage = self._get_resource_usage(jobid)
            else:
                resource_usage = {}
                
            # Clean up job template
            self.session.deleteJobTemplate(jt)
            
            return {
                'jobId': jobid,
                'status': status,
                'resourceUsage': resource_usage
            }
            
        except Exception as e:
            self.logger.error(f"DRMAA job submission failed: {str(e)}")
            raise

    def _monitor_job(self, jobid):
        """Monitor job status."""
        while True:
            retval = self.session.wait(jobid, self.session.TIMEOUT_WAIT_FOREVER)
            if retval.done:
                return retval.exitStatus

    def _get_resource_usage(self, jobid):
        """Get resource usage."""
        retval = self.session.wait(jobid, self.session.TIMEOUT_WAIT_FOREVER)
        return retval.resourceUsage if hasattr(retval, 'resourceUsage') else {}


class SubprocessExecutorStrategy(ClusterExecutorBase):
    """Subprocess-based execution strategy."""
    
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.queue = kwargs.get('queue')

    def submit_and_monitor(self, job_script, job_args, job_name):
        """Submit and monitor a job using subprocess commands."""
        # Build and execute submit command
        cmd = self._build_submit_cmd(job_script, job_args, job_name)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Job submission failed: {result.stderr}")

        # Parse job ID and monitor
        job_id = self._parse_job_id(result.stdout)
        status = self._monitor_job(job_id, job_script)

        # Get resource usage if job completed
        resource_usage = {}
        if status == "COMPLETED":
            resource_usage = self._get_resource_usage(job_id)

        return {
            'jobId': job_id,
            'status': status,
            'resourceUsage': resource_usage
        }

    @abstractmethod
    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build cluster-specific submit command."""
        pass

    @abstractmethod
    def _parse_job_id(self, submit_output):
        """Parse job ID from submission output."""
        pass

    @abstractmethod
    def _monitor_job(self, job_id, job_script):
        """Monitor job and return results."""
        pass

    @abstractmethod
    def _get_resource_usage(self, job_id):
        """Get resource usage information."""
        pass


class BaseExecutor(ABC):
    """Base class for all executors."""

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.task_name = "base_task"
        self.default_total_time = 0
        self.queue = kwargs.get('queue')

    def run(self, statement_list, **kwargs):
        """Execute a list of statements.

        Args:
            statement_list (list): List of commands to execute
            **kwargs: Additional execution options

        Returns:
            list: List of benchmark data for each job
        """
        if isinstance(statement_list, str):
            statement_list = [statement_list]

        benchmark_data = []
        for statement in statement_list:
            # Create temporary job script
            job_script = self.build_job_script(statement)

            try:
                # Set up job arguments
                job_args = {
                    'queue': self.queue,
                    'job_name': self.task_name,
                    'output_path': f"{job_script}.out",
                    'error_path': f"{job_script}.err"
                }

                # Submit and monitor job
                result = self.executor.submit_and_monitor(
                    job_script,
                    job_args,
                    self.task_name
                )

                # Collect benchmark data
                benchmark = {
                    'task': self.task_name,
                    'total_t': self.default_total_time,
                    'job_id': result.get('jobId'),
                    'status': result.get('status'),
                    'resources': result.get('resourceUsage', {})
                }
                benchmark_data.append(benchmark)

            finally:
                # Clean up temporary files
                try:
                    os.unlink(job_script)
                    for ext in ['.out', '.err']:
                        if os.path.exists(job_script + ext):
                            os.unlink(job_script + ext)
                except OSError:
                    pass

        return benchmark_data

    def build_job_script(self, statement):
        """Build a job script for execution.

        Args:
            statement (str): The command to execute

        Returns:
            str: Path to the job script
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as job_script:
            job_script.write(statement)
            return job_script.name


class SGEExecutor(BaseExecutor):
    """SGE executor supporting both DRMAA and subprocess approaches."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "sge_task"
        self.default_total_time = 8
        self.queue = kwargs.get('queue')  # Store queue for later use

        # Always try DRMAA first
        try:
            import drmaa
            session = drmaa.Session()
            session.initialize()
            # Don't pass queue to DRMAACluster initialization
            cluster_kwargs = {k: v for k, v in kwargs.items() if k != 'queue'}
            self.executor = DRMAAExecutorStrategy(
                session=session,
                queue_manager_cls=SGECluster,
                **cluster_kwargs)
            self.logger.info("Using DRMAA-based execution")
        except (ImportError, Exception) as e:
            self.logger.warning(f"DRMAA initialization failed: {e}")
            self.logger.warning("Falling back to subprocess-based execution")
            self.executor = SubprocessSGEStrategy(**kwargs)

    def run(self, statement_list, **kwargs):
        """Execute a list of statements.

        Args:
            statement_list (list): List of commands to execute
            **kwargs: Additional execution options

        Returns:
            list: List of benchmark data for each job
        """
        if isinstance(statement_list, str):
            statement_list = [statement_list]

        benchmark_data = []
        for statement in statement_list:
            # Create job script
            job_script = self.build_job_script(statement)

            try:
                # Set up job arguments
                job_args = {
                    'queue': self.queue,
                    'job_name': self.task_name,
                    'output_path': f"{job_script}.out",
                    'error_path': f"{job_script}.err"
                }

                # Submit and monitor job
                result = self.executor.submit_and_monitor(
                    job_script,
                    job_args,
                    self.task_name
                )

                # Collect benchmark data
                benchmark = {
                    'task': self.task_name,
                    'total_t': self.default_total_time,
                    'job_id': result.get('jobId'),
                    'status': result.get('status'),
                    'resources': result.get('resourceUsage', {})
                }
                benchmark_data.append(benchmark)

            finally:
                # Clean up temporary files
                try:
                    os.unlink(job_script)
                    for ext in ['.out', '.err']:
                        if os.path.exists(job_script + ext):
                            os.unlink(job_script + ext)
                except OSError:
                    pass

        return benchmark_data


class SubprocessSGEStrategy(SubprocessExecutorStrategy):
    """Subprocess-based execution strategy for SGE."""

    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build SGE-specific submit command."""
        cmd = ["qsub", "-N", job_name]
        if "queue" in job_args:
            cmd.extend(["-q", job_args["queue"]])
        if "job_memory" in job_args:
            cmd.extend(["-l", f"h_vmem={job_args['job_memory']}"])
        if "job_threads" in job_args:
            cmd.extend(["-pe", "smp", str(job_args["job_threads"])])
        if "output_path" in job_args:
            cmd.extend(["-o", job_args["output_path"]])
        if "error_path" in job_args:
            cmd.extend(["-e", job_args["error_path"]])
        cmd.append(job_script)
        return " ".join(cmd)

    def _parse_job_id(self, submit_output):
        """Parse job ID from SGE submission output."""
        # Adjust regex to match the mock output format
        match = re.search(r"(\d+)", submit_output)
        if match:
            return match.group(1)
        raise ValueError(f"Could not parse job ID from: {submit_output}")

    def _monitor_job(self, job_id, job_script):
        """Monitor SGE job and return results."""
        while True:
            # Check job status
            cmd = f"qstat -j {job_id}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                # Job not found in qstat, check final status with qacct
                cmd = f"qacct -j {job_id}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if "exit_status    0" in result.stdout:
                    return "COMPLETED"
                elif "exit_status    1" in result.stdout:
                    raise RuntimeError(f"Job {job_id} failed with exit status: 1")
                else:
                    raise RuntimeError(f"Job {job_id} failed")

            time.sleep(10)  # Wait before checking again

    def _get_resource_usage(self, job_id):
        """Get resource usage from qacct."""
        cmd = f"qacct -j {job_id}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            # Parse resource usage from qacct output
            usage = {}
            for line in result.stdout.split("\n"):
                if "maxvmem" in line:
                    usage["memory"] = line.split()[1] if len(line.split()) > 1 else "N/A"
                elif "cpu" in line:
                    usage["cpu"] = line.split()[1] if len(line.split()) > 1 else "N/A"
            return usage
        return {}


class SlurmExecutor(BaseExecutor):
    """SLURM executor supporting both DRMAA and subprocess approaches."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.task_name = "slurm_task"
        self.default_total_time = 10
        self.queue = kwargs.get('queue')

        # Initialize as subprocess strategy by default
        self.executor = SubprocessSlurmStrategy(**kwargs)
        self.logger.info("Using subprocess-based execution")

    def __del__(self):
        """Ensure DRMAA session is cleaned up."""
        try:
            if hasattr(self, 'session'):
                self.session.exit()
        except Exception as e:
            # Log but don't raise in destructor
            self.logger.warning(f"Error during cleanup in destructor: {str(e)}")

    def build_job_script(self, statement):
        """Build a job script for execution.
        
        Args:
            statement (str): The command to execute
            
        Returns:
            str: Path to the job script
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as job_script:
            job_script.write(statement)
            return job_script.name

    def run(self, statement_list, **kwargs):
        """Execute a list of statements.
        
        Args:
            statement_list (list): List of commands to execute
            **kwargs: Additional execution options
            
        Returns:
            list: List of benchmark data for each job
        """
        if isinstance(statement_list, str):
            statement_list = [statement_list]
            
        benchmark_data = []
        for statement in statement_list:
            # Create job script
            job_script = self.build_job_script(statement)
            
            try:
                # Set up job arguments
                job_args = {
                    'queue': self.queue,
                    'job_name': self.task_name,
                    'output_path': f"{job_script}.out",
                    'error_path': f"{job_script}.err"
                }

                # Submit and monitor job
                result = self.executor.submit_and_monitor(
                    job_script,
                    job_args,
                    self.task_name
                )

                # Collect benchmark data
                benchmark = {
                    'task': self.task_name,
                    'total_t': self.default_total_time,
                    'job_id': result.get('jobId'),
                    'status': result.get('status'),
                    'resources': result.get('resourceUsage', {})
                }
                benchmark_data.append(benchmark)

            finally:
                # Clean up temporary files
                try:
                    os.unlink(job_script)
                    for ext in ['.out', '.err']:
                        if os.path.exists(job_script + ext):
                            os.unlink(job_script + ext)
                except OSError:
                    pass

        return benchmark_data


class SubprocessSlurmStrategy(SubprocessExecutorStrategy):
    """Subprocess-based execution strategy for SLURM."""

    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build SLURM-specific submit command."""
        cmd = ["sbatch", "--parsable", "-J", job_name]
        if job_args.get('queue'):
            cmd.extend(["-p", job_args["queue"]])
        if job_args.get('job_memory'):
            cmd.extend(["--mem", job_args["job_memory"]])
        if job_args.get('job_threads'):
            cmd.extend(["-c", str(job_args["job_threads"])])
        if job_args.get('output_path'):
            cmd.extend(["-o", job_args["output_path"]])
        if job_args.get('error_path'):
            cmd.extend(["-e", job_args["error_path"]])
        cmd.append(job_script)
        return " ".join(cmd)

    def _parse_job_id(self, submit_output):
        """Parse job ID from SLURM submission output."""
        return submit_output.strip()

    def submit_and_monitor(self, job_script, job_args, job_name):
        """Submit and monitor a job using subprocess commands."""
        # Build and execute submit command
        cmd = self._build_submit_cmd(job_script, job_args, job_name)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Job submission failed: {result.stderr}")

        # Parse job ID and monitor
        job_id = self._parse_job_id(result.stdout)
        status = self._monitor_job(job_id, job_script)

        # Get resource usage if job completed
        resource_usage = {}
        if status == "COMPLETED":
            resource_usage = self._get_resource_usage(job_id)

        return {
            'jobId': job_id,
            'status': status,
            'resourceUsage': resource_usage
        }

    def _monitor_job(self, job_id, job_script):
        """Monitor SLURM job and return results."""
        error_states = {"FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "CANCELLED"}
        success_states = {"COMPLETED"}
        
        while True:
            # Check job status using squeue
            cmd = f"squeue -h -j {job_id} -o %T"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            current_state = result.stdout.strip().upper()
            
            if result.returncode != 0:
                # Job not found in squeue, check final status with sacct
                cmd = f"sacct -j {job_id} -o State -n"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                final_state = result.stdout.strip().upper()
                
                if final_state in success_states:
                    return "COMPLETED"
                else:
                    raise RuntimeError(f"Job {job_id} failed with status: {final_state}")
            else:
                if current_state in error_states:
                    raise RuntimeError(f"Job {job_id} failed with status: {current_state}")
                elif current_state not in success_states:
                    time.sleep(10)  # Wait before checking again

    def _get_resource_usage(self, job_id):
        """Get resource usage from sacct."""
        cmd = f"sacct -j {job_id} -o JobID,State,Elapsed,MaxRSS,MaxVMSize,MaxDiskRead,MaxDiskWrite -P"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            # Parse resource usage from sacct output
            fields = result.stdout.strip().split("|")
            return {
                "elapsed": fields[2],
                "max_rss": fields[3],
                "max_vmem": fields[4],
                "max_disk_read": fields[5],
                "max_disk_write": fields[6]
            }
        return {}


class SubprocessTorqueStrategy(SubprocessExecutorStrategy):
    """Subprocess-based execution strategy for Torque."""

    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build Torque-specific submit command."""
        cmd = ["qsub", "-N", job_name]
        if "queue" in job_args:
            cmd.extend(["-q", job_args["queue"]])
        if "job_memory" in job_args:
            cmd.extend(["-l", f"mem={job_args['job_memory']}"])
        if "job_threads" in job_args:
            cmd.extend(["-l", f"nodes=1:ppn={job_args['job_threads']}"])
        if "output_path" in job_args:
            cmd.extend(["-o", job_args["output_path"]])
        if "error_path" in job_args:
            cmd.extend(["-e", job_args["error_path"]])
        cmd.append(job_script)
        return " ".join(cmd)

    def _parse_job_id(self, submit_output):
        """Parse job ID from Torque submission output."""
        return submit_output.strip()

    def _monitor_job(self, job_id, job_script):
        """Monitor Torque job and return results."""
        while True:
            # Check job status using qstat -f
            cmd = f"qstat -f {job_id}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                # Job not found in qstat, check final status with tracejob
                cmd = f"tracejob {job_id}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if "Exit_status=0" in result.stdout:
                    return "COMPLETED"
                elif "Exit_status=1" in result.stdout:
                    raise RuntimeError(f"Job {job_id} failed with exit status: 1")
                else:
                    raise RuntimeError(f"Job {job_id} failed")

            time.sleep(10)  # Wait before checking again

    def _get_resource_usage(self, job_id):
        """Get resource usage from tracejob."""
        cmd = f"tracejob {job_id}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            usage = {}
            for line in result.stdout.split("\n"):
                if "resources_used.mem" in line:
                    usage["memory"] = line.split("=")[1]
                elif "resources_used.cput" in line:
                    usage["cpu"] = line.split("=")[1]
            return usage
        return {}


class TorqueExecutor(BaseExecutor):
    """Torque executor supporting both DRMAA and subprocess approaches."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "torque_task"
        self.default_total_time = 7
        self.queue = kwargs.get('queue')  # Store queue for later use

        # Always try DRMAA first
        try:
            import drmaa
            session = drmaa.Session()
            session.initialize()
            # Don't pass queue to DRMAACluster initialization
            cluster_kwargs = {k: v for k, v in kwargs.items() if k != 'queue'}
            self.executor = DRMAAExecutorStrategy(
                session=session,
                queue_manager_cls=TorqueCluster,
                **cluster_kwargs)
            self.logger.info("Using DRMAA-based execution")
        except (ImportError, Exception) as e:
            self.logger.warning(f"DRMAA initialization failed: {e}")
            self.logger.warning("Falling back to subprocess-based execution")
            self.executor = SubprocessTorqueStrategy(**kwargs)

    def run(self, statement_list, **kwargs):
        """Execute a list of statements.

        Args:
            statement_list (list): List of commands to execute
            **kwargs: Additional execution options

        Returns:
            list: List of benchmark data for each job
        """
        if isinstance(statement_list, str):
            statement_list = [statement_list]

        benchmark_data = []
        for statement in statement_list:
            # Create job script
            job_script = self.build_job_script(statement)

            try:
                # Set up job arguments
                job_args = {
                    'queue': self.queue,
                    'job_name': self.task_name,
                    'output_path': f"{job_script}.out",
                    'error_path': f"{job_script}.err"
                }

                # Submit and monitor job
                result = self.executor.submit_and_monitor(
                    job_script,
                    job_args,
                    self.task_name
                )

                # Collect benchmark data
                benchmark = {
                    'task': self.task_name,
                    'total_t': self.default_total_time,
                    'job_id': result.get('jobId'),
                    'status': result.get('status'),
                    'resources': result.get('resourceUsage', {})
                }
                benchmark_data.append(benchmark)

            finally:
                # Clean up temporary files
                try:
                    os.unlink(job_script)
                    for ext in ['.out', '.err']:
                        if os.path.exists(job_script + ext):
                            os.unlink(job_script + ext)
                except OSError:
                    pass

        return benchmark_data


class LocalExecutor(BaseExecutor):
    """Executor for running jobs locally."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "local_task"
        self.default_total_time = 5
        self.executor = self  # Local executor is its own executor

    def build_job_script(self, statement):
        """Build a job script for execution.

        Args:
            statement (str): The command to execute

        Returns:
            str: Path to the job script
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as job_script:
            job_script.write(statement)
            return job_script.name

    def submit_and_monitor(self, job_script, job_args, job_name):
        """Submit and monitor a job using subprocess commands."""
        try:
            # Execute the job script
            result = subprocess.run(['bash', job_script], capture_output=True, text=True)

            if result.returncode == 0:
                return {
                    'jobId': None,
                    'status': 'COMPLETED',
                    'resourceUsage': {}
                }
            else:
                raise RuntimeError(f"Job failed with exit code {result.returncode}: {result.stderr}")

        except subprocess.SubprocessError as e:
            raise RuntimeError(f"Error executing command: {str(e)}")
