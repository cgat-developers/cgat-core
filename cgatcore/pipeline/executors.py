import os
import time
import logging
import subprocess
from abc import ABC, abstractmethod
from cgatcore.pipeline.cluster import (
    DRMAACluster, SGECluster, 
    SlurmCluster, TorqueCluster)
from cgatcore.pipeline.base_executor import BaseExecutor


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
            
            # Wait for job completion
            retval = self.session.wait(jobid, self.session.TIMEOUT_WAIT_FOREVER)
            self.logger.info(f"Job {jobid} completed with status {retval.exitStatus}")
            
            # Clean up
            self.session.deleteJobTemplate(jt)
            
            # Return job info
            return {
                'exit_status': retval.exitStatus,
                'resourceUsage': retval.resourceUsage if hasattr(retval, 'resourceUsage') else {},
                'job_id': jobid
            }
            
        except Exception as e:
            self.logger.error(f"DRMAA job submission failed: {str(e)}")
            raise


class SubprocessExecutorStrategy(ClusterExecutorBase):
    """Subprocess-based execution strategy."""
    
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        
    def submit_and_monitor(self, job_script, job_args, job_name):
        """Implement direct subprocess-based submission."""
        # Add output/error paths if not provided
        if isinstance(job_script, tuple):
            script_content, script_path = job_script
        else:
            script_path = job_script
            
        if 'output_path' not in job_args:
            job_args['output_path'] = f"{script_path}.o"
        if 'error_path' not in job_args:
            job_args['error_path'] = f"{script_path}.e"
            
        # Submit job
        cmd = self._build_submit_cmd(script_path, job_args, job_name)
        process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if process.returncode != 0:
            self.logger.error(f"Job submission failed: {process.stderr}")
            raise RuntimeError(f"Job submission failed: {process.stderr}")
            
        job_id = self._parse_job_id(process.stdout)
        return self._monitor_job(job_id, script_path)
    
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

    def run(self, statement_list):
        """Execute statements on the cluster."""
        benchmark_data = []

        for statement in statement_list:
            job_script = self.build_job_script(statement)
            job_args = {
                'job_memory': self.job_memory,
                'job_threads': self.job_threads
            }
            retval = self.executor.submit_and_monitor(
                job_script, job_args, self.task_name)
            # Handle both list and single Result objects
            if isinstance(retval, (list, tuple)):
                benchmark_data.extend(retval)
            else:
                benchmark_data.append(retval)

        return self.collect_benchmark_data(statement_list, benchmark_data)


class SubprocessSGEStrategy(SubprocessExecutorStrategy):
    """Subprocess-based execution strategy for SGE."""
    
    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build SGE-specific submit command."""
        return (
            f"qsub -N {job_name} -cwd "
            f"-o {job_args['output_path']} "
            f"-e {job_args['error_path']} "
            f"{job_script}")
    
    def _parse_job_id(self, submit_output):
        """Parse job ID from SGE submission output."""
        job_id = submit_output.strip()
        try:
            return str(int(job_id))  # Verify it's a valid number
        except ValueError:
            # Try to extract just the number if we got a full message
            try:
                return str(int(job_id.split()[-1]))
            except (IndexError, ValueError):
                raise RuntimeError(f"Could not parse job ID from qsub output: {job_id}")
    
    def _monitor_job(self, job_id, job_script):
        """Monitor SGE job and return results."""
        while True:
            cmd = f"qstat -j {job_id}"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if process.returncode != 0:
                # Job not found in qstat could mean it's completed
                # Use qacct to get final status
                cmd = f"qacct -j {job_id}"
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if "exit_status" in process.stdout:
                    exit_status = process.stdout.split("exit_status")[1].split()[0]
                    if exit_status == "0":
                        self.logger.info(f"Job {job_id} completed successfully")
                        break
                    else:
                        self.logger.error(f"Job {job_id} failed with exit status: {exit_status}")
                        raise RuntimeError(f"Job {job_id} failed with exit status: {exit_status}")
                
                self.logger.error(f"Failed to get job status: {process.stderr}")
                raise RuntimeError(f"Failed to get job status: {process.stderr}")
            
            time.sleep(10)
            
        # Create a simple result object to match DRMAA interface
        class Result:
            def __init__(self):
                self.resourceUsage = (
                    self._get_resource_usage(job_id))
                
            def _get_resource_usage(self, job_id):
                """Get resource usage from qacct."""
                cmd = (f"qacct -j {job_id} -o format=mem,cpu,io")
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if process.returncode == 0:
                    # Parse and return as dict
                    values = process.stdout.strip().split()
                    return {
                        'max_rss': values[0] if len(values) > 0 else None,
                        'cpu_time': values[1] if len(values) > 1 else None,
                        'io': values[2] if len(values) > 2 else None
                    }
                return {}
                
        return Result()


class SlurmExecutor(BaseExecutor):
    """SLURM executor supporting both DRMAA and subprocess approaches."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "slurm_task"
        self.default_total_time = 10
        self.queue = kwargs.get('queue')
        
        # Always try DRMAA first
        try:
            import drmaa
            from cgatcore.pipeline.execution import start_session, close_session
            
            # Test if we can initialize a session
            test_session = drmaa.Session()
            test_session.initialize()
            test_session.exit()
            
            # If we got here, DRMAA is working, so use the global session
            session = start_session()
            if session is None:
                self.logger.warning("Failed to initialize DRMAA session")
                raise Exception("Failed to initialize DRMAA session")
                
            # Don't pass queue to DRMAACluster initialization
            cluster_kwargs = {k: v for k, v in kwargs.items() if k != 'queue'}
            self.executor = DRMAAExecutorStrategy(
                session=session,
                queue_manager_cls=SlurmCluster,
                queue=self.queue,  # Pass queue here
                **cluster_kwargs)
            self.logger.info("Using DRMAA-based execution")
        except ImportError as e:
            self.logger.warning(f"DRMAA Python package not available: {str(e)}")
            self.logger.info("Using subprocess-based execution")
            self.executor = SubprocessSlurmStrategy(**kwargs)
        except RuntimeError as e:
            if "DRMAA_LIBRARY_PATH" in str(e):
                self.logger.warning(
                    "DRMAA library not found. Please ensure SLURM's DRMAA library "
                    "is installed and DRMAA_LIBRARY_PATH environment variable is set. "
                    "Common locations are:\n"
                    "  - /usr/lib/libdrmaa.so\n"
                    "  - /usr/local/lib/libdrmaa.so\n"
                    "  - /usr/lib64/libdrmaa.so\n"
                    "Using subprocess-based execution for now."
                )
            else:
                self.logger.warning(f"DRMAA runtime error: {str(e)}")
            self.logger.info("Using subprocess-based execution")
            self.executor = SubprocessSlurmStrategy(**kwargs)
        except Exception as e:
            self.logger.warning(f"DRMAA initialization failed: {str(e)}")
            self.logger.info("Using subprocess-based execution")
            self.executor = SubprocessSlurmStrategy(**kwargs)
    
    def __del__(self):
        """Ensure DRMAA session is cleaned up."""
        if hasattr(self, 'executor') and isinstance(self.executor, DRMAAExecutorStrategy):
            try:
                from cgatcore.pipeline.execution import close_session
                close_session()
            except:
                pass

    def run(self, statement_list):
        """Execute statements on the cluster."""
        benchmark_data = []

        for statement in statement_list:
            job_script = self.build_job_script(statement)
            job_args = {
                'job_memory': self.job_memory,
                'job_threads': self.job_threads
            }
            retval = self.executor.submit_and_monitor(
                job_script, job_args, self.task_name)
            # Handle both list and single Result objects
            if isinstance(retval, (list, tuple)):
                benchmark_data.extend(retval)
            else:
                benchmark_data.append(retval)

        return self.collect_benchmark_data(statement_list, benchmark_data)


class SubprocessSlurmStrategy(SubprocessExecutorStrategy):
    """Subprocess-based execution strategy for SLURM."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.queue = kwargs.get('queue')
        self.options = kwargs.get('options', '')
        self.job_memory = kwargs.get('job_memory', '4G')
        self.job_threads = kwargs.get('job_threads', 1)
    
    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build SLURM-specific submit command."""
        cmd = ["sbatch", "--parsable"]
        
        if self.queue:
            cmd.extend(["--partition", self.queue])
        
        if self.job_memory and self.job_memory != "unlimited":
            cmd.extend(["--mem", str(self.job_memory)])
            
        if self.job_threads and self.job_threads > 1:
            cmd.extend(["--cpus-per-task", str(self.job_threads)])
            
        if self.options:
            cmd.extend(self.options.split())
            
        cmd.extend([
            "--job-name", job_name,
            "--output", job_args['output_path'],
            "--error", job_args['error_path'],
            job_script
        ])
        
        return " ".join(cmd)
    
    def _parse_job_id(self, submit_output):
        """Parse job ID from SLURM submission output."""
        job_id = submit_output.strip()
        try:
            return str(int(job_id))  # Verify it's a valid number
        except ValueError:
            # Try to extract just the number if we got a full message
            try:
                return str(int(job_id.split()[-1]))
            except (IndexError, ValueError):
                raise RuntimeError(f"Could not parse job ID from sbatch output: {job_id}")
    
    def _monitor_job(self, job_id, job_script):
        """Monitor SLURM job and return results."""
        start_time = time.time()
        max_wait_time = 7200  # Maximum time to wait - 2 hours for real jobs
        check_interval = 10  # Time between checks in seconds
        
        # Define job states
        RUNNING_STATES = ["RUNNING", "PENDING", "CONFIGURING", "COMPLETING"]
        COMPLETED_STATES = ["COMPLETED", "COMPLETE", "DONE"]
        FAILED_STATES = ["FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY", 
                        "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED", "SPECIAL_EXIT"]
        
        def get_job_error():
            """Get detailed error information from SLURM."""
            error_info = []
            
            # Try sacct first for detailed state
            cmd = f"sacct -j {job_id} --format=JobID,State,ExitCode,DerivedExitCode,Comment,Reason --noheader --parsable2"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if process.returncode == 0 and process.stdout.strip():
                error_info.append(f"SACCT details: {process.stdout.strip()}")
            
            # Try scontrol for more details
            cmd = f"scontrol show job {job_id}"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if process.returncode == 0:
                for line in process.stdout.split('\n'):
                    if any(x in line for x in ['Reason=', 'ExitCode=', 'Comment=']):
                        error_info.append(line.strip())
            
            # Check job script output/error files
            script_dir = os.path.dirname(job_script)
            for ext in ['.out', '.err']:
                output_file = os.path.join(script_dir, f"slurm-{job_id}{ext}")
                if os.path.exists(output_file):
                    try:
                        with open(output_file, 'r') as f:
                            content = f.read().strip()
                            if content:
                                error_info.append(f"Content of {output_file}:")
                                error_info.append(content)
                    except Exception as e:
                        error_info.append(f"Could not read {output_file}: {str(e)}")
            
            return '\n'.join(error_info)
        
        while True:
            if time.time() - start_time > max_wait_time:
                error_details = get_job_error()
                self.logger.error(f"Job {job_id} exceeded maximum wait time of {max_wait_time} seconds")
                self.logger.error(f"Error details:\n{error_details}")
                raise RuntimeError(f"Job {job_id} exceeded maximum wait time. Details:\n{error_details}")
                
            # First try squeue to check if job is still running
            cmd = f"squeue -j {job_id} -h -o %T"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if process.returncode != 0:
                # Job not in queue anymore, check if it completed
                cmd = f"sacct -j {job_id} --format=State --noheader --parsable2"
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if process.returncode != 0:
                    # If sacct fails, try scontrol as a backup
                    cmd = f"scontrol show job {job_id}"
                    process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    
                    if process.returncode != 0:
                        error_details = get_job_error()
                        self.logger.error(f"Failed to get job status: {process.stderr}")
                        self.logger.error(f"Error details:\n{error_details}")
                        raise RuntimeError(f"Failed to get job status: {process.stderr}\nDetails:\n{error_details}")
                    
                    # Parse scontrol output
                    for line in process.stdout.split('\n'):
                        if "JobState=" in line:
                            status = line.split("JobState=")[1].split()[0].upper()
                            break
                    else:
                        status = "UNKNOWN"
                else:
                    status = process.stdout.strip().split('|')[0].upper()
                
                # Check completion status
                if any(s in status for s in COMPLETED_STATES):
                    self.logger.info(f"Job {job_id} completed successfully")
                    break
                elif any(s in status for s in FAILED_STATES):
                    error_details = get_job_error()
                    self.logger.error(f"Job {job_id} failed with status: {status}")
                    self.logger.error(f"Error details:\n{error_details}")
                    raise RuntimeError(f"Job {job_id} failed with status: {status}\nDetails:\n{error_details}")
                elif status == "UNKNOWN":
                    self.logger.debug(f"Could not determine status for job {job_id}, checking error details")
                    error_details = get_job_error()
                    if error_details:
                        self.logger.error(f"Found error details for unknown status:\n{error_details}")
            else:
                # Job still in queue or running
                status = process.stdout.strip().upper()
                if not status:  # Empty status
                    self.logger.debug(f"Empty status for job {job_id}, checking with scontrol")
                    cmd = f"scontrol show job {job_id}"
                    process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    if process.returncode == 0:
                        for line in process.stdout.split('\n'):
                            if "JobState=" in line:
                                status = line.split("JobState=")[1].split()[0].upper()
                                break
                    
                self.logger.debug(f"Job {job_id} status: {status}")
                
                if status in FAILED_STATES:
                    error_details = get_job_error()
                    self.logger.error(f"Job {job_id} failed with status: {status}")
                    self.logger.error(f"Error details:\n{error_details}")
                    raise RuntimeError(f"Job {job_id} failed with status: {status}\nDetails:\n{error_details}")
                elif status in COMPLETED_STATES:
                    self.logger.info(f"Job {job_id} completed successfully")
                    break
                elif status in RUNNING_STATES or not status:  # Include empty status here
                    self.logger.debug(f"Job {job_id} is {status or 'in unknown state'}")
                else:
                    self.logger.debug(f"Job {job_id} has unknown status: {status}")
                    error_details = get_job_error()
                    if error_details:
                        self.logger.debug(f"Additional status details:\n{error_details}")
            
            time.sleep(check_interval)
            
        # Create a simple result object to match DRMAA interface
        class Result:
            def __init__(self):
                self.exitStatus = 0  # Assume success since we got here
                self.resourceUsage = self._get_resource_usage(job_id)
                
            def _get_resource_usage(self, job_id):
                """Get resource usage from sacct."""
                cmd = (
                    f"sacct -j {job_id} "
                    f"--format=JobID,State,Elapsed,MaxRSS,"
                    f"MaxVMSize,AveRSS,AveVMSize,ExitCode "
                    f"--noheader --parsable2")
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if process.returncode == 0:
                    # Parse and return as dict
                    values = process.stdout.strip().split('|')
                    return {
                        'max_rss': values[3] if len(values) > 3 else '0',
                        'max_vmem': values[4] if len(values) > 4 else '0',
                        'wallclock': values[2] if len(values) > 2 else '0',
                        'cpu': '0',
                        'exitcode': values[7] if len(values) > 7 else '0:0'
                    }
                
                # If sacct fails, try scontrol as backup
                cmd = f"scontrol show job {job_id}"
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if process.returncode == 0:
                    # Parse memory from scontrol output
                    mem = '0'
                    for line in process.stdout.split('\n'):
                        if "MaxRSS=" in line:
                            mem = line.split("MaxRSS=")[1].split()[0]
                            break
                    return {
                        'max_rss': mem,
                        'max_vmem': '0',
                        'wallclock': '0',
                        'cpu': '0',
                        'exitcode': '0:0'
                    }
                
                return {
                    'max_rss': '0',
                    'max_vmem': '0',
                    'wallclock': '0',
                    'cpu': '0',
                    'exitcode': '0:0'
                }
                
        return Result()


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

    def run(self, statement_list):
        """Execute statements on the cluster."""
        benchmark_data = []

        for statement in statement_list:
            job_script = self.build_job_script(statement)
            job_args = {
                'job_memory': self.job_memory,
                'job_threads': self.job_threads
            }
            retval = self.executor.submit_and_monitor(
                job_script, job_args, self.task_name)
            # Handle both list and single Result objects
            if isinstance(retval, (list, tuple)):
                benchmark_data.extend(retval)
            else:
                benchmark_data.append(retval)

        return self.collect_benchmark_data(statement_list, benchmark_data)


class SubprocessTorqueStrategy(SubprocessExecutorStrategy):
    """Subprocess-based execution strategy for Torque."""
    
    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build Torque-specific submit command."""
        return (
            f"qsub -N {job_name} "
            f"-o {job_args['output_path']} "
            f"-e {job_args['error_path']} "
            f"{job_script}")
    
    def _parse_job_id(self, submit_output):
        """Parse job ID from Torque submission output."""
        job_id = submit_output.strip()
        try:
            return str(int(job_id))  # Verify it's a valid number
        except ValueError:
            # Try to extract just the number if we got a full message
            try:
                return str(int(job_id.split()[-1]))
            except (IndexError, ValueError):
                raise RuntimeError(f"Could not parse job ID from qsub output: {job_id}")
    
    def _monitor_job(self, job_id, job_script):
        """Monitor Torque job and return results."""
        while True:
            cmd = f"qstat -f {job_id}"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if process.returncode != 0:
                # Job not found in qstat could mean it's completed
                # Use tracejob to get final status
                cmd = f"tracejob {job_id}"
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if "Exit_status=" in process.stdout:
                    if "Exit_status=0" in process.stdout:
                        self.logger.info(f"Job {job_id} completed successfully")
                        break
                    else:
                        status = process.stdout.split("Exit_status=")[1].split()[0]
                        self.logger.error(f"Job {job_id} failed with exit status: {status}")
                        raise RuntimeError(f"Job {job_id} failed with exit status: {status}")
                
                self.logger.error(f"Failed to get job status: {process.stderr}")
                raise RuntimeError(f"Failed to get job status: {process.stderr}")
            
            time.sleep(10)
            
        # Create a simple result object to match DRMAA interface
        class Result:
            def __init__(self):
                self.resourceUsage = (
                    self._get_resource_usage(job_id))
                
            def _get_resource_usage(self, job_id):
                """Get resource usage from tracejob."""
                cmd = (f"tracejob -v {job_id}")
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if process.returncode == 0:
                    # Parse and return as dict
                    values = process.stdout.strip().split()
                    return {
                        'max_rss': values[0] if len(values) > 0 else None,
                        'cpu_time': values[1] if len(values) > 1 else None,
                        'io': values[2] if len(values) > 2 else None
                    }
                return {}
                
        return Result()


class LocalExecutor(BaseExecutor):
    """Executor for running jobs locally."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "local_task"
        self.default_total_time = 5

    def run(self, statement_list):
        benchmark_data = []
        for statement in statement_list:
            self.logger.info(f"Running local statement: {statement}")

            full_statement, job_path = self.build_job_script(statement)

            # Execute locally using subprocess
            process = subprocess.Popen(
                full_statement,
                shell=True,
                cwd=self.config.get("work_dir", "."),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            stdout, stderr = process.communicate()

            if process.returncode != 0:
                self.logger.error(f"Local execution failed: {stderr.decode('utf-8')}")
                raise RuntimeError(f"Local execution failed: {stderr.decode('utf-8')}")

            self.logger.info(f"Local job completed. Output: {stdout.decode('utf-8')}")

            benchmark_data.append(self.collect_benchmark_data([statement], resource_usage=[]))

        return benchmark_data

    def build_job_script(self, statement):
        """Custom build job script for local execution."""
        return super().build_job_script(statement)

    def collect_benchmark_data(self, statements, resource_usage=None):
        """Collect benchmark data for local execution.
        
        Args:
            statements (list): List of executed statements
            resource_usage (list, optional): Resource usage data
            
        Returns:
            dict: Benchmark data including task name and execution time
        """
        return {
            "task": self.task_name,
            "total_t": self.default_total_time,
            "statements": statements,
            "resource_usage": resource_usage or []
        }
