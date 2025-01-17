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
    
    def __init__(self, session, queue_manager_cls, **kwargs):
        self.session = session
        self.cluster_manager = queue_manager_cls(session, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.queue = kwargs.get('queue')
        
    def submit_and_monitor(self, job_script, job_args, job_name):
        """Use existing DRMAA cluster implementation."""
        # Extract script path from tuple if needed
        script_path = job_script[1] if isinstance(job_script, tuple) else job_script
        
        try:
            # Import drmaa here since it's only needed for this strategy
            import drmaa
            
            # Setup job using existing cluster.py functionality
            jt = self.cluster_manager.setup_drmaa_job_template(
                self.session,
                job_name=job_name,
                job_memory=job_args.get('job_memory'),
                job_threads=job_args.get('job_threads'),
                working_directory=os.path.dirname(script_path),
                queue=self.queue)
            
            # Set the command to execute
            jt.remoteCommand = script_path
            jt.joinFiles = True
            
            # Submit job
            job_id = self.session.runJob(jt)
            self.logger.info(f"Submitted job {job_id}")
            
            # Monitor job status
            retval = None
            while True:
                try:
                    job_status = str(self.session.jobStatus(job_id))
                    if job_status == str(drmaa.JobState.DONE):
                        break
                    elif job_status == str(drmaa.JobState.FAILED):
                        raise RuntimeError(f"Job {job_id} failed with status: {job_status}")
                    time.sleep(5)  # Wait before checking again
                except drmaa.InvalidJobException:
                    # Job completed and was cleaned up
                    break
            
            # Get job info and resource usage
            job_info = self.session.wait(job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
            hostname = job_info.resourceUsage.get('submission_host', 'unknown')
            retval = self.cluster_manager.get_resource_usage(job_id, job_info, hostname)
            
            return retval
            
        except Exception as e:
            self.logger.error(f"DRMAA execution failed: {e}")
            raise
            
        finally:
            if 'jt' in locals():
                self.session.deleteJobTemplate(jt)


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
            session = drmaa.Session()
            session.initialize()
            # Don't pass queue to DRMAACluster initialization
            cluster_kwargs = {k: v for k, v in kwargs.items() if k != 'queue'}
            self.executor = DRMAAExecutorStrategy(
                session=session,
                queue_manager_cls=SlurmCluster,
                **cluster_kwargs)
            self.logger.info("Using DRMAA-based execution")
        except (ImportError, Exception) as e:
            self.logger.warning(f"DRMAA initialization failed: {e}")
            self.logger.warning("Falling back to subprocess-based execution")
            self.executor = SubprocessSlurmStrategy(**kwargs)

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
    
    def _build_submit_cmd(self, job_script, job_args, job_name):
        """Build SLURM-specific submit command."""
        return (f"sbatch --parsable "
                f"--job-name={job_name} "
                f"--output={job_args['output_path']} "
                f"--error={job_args['error_path']} "
                f"{job_script}")
    
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
        while True:
            # First try squeue to check if job is still running
            cmd = f"squeue -j {job_id} -h -o %T"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if process.returncode != 0:
                # Job not in queue, check final status with sacct
                cmd = f"sacct -j {job_id} --format=State --noheader --parsable2"
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if process.returncode != 0:
                    self.logger.error(f"Failed to get job status: {process.stderr}")
                    raise RuntimeError(f"Failed to get job status: {process.stderr}")

                status = process.stdout.strip().split('\n')[0]  # Get first line only
                
                # Check for various forms of completion status
                if any(s in status.upper() for s in ["COMPLETED", "COMPLETE", "DONE"]):
                    self.logger.info(f"Job {job_id} completed successfully")
                    break
                elif any(s in status.upper() for s in ["FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY"]):
                    self.logger.error(f"Job {job_id} failed with status: {status}")
                    raise RuntimeError(f"Job {job_id} failed with status: {status}")
            else:
                # Job still in queue, wait and check again
                status = process.stdout.strip()
                self.logger.debug(f"Job {job_id} status: {status}")
                
                if status in ["FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL"]:
                    self.logger.error(f"Job {job_id} failed with status: {status}")
                    raise RuntimeError(f"Job {job_id} failed with status: {status}")
            
            time.sleep(10)
            
        # Create a simple result object to match DRMAA interface
        class Result:
            def __init__(self):
                self.resourceUsage = (
                    self._get_resource_usage(job_id))
                
            def _get_resource_usage(self, job_id):
                """Get resource usage from sacct."""
                cmd = (
                    f"sacct -j {job_id} "
                    f"--format=JobID,State,Elapsed,MaxRSS,"
                    f"MaxVMSize,AveRSS,AveVMSize "
                    f"--noheader --parsable2")
                process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if process.returncode == 0:
                    # Parse and return as dict
                    values = process.stdout.strip().split('|')
                    return {
                        'max_rss': values[3] if len(values) > 3 else None,
                        'max_vmem': values[4] if len(values) > 4 else None,
                        'average_rss': values[5] if len(values) > 5 else None,
                        'average_vmem': values[6] if len(values) > 6 else None
                    }
                return {}
                
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
