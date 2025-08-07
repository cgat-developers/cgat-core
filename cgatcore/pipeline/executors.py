import subprocess
import time
import logging
from cgatcore.pipeline.base_executor import BaseExecutor


class SGEExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "sge_task"
        self.default_total_time = 8

    def run(self, statement_list):
        benchmark_data = []
        for statement in statement_list:
            self.logger.info(f"Running statement on SGE: {statement}")

            full_statement, job_path = self.build_job_script(statement)

            # Build the SGE job submission command
            sge_command = f"qsub -N {self.config.get('job_name', 'default_job')} -cwd -o {job_path}.o -e {job_path}.e {job_path}"

            process = subprocess.run(sge_command, shell=True, capture_output=True, text=True)

            if process.returncode != 0:
                self.logger.error(f"SGE job submission failed: {process.stderr}")
                raise RuntimeError(f"SGE job submission failed: {process.stderr}")

            # Parse job ID from qsub output
            output = process.stdout.strip()
            # SGE typically returns just the job ID number
            job_id = output.split()[0] if output else "unknown"
            
            self.logger.info(f"SGE job submitted with ID: {job_id}")

            # Monitor job completion
            self.monitor_job_completion(job_id)

            benchmark_data.append(self.collect_benchmark_data([statement], resource_usage=[]))

        return benchmark_data

    def build_job_script(self, statement):
        """Custom build job script for SGE."""
        return super().build_job_script(statement)

    def monitor_job_completion(self, job_id):
        """Monitor the completion of an SGE job.

        Args:
            job_id (str): The SGE job ID to monitor.

        Raises:
            RuntimeError: If the job fails.
        """
        import time
        
        # Track job start time
        job_start_time = time.time()
        self.logger.info(f"Started monitoring SGE job {job_id}")
        
        # Use consistent polling interval
        polling_interval = 10
        
        while True:
            current_time = time.time()
            elapsed = current_time - job_start_time
            
            # Periodically log status for debugging
            if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                self.logger.debug(f"Still monitoring job {job_id} after {int(elapsed / 60)} minutes")
                
            # Use qstat to get job status
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
            
            # Wait before checking again
            time.sleep(polling_interval)

    def collect_benchmark_data(self, statements, resource_usage=None):
        """Collect benchmark data for SGE jobs.
        
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


class SlurmExecutor(BaseExecutor):
    """Executor for running jobs on Slurm cluster."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "slurm_task"
        self.default_total_time = 10

    def run(self, statement_list):
        benchmark_data = []
        for statement in statement_list:
            self.logger.info(f"Running statement on Slurm: {statement}")

            full_statement, job_path = self.build_job_script(statement)

            # Build the Slurm job submission command
            slurm_command = f"sbatch --job-name={self.config.get('job_name', 'default_job')} --output={job_path}.o --error={job_path}.e {job_path}"

            process = subprocess.run(slurm_command, shell=True, capture_output=True, text=True)

            if process.returncode != 0:
                self.logger.error(f"Slurm job submission failed: {process.stderr}")
                raise RuntimeError(f"Slurm job submission failed: {process.stderr}")

            sbatch_output = process.stdout.strip()
            if "Submitted batch job" in sbatch_output:
                job_id = sbatch_output.split()[-1]  # Get the last word (job ID)
            else:
                job_id = sbatch_output  # Fallback to full output
            
            self.logger.info(f"Slurm job submitted with ID: {job_id}")

            # Monitor job completion
            self.monitor_job_completion(job_id)

            benchmark_data.append(self.collect_benchmark_data([statement], resource_usage=[]))

        return benchmark_data

    def build_job_script(self, statement):
        """Build a job script with Slurm-specific directives.
        
        Args:
            statement (str): The command to execute
            
        Returns:
            tuple: Enhanced statement and path to job script
        """
        import uuid
        import time
        import os
        
        # Get work_dir from the executor config or use current directory
        work_dir = self.config.get('work_dir', os.getcwd())
        
        # Create job_scripts directory in work_dir if it doesn't exist
        job_script_dir = os.path.join(work_dir, "job_scripts")
        os.makedirs(job_script_dir, exist_ok=True)
        
        # Create a unique script name using job name (if available), timestamp, and UUID
        job_name = self.config.get("job_name", "slurm_job")
        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]  # Use first 8 chars of UUID
        
        # Format: ctmp_{job_name}_{timestamp}_{uuid}.sh
        script_name = f"ctmp_{job_name}_{timestamp}_{unique_id}.sh"
        script_path = os.path.join(job_script_dir, script_name)
        
        # Enhanced statement that handles output directory creation for shell redirection
        enhanced_statement = self._prepare_statement_with_output_dirs(statement)
        
        # Slurm resource parameters
        time_limit = self.config.get('job_time', '7-00:00:00')  # Default: 7 days
        memory = self.config.get('job_memory', '4G')
        cpus = self.config.get('job_threads', 1)
        
        # Build Slurm job script with proper directives
        script_content = [
            "#!/bin/bash",
            f"#SBATCH --job-name={job_name}",
            f"#SBATCH --time={time_limit}",
            f"#SBATCH --mem={memory}",
            f"#SBATCH --cpus-per-task={cpus}",
            f"#SBATCH --output={script_path}.o",
            f"#SBATCH --error={script_path}.e",
            "",
            f"# Job: {job_name}",
            f"# Generated: {time.ctime()}",
            f"# ID: {unique_id}",
            f"# Resources: {cpus} CPUs, {memory} memory, {time_limit} time",
            "",
            enhanced_statement
        ]
        
        with open(script_path, "w") as script_file:
            script_file.write("\n".join(script_content))
        
        os.chmod(script_path, 0o755)  # Make it executable
        self.logger.info(f"Created job script: {script_path}")
        return enhanced_statement, script_path

    def monitor_job_completion(self, job_id):
        """Monitor the completion of a Slurm job.

        Args:
            job_id (str): The Slurm job ID to monitor.

        Raises:
            RuntimeError: If the job fails.
        """
        import time
        
        # Track job start time and last status
        job_start_time = time.time()
        self.logger.info(f"Started monitoring Slurm job {job_id}")
        last_status = None
        
        # Use consistent polling interval
        polling_interval = 10
        
        while True:
            current_time = time.time()
            elapsed = current_time - job_start_time
            # Periodically log status for debugging
            if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                self.logger.debug(f"Still monitoring job {job_id} after {int(elapsed / 60)} minutes")
            
            # Use sacct to get job status
            cmd = f"sacct -j {job_id} --format=State --noheader --parsable2"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if process.returncode != 0:
                self.logger.error(f"Failed to get job status: {process.stderr}")
                if "slurm_load_jobs error: Invalid job id specified" in process.stderr:
                    self.logger.error(f"Job {job_id} no longer exists in Slurm")
                    raise RuntimeError(f"Job {job_id} disappeared from Slurm queue")
                else:
                    raise RuntimeError(f"Failed to get job status: {process.stderr}")

            # Get the first line of status (main job status)
            status_lines = process.stdout.strip().split('\n')
            if not status_lines or not status_lines[0].strip():
                # Job status not available yet (just submitted), wait and continue
                self.logger.debug(f"Job {job_id} status not available yet, waiting...")
                time.sleep(polling_interval)
                continue
                
            # Parse status
            status = status_lines[0].strip()
            
            # Track status changes for logging
            if status != last_status:
                self.logger.info(f"Job {job_id} status: {status}")
                last_status = status
            
            # Handle empty status (job just submitted or sacct delay)
            if not status:
                time.sleep(polling_interval)
                continue
            
            if status in ["COMPLETED", "COMPLETED+"]:
                run_time = time.time() - job_start_time
                self.logger.info(f"Job {job_id} completed successfully after {run_time:.2f} seconds")
                break
            elif status in ["FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY"]:
                run_time = time.time() - job_start_time
                self.logger.error(f"Job {job_id} failed with status: {status} after {run_time:.2f} seconds")
                raise RuntimeError(f"Job {job_id} failed with status: {status}")
            else:
                # Job still running or pending, wait and check again
                self.logger.debug(f"Job {job_id} status: {status}, waiting...")
                time.sleep(polling_interval)

    def collect_benchmark_data(self, statements, resource_usage=None):
        """Collect benchmark data for Slurm jobs.
        
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


class TorqueExecutor(BaseExecutor):
    """Executor for running jobs on Torque cluster."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.task_name = "torque_task"
        self.default_total_time = 7

    def run(self, statement_list):
        benchmark_data = []
        for statement in statement_list:
            self.logger.info(f"Running statement on Torque: {statement}")

            full_statement, job_path = self.build_job_script(statement)

            # Build the Torque job submission command
            torque_command = f"qsub -N {self.config.get('job_name', 'default_job')} -o {job_path}.o -e {job_path}.e {job_path}"

            process = subprocess.run(torque_command, shell=True, capture_output=True, text=True)

            if process.returncode != 0:
                self.logger.error(f"Torque job submission failed: {process.stderr}")
                raise RuntimeError(f"Torque job submission failed: {process.stderr}")

            output = process.stdout.strip()
            # Torque typically returns the job ID in format like "123456.hostname"
            job_id = output.split()[0] if output else "unknown"
            self.logger.info(f"Torque job submitted with ID: {job_id}")

            # Monitor job completion
            self.monitor_job_completion(job_id)

            benchmark_data.append(self.collect_benchmark_data([statement], resource_usage=[]))

        return benchmark_data

    def build_job_script(self, statement):
        """Custom build job script for Torque."""
        return super().build_job_script(statement)

    def monitor_job_completion(self, job_id):
        """Monitor the completion of a Torque job.

        Args:
            job_id (str): The Torque job ID to monitor.

        Raises:
            RuntimeError: If the job fails or times out.
        """
        import time
        
        # Track job start time
        job_start_time = time.time()
        self.logger.info(f"Started monitoring Torque job {job_id}")
        
        # Use consistent polling interval
        polling_interval = 10
        
        while True:
            current_time = time.time()
            elapsed = current_time - job_start_time
            # Periodically log status for debugging
            if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                self.logger.debug(f"Still monitoring job {job_id} after {int(elapsed / 60)} minutes")
            
            # Use qstat to get job status
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
            
            # Wait before checking again
            time.sleep(polling_interval)

    def collect_benchmark_data(self, statements, resource_usage=None):
        """Collect benchmark data for Torque jobs.
        
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
