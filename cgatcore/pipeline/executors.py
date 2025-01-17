import subprocess
import time
import logging
from cgatcore.pipeline.base_executor import BaseExecutor
from cgatcore.pipeline.control import LoggingFilterpipelineName


class SGEExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("cgatcore.pipeline")
        self.logger.addFilter(LoggingFilterpipelineName("sge"))  
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

            self.logger.info(f"SGE job submitted: {process.stdout.strip()}")

            # Monitor job completion
            self.monitor_job_completion(process.stdout.strip())

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
            RuntimeError: If the job fails or times out.
        """
        while True:
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
            time.sleep(10)

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
        self.logger = logging.getLogger("cgatcore.pipeline")
        self.logger.addFilter(LoggingFilterpipelineName("slurm"))  
        self.task_name = "slurm_task"
        self.default_total_time = 10

    def run(self, statement_list):
        benchmark_data = []
        for statement in statement_list:
            self.logger.info(f"Running statement on Slurm: {statement}")

            full_statement, job_path = self.build_job_script(statement)

            # Build the Slurm job submission command
            slurm_command = f"sbatch --parsable --job-name={self.config.get('job_name', 'default_job')} --output={job_path}.o --error={job_path}.e {job_path}"

            process = subprocess.run(slurm_command, shell=True, capture_output=True, text=True)

            if process.returncode != 0:
                self.logger.error(f"Slurm job submission failed: {process.stderr}")
                raise RuntimeError(f"Slurm job submission failed: {process.stderr}")

            # Get just the job ID number from the output
            job_id = process.stdout.strip()
            self.logger.info(f"Slurm job submitted with ID: {job_id}")

            # Monitor job completion
            self.monitor_job_completion(job_id)

            benchmark_data.append(self.collect_benchmark_data([statement], resource_usage=[]))

        return benchmark_data

    def build_job_script(self, statement):
        """Custom build job script for Slurm."""
        return super().build_job_script(statement)

    def monitor_job_completion(self, job_id):
        """Monitor the completion of a Slurm job.

        Args:
            job_id (str): The Slurm job ID to monitor.

        Raises:
            RuntimeError: If the job fails or times out.
        """
        while True:
            # Use sacct to get job status
            cmd = f"sacct -j {job_id} --format=State --noheader --parsable2"
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if process.returncode != 0:
                self.logger.error(f"Failed to get job status: {process.stderr}")
                raise RuntimeError(f"Failed to get job status: {process.stderr}")

            status = process.stdout.strip()
            
            # Check job status
            if status in ["COMPLETED", "COMPLETED+"]:
                self.logger.info(f"Job {job_id} completed successfully")
                break
            elif status in ["FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL"]:
                self.logger.error(f"Job {job_id} failed with status: {status}")
                raise RuntimeError(f"Job {job_id} failed with status: {status}")
            
            # Wait before checking again
            time.sleep(10)

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
        self.logger = logging.getLogger("cgatcore.pipeline")
        self.logger.addFilter(LoggingFilterpipelineName("torque"))  
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

            job_id = process.stdout.strip()
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
        while True:
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
            time.sleep(10)

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
    """Executor for running jobs locally using subprocess."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("cgatcore.pipeline")
        self.logger.addFilter(LoggingFilterpipelineName("local"))  
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
