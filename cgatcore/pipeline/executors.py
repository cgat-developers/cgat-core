import subprocess
import time
import logging
import os
import tempfile
import re
from cgatcore.pipeline.base_executor import BaseExecutor
from cgatcore.pipeline.logging_utils import LoggingFilterpipelineName
import glob


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
        
        # Get default partition
        self.get_default_partition()
        
    def get_default_partition(self):
        """Get the default SLURM partition to use."""
        # First check if partition is specified in config
        partition = self.config.get("queue", None)
        
        if not partition:
            # Try to get list of available partitions
            try:
                process = subprocess.run(["sinfo", "-h", "-o", "%R"], 
                                      capture_output=True, text=True)
                if process.returncode == 0:
                    # Get first available partition
                    partitions = process.stdout.strip().split('\n')
                    if partitions:
                        partition = partitions[0]
                        self.logger.info(f"Using SLURM partition: {partition}")
            except Exception as e:
                self.logger.warning(f"Failed to get SLURM partitions: {e}")
        
        if not partition:
            # If still no partition, try common names
            partition = "all"  # Common default name
            
        self.default_partition = partition

    def run(self, statement_list):
        """Run a list of statements on the cluster.

        Parameters
        ----------
        statement_list : list
            List of commands to execute.

        Returns
        -------
        list : Benchmark data for each job.
        """
        benchmark_data = []

        for statement in statement_list:
            self.logger.info(f"Running statement on Slurm: {statement}")

            # Get job parameters from config
            job_threads = str(self.config.get("job_threads", self.config.get("num_cpus", "1")))
            job_memory = self.config.get("memory", "4G")
            job_queue = self.config.get("queue", self.default_partition)
            job_time = self.config.get("runtime", "01:00:00")

            # Build job script
            full_statement, job_path = self.build_job_script(statement)
            
            # Build sbatch command with explicit parameters
            slurm_command = [
                "sbatch",
                "--parsable",
                f"--partition={job_queue}",
                f"--mem={job_memory}",
                f"--cpus-per-task={job_threads}",
                f"--time={job_time}",
                "--export=ALL",
                job_path
            ]
            
            self.logger.info(f"Submitting SLURM job with command: {' '.join(slurm_command)}")
            process = subprocess.run(slurm_command, capture_output=True, text=True)

            if process.returncode != 0:
                self.logger.error(f"Slurm job submission failed: {process.stderr}")
                raise RuntimeError(f"Slurm job submission failed: {process.stderr}")

            # Get just the job ID number from the output
            job_id = process.stdout.strip()
            self.logger.info(f"Slurm job submitted with ID: {job_id}")

            try:
                # Monitor job completion
                self.monitor_job_completion(job_id)
                benchmark_data.append(self.collect_benchmark_data([statement], resource_usage=[]))
            except Exception as e:
                # Try to get error file content
                try:
                    err_file = os.path.join(os.getcwd(), "slurm_jobs", f"{os.path.basename(job_path)}.err")
                    if os.path.exists(err_file):
                        with open(err_file, 'r') as f:
                            error_content = f.read()
                            self.logger.error(f"Job error output:\n{error_content}")
                except Exception as err_e:
                    self.logger.error(f"Could not read error file: {str(err_e)}")
                raise

        return benchmark_data

    def build_job_script(self, statement):
        """Custom build job script for Slurm."""
        # Use current working directory for job scripts
        job_path = os.path.join(os.getcwd(), "slurm_jobs")
        os.makedirs(job_path, exist_ok=True)
        
        # Generate a unique script name using timestamp
        timestamp = int(time.time())
        script_name = f"slurm_job_{timestamp}"
        script_path = os.path.join(job_path, f"{script_name}.sh")
        
        # Create output/error file paths in job directory
        out_file = os.path.join(job_path, f"{script_name}.out")
        err_file = os.path.join(job_path, f"{script_name}.err")
        
        # Parse the kallisto command to get output directory
        # Example: kallisto quant ... -o quant/i48-control1_S1 ...
        match = re.search(r'-o\s+(\S+)', statement)
        if match:
            output_dir = match.group(1)
        else:
            output_dir = None
        
        # Write SLURM script with proper headers
        with open(script_path, "w") as script_file:
            script_file.write("#!/bin/bash\n")
            script_file.write(f"#SBATCH --output={out_file}\n")
            script_file.write(f"#SBATCH --error={err_file}\n\n")
            
            # Load required modules and set environment
            script_file.write("# Load required modules and set environment\n")
            script_file.write("set -e\n")  # Exit on error
            script_file.write("set -x\n")  # Print commands as they execute
            script_file.write("module purge\n")
            script_file.write("module load kallisto\n\n")
            
            # Print environment info for debugging
            script_file.write("# Print environment info\n")
            script_file.write("echo 'Job started at: ' $(date)\n")
            script_file.write("echo 'Running on node: ' $HOSTNAME\n")
            script_file.write("echo 'Current directory: ' $PWD\n")
            script_file.write("module list\n\n")
            
            # Create all necessary output directories
            script_file.write("# Create output directories\n")
            if output_dir:
                script_file.write(f'mkdir -p "{output_dir}"\n')
                # Also create directory for log file
                script_file.write(f'mkdir -p "$(dirname "{output_dir}/abundance.tsv.log")"\n')
            
            # Add the actual command
            script_file.write("# Run the command\n")
            script_file.write(f"echo 'Running command: {statement}'\n")
            script_file.write(f"{statement}\n")
            script_file.write("\necho 'Job finished at: ' $(date)\n")
        
        os.chmod(script_path, 0o755)  # Make it executable
        return statement, script_path

    def monitor_job_completion(self, job_id):
        """Monitor a running SLURM job until it completes."""
        max_sleep_time = 10  # Maximum time to sleep between checks
        sleep_time = 1      # Initial sleep time
        
        while True:
            try:
                # Use sacct to get detailed job information
                cmd = ["sacct", "-j", str(job_id), "--format=State,ExitCode,NodeList,Start,End,Elapsed", "--parsable2", "--noheader"]
                process = subprocess.run(cmd, capture_output=True, text=True)
                
                if process.returncode != 0:
                    self.logger.error(f"Error checking job status: {process.stderr}")
                    raise RuntimeError(f"Error checking job status: {process.stderr}")
                
                # Parse the sacct output
                output = process.stdout.strip()
                if not output:
                    self.logger.info(f"Job {job_id} status: None")
                    time.sleep(sleep_time)
                    continue
                
                # Get the first line which contains the main job info
                job_info = output.split('\n')[0].split('|')
                state = job_info[0]
                
                self.logger.info(f"Job {job_id} status: {state}")
                
                if state in ["COMPLETED"]:
                    return
                elif state in ["FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL"]:
                    # Get the error file content
                    try:
                        err_file = os.path.join(os.getcwd(), "slurm_jobs", f"slurm_job_{job_id}.err")
                        out_file = os.path.join(os.getcwd(), "slurm_jobs", f"slurm_job_{job_id}.out")
                        error_content = ""
                        
                        if os.path.exists(err_file):
                            with open(err_file, 'r') as f:
                                error_content = f.read()
                                if error_content:
                                    self.logger.error(f"Job error output:\n{error_content}")
                        
                        if os.path.exists(out_file):
                            with open(out_file, 'r') as f:
                                out_content = f.read()
                                if out_content:
                                    self.logger.error(f"Job output:\n{out_content}")
                                    
                    except Exception as err_e:
                        self.logger.error(f"Could not read error/output files: {str(err_e)}")
                    
                    # Try to get detailed job information
                    try:
                        cmd = ["scontrol", "show", "job", str(job_id)]
                        process = subprocess.run(cmd, capture_output=True, text=True)
                        if process.returncode == 0:
                            self.logger.error(f"Detailed job info:\n{process.stdout}")
                    except Exception as e:
                        self.logger.error(f"Could not get detailed job info: {str(e)}")
                        
                    error_msg = f"Job {job_id} failed with status: {output}"
                    raise RuntimeError(error_msg)
                
                # Exponential backoff for sleep time
                sleep_time = min(sleep_time * 1.5, max_sleep_time)
                time.sleep(sleep_time)
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error running sacct: {e}")
                raise RuntimeError(f"Error running sacct: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error monitoring job: {e}")
                raise

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
