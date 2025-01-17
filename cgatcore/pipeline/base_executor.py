# cgatcore/pipeline/base_executor.py
import os
import tempfile


class BaseExecutor:
    """Base class for executors that defines the interface for running jobs."""

    def __init__(self, **kwargs):
        """Initialize the executor with configuration options."""
        self.config = kwargs
        self.task_name = "base_task"  # Should be overridden by subclasses
        self.default_total_time = 0  # Should be overridden by subclasses
        
        # Initialize job memory and threads
        self.job_memory = kwargs.get('job_memory', '1G')
        self.job_threads = kwargs.get('job_threads', 1)

    def run(self, statement, *args, **kwargs):
        """Run the given job statement. This should be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method")

    def collect_metric_data(self, *args, **kwargs):
        """Collect metric data if needed."""
        raise NotImplementedError("Subclasses must implement this method")

    def collect_benchmark_data(self, statements, resource_usage=None):
        """Collect benchmark data for job execution.
        
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

    def build_job_script(self, statement):
        """Build a simple job script for execution.
        Args:
        statement (str): The command or script to be executed.
        Returns:
        tuple: A tuple containing the full command (as a string) and the path where the job script is stored.
        """
        
        job_script_dir = self.config.get("job_script_dir", tempfile.gettempdir())
        os.makedirs(job_script_dir, exist_ok=True)
    
        script_path = os.path.join(job_script_dir, "job_script.sh")
        with open(script_path, "w") as script_file:
            script_file.write(f"#!/bin/bash\n\n{statement}\n")
        
        os.chmod(script_path, 0o755)  # Make it executable
        return statement, script_path

    def __enter__(self):
        """Enter the runtime context related to this object."""
        # Any initialisation logic needed for the executor can be added here
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        # Cleanup logic, if any, can be added here
        pass


class Executor(BaseExecutor):
    """Main executor class that handles job execution and resource management."""

    def __init__(self, **kwargs):
        """Initialize with configuration options."""
        super().__init__(**kwargs)
        self.task_name = "executor_task"
        self.default_total_time = 5
        
        # Initialize job options
        self.job_options = kwargs.get('job_options', '')
        self.queue = kwargs.get('queue')
        self.cluster_queue_manager = kwargs.get('cluster_queue_manager', 'slurm')

    def run(self, statement_list, **kwargs):
        """Execute a list of statements.
        
        Args:
            statement_list (list): List of commands to execute
            **kwargs: Additional execution options
            
        Returns:
            tuple: (exit_code, stdout, stderr)
        """
        if isinstance(statement_list, str):
            statement_list = [statement_list]
            
        results = []
        for statement in statement_list:
            # Choose appropriate executor based on configuration
            if self.cluster_queue_manager == 'slurm':
                from cgatcore.pipeline.executors import SlurmExecutor
                executor = SlurmExecutor(**self.config)
            elif self.cluster_queue_manager == 'sge':
                from cgatcore.pipeline.executors import SGEExecutor
                executor = SGEExecutor(**self.config)
            elif self.cluster_queue_manager == 'torque':
                from cgatcore.pipeline.executors import TorqueExecutor
                executor = TorqueExecutor(**self.config)
            else:
                from cgatcore.pipeline.executors import LocalExecutor
                executor = LocalExecutor(**self.config)
                
            result = executor.run(statement)
            results.append(result)
            
        return results[0] if len(results) == 1 else results
