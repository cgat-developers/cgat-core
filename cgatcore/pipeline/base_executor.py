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
        
        # Enhanced script that handles output directory creation for shell redirection
        enhanced_statement = self._prepare_statement_with_output_dirs(statement)
        
        with open(script_path, "w") as script_file:
            script_file.write(f"#!/bin/bash\n\n{enhanced_statement}\n")
        
        os.chmod(script_path, 0o755)  # Make it executable
        return enhanced_statement, script_path

    def _prepare_statement_with_output_dirs(self, statement):
        """Prepare statement by ensuring output directories exist for shell redirection.
        
        This method analyzes the statement for output redirection patterns like
        '> path/to/file.log' and ensures the parent directories exist.
        """
        import re
        
        # Find all output redirections in the statement
        redirect_pattern = r'>\s+([^\s]+)'
        redirections = re.findall(redirect_pattern, statement)
        
        if not redirections:
            return statement
        
        # Build commands to create necessary directories
        mkdir_commands = []
        for output_file in redirections:
            # Extract directory from output file path
            output_dir = os.path.dirname(output_file)
            if output_dir and output_dir != '.':
                mkdir_commands.append(f"mkdir -p {output_dir}")
        
        if mkdir_commands:
            # Remove duplicates while preserving order
            unique_mkdir_commands = list(dict.fromkeys(mkdir_commands))
            mkdir_statement = " && ".join(unique_mkdir_commands)
            return f"{mkdir_statement} && {statement}"
        
        return statement

    def __enter__(self):
        """Enter the runtime context related to this object."""
        # Any initialisation logic needed for the executor can be added here
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        # Cleanup logic, if any, can be added here
        pass
