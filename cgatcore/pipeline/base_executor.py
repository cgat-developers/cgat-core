# cgatcore/pipeline/base_executor.py
import os
import tempfile


class BaseExecutor:
    """Base class for executors that defines the interface for running jobs."""

    def __init__(self, **kwargs):
        """Initialize the executor with configuration options."""
        self.config = kwargs

    def run(self, statement, *args, **kwargs):
        """Run the given job statement. This should be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method")

    def collect_benchmark_data(self, *args, **kwargs):
        """Collect benchmark data if needed."""
        raise NotImplementedError("Subclasses must implement this method")

    def collect_metric_data(self, *args, **kwargs):
        """Collect metric data if needed."""
        raise NotImplementedError("Subclasses must implement this method")

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
