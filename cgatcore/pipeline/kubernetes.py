# cgatcore/pipeline/kubernetes.py

import logging
import os
import time
import json
from datetime import datetime
from kubernetes import client, config
from cgatcore.pipeline.base_executor import BaseExecutor
from kubernetes.client import exceptions

logger = logging.getLogger(__name__)


class KubernetesExecutor(BaseExecutor):
    """Executor for managing and running jobs on a Kubernetes cluster.
    
    This class is responsible for submitting jobs to a Kubernetes cluster, monitoring their execution,
    and collecting benchmark data related to their performance.
    
    Attributes:
        namespace (str): The Kubernetes namespace in which to run the jobs. Defaults to 'default'.
        api (CoreV1Api): The Kubernetes Core API client for interacting with the cluster.
        batch_api (BatchV1Api): The Kubernetes Batch API client for managing jobs.
    """

    def __init__(self, **kwargs):
        """Initializes the KubernetesExecutor with the specified configuration options.
        
        Args:
            **kwargs: Additional configuration options, including the namespace.
        """
        super().__init__(**kwargs)
        self.namespace = kwargs.get("namespace", "default")
        
        # Load Kubernetes configuration
        try:
            config.load_kube_config()
            self.api = client.CoreV1Api()
            self.batch_api = client.BatchV1Api()
            logger.info("Kubernetes configuration loaded successfully.")
        except exceptions.ConfigException as e:
            logger.error("Failed to load Kubernetes configuration", exc_info=True)
            raise e

    def run(self, statement, job_path, job_condaenv):
        """Submits a job to the Kubernetes cluster to run the specified command.
        
        This method creates a Kubernetes Job object and submits it to the cluster. The job runs the
        specified command in a container, using the provided Conda environment.
        
        Args:
            statement (str): The command to execute in the job.
            job_path (str): The path to the job script.
            job_condaenv (str): The name of the Conda environment to use.
        """
        job_name = f"cgat-{os.path.basename(job_path)}-{int(time.time())}"
        container_image = "your-docker-image:tag"  # Replace with your Docker image
        
        # Define Kubernetes Job spec
        job_spec = client.V1Job(
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name="cgat-job",
                                image=container_image,
                                command=["/bin/bash", "-c", statement],
                                env=[client.V1EnvVar(name="CONDA_ENV", value=job_condaenv)],
                            )
                        ],
                        restart_policy="Never"
                    )
                ),
                backoff_limit=4  # Retry policy in case of transient failures
            )
        )

        # Create and monitor Kubernetes Job
        try:
            logger.info(f"Creating Kubernetes Job '{job_name}' in namespace '{self.namespace}'.")
            start_time = datetime.now()
            self.batch_api.create_namespaced_job(self.namespace, job_spec)
            self._wait_for_job_completion(job_name)
            end_time = datetime.now()
            logs = self._get_pod_logs(job_name)
            self.collect_metric_data("Kubernetes Job", start_time, end_time, "time_data.json")
        finally:
            self._cleanup_job(job_name)

        return logs

    def _wait_for_job_completion(self, job_name):
        """Wait until the job completes or fails."""
        while True:
            job_status = self.batch_api.read_namespaced_job_status(job_name, self.namespace).status
            if job_status.succeeded:
                logger.info(f"Job '{job_name}' completed successfully.")
                return
            if job_status.failed:
                logger.error(f"Job '{job_name}' failed.")
                raise RuntimeError(f"Kubernetes Job {job_name} failed.")
            time.sleep(5)

    def _get_pod_logs(self, job_name):
        """Retrieve logs from the Job's pod."""
        pods = self.api.list_namespaced_pod(self.namespace, label_selector=f"job-name={job_name}").items
        if not pods:
            logger.error(f"No pod found for job '{job_name}'.")
            raise RuntimeError(f"No pod found for job '{job_name}'.")
        
        pod_name = pods[0].metadata.name
        logger.info(f"Fetching logs from pod '{pod_name}'.")
        return self.api.read_namespaced_pod_log(pod_name, self.namespace)

    def _cleanup_job(self, job_name):
        """Delete the Job and its pods."""
        try:
            self.batch_api.delete_namespaced_job(job_name, self.namespace, propagation_policy="Background")
            logger.info(f"Job '{job_name}' cleaned up successfully.")
        except exceptions.ApiException as e:
            logger.warning(f"Failed to delete Job '{job_name}'", exc_info=True)

    def collect_benchmark_data(self, statements, resource_usage=None):
        """Collect benchmark data for Kubernetes jobs.
        
        This method gathers information about the executed statements and any resource usage data.
        
        Args:
            statements (list): List of executed statements.
            resource_usage (list, optional): Resource usage data.
        
        Returns:
            dict: A dictionary containing the task name, total execution time, executed statements,
                  and resource usage data.
        """
        return {
            "task": "kubernetes_task",
            "total_t": 12,  # Example value, adjust as needed
            "statements": statements,
            "resource_usage": resource_usage or []
        }

    def collect_metric_data(self, process, start_time, end_time, time_data_file):
        """
        Collects metric data related to job duration and writes it to a file.
        
        Parameters:
        - process (str): Process name for tracking purposes.
        - start_time (datetime): Timestamp when the job started.
        - end_time (datetime): Timestamp when the job ended.
        - time_data_file (str): Path to a file where timing data will be saved.
        """
        duration = (end_time - start_time).total_seconds()
        metric_data = {
            "process": process,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration
        }
        
        # Log metric data
        logger.info(
            f"Metric data collected for process '{process}': start time = {start_time}, end time = {end_time}, "
            f"duration = {duration} seconds."
        )
        
        # Write metric data to file
        try:
            with open(time_data_file, "w") as f:
                json.dump(metric_data, f, indent=4)
            logger.info(f"Metric data saved to {time_data_file}")
        
        except Exception as e:
            logger.error("Error writing metric data to file", exc_info=True)
            raise e
