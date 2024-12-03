import unittest
from unittest.mock import patch, MagicMock
from cgatcore.pipeline.execution import Executor, ContainerConfig


class TestContainerConfig(unittest.TestCase):
    def setUp(self):
        """Set up a mock for get_params and an Executor instance."""
        patcher = patch("cgatcore.pipeline.execution.get_params")
        self.mock_get_params = patcher.start()
        self.addCleanup(patcher.stop)

        # Mock the return value of get_params
        self.mock_get_params.return_value = {
            "cluster": {
                "options": "",
                "queue": None,
                "memory_default": "4G",
                "tmpdir": "/tmp",
                "monitor_interval_queued_default": 30,
                "monitor_interval_running_default": 30,
            },
            "cluster_tmpdir": "/tmp",
            "tmpdir": "/tmp",
            "work_dir": "/tmp",
            "os": "Linux",
        }

        self.executor = Executor()

    def test_set_container_config_docker(self):
        """Test setting container configuration for Docker."""
        self.executor.set_container_config(
            image="ubuntu:20.04",
            volumes=["/data:/data", "/reference:/reference"],
            env_vars={"TEST_VAR": "value"},
            runtime="docker"
        )
        config = self.executor.container_config
        self.assertIsInstance(config, ContainerConfig)
        self.assertEqual(config.image, "ubuntu:20.04")
        self.assertEqual(config.runtime, "docker")
        self.assertIn("/data:/data", config.volumes)
        self.assertIn("/reference:/reference", config.volumes)
        self.assertEqual(config.env_vars["TEST_VAR"], "value")

    def test_set_container_config_singularity(self):
        """Test setting container configuration for Singularity."""
        self.executor.set_container_config(
            image="/path/to/container.sif",
            volumes=["/data:/data", "/reference:/reference"],
            env_vars={"TEST_VAR": "value"},
            runtime="singularity"
        )
        config = self.executor.container_config
        self.assertIsInstance(config, ContainerConfig)
        self.assertEqual(config.image, "/path/to/container.sif")
        self.assertEqual(config.runtime, "singularity")
        self.assertIn("/data:/data", config.volumes)
        self.assertIn("/reference:/reference", config.volumes)
        self.assertEqual(config.env_vars["TEST_VAR"], "value")

    def test_invalid_runtime(self):
        """Test setting an invalid container runtime."""
        with self.assertRaises(ValueError) as context:
            self.executor.set_container_config(
                image="ubuntu:20.04", runtime="invalid_runtime"
            )
        self.assertIn("Unsupported container runtime", str(context.exception))

    def test_missing_image(self):
        """Test setting container configuration without an image."""
        with self.assertRaises(ValueError) as context:
            self.executor.set_container_config(
                image=None, runtime="docker"
            )
        self.assertIn("An image must be specified", str(context.exception))


if __name__ == "__main__":
    unittest.main()
