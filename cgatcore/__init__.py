# cgatcore/__init__.py

class CgatCore:
    """Main class to encapsulate CGAT core functionality with lazy loading."""

    def __init__(self):
        self._pipeline = None
        self._remote = None

    @property
    def pipeline(self):
        """Lazy load the pipeline module."""
        if self._pipeline is None:
            from cgatcore import pipeline
            self._pipeline = pipeline
        return self._pipeline

    @property
    def remote(self):
        """Lazy load the remote module."""
        if self._remote is None:
            from cgatcore import remote
            self._remote = remote
        return self._remote


# Create a global instance of the CgatCore class
cgatcore = CgatCore()

# Expose the pipeline and remote modules as top-level attributes
pipeline = cgatcore.pipeline
remote = cgatcore.remote

__all__ = ["pipeline", "remote"]
