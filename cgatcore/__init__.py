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

    def get_remote(self):
        """Dynamically load and return the remote module when explicitly called."""
        if self._remote is None:
            from cgatcore import remote
            self._remote = remote
        return self._remote


# Create a global instance of the CgatCore class
cgatcore = CgatCore()
