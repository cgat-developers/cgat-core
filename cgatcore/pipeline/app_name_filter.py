import logging

class AppNameFilter(logging.Filter):
    """Filter that adds an app_name attribute to log records."""
    
    def __init__(self, app_name):
        """Initialize the filter with an app name.
        
        Args:
            app_name (str): Name to be added to log records
        """
        super().__init__()
        self.app_name = app_name
        
    def filter(self, record):
        """Add app_name attribute to the record.
        
        Args:
            record (LogRecord): The log record to filter
            
        Returns:
            bool: Always returns True to allow the record through
        """
        record.app_name = self.app_name
        return True
