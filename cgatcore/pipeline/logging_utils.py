import logging

class LoggingFilterpipelineName(logging.Filter):
    """add pipeline name to log message.

    With this filter, %(app_name)s can be used in log formats.
    """
    def __init__(self, name, *args, **kwargs):
        logging.Filter.__init__(self, *args, **kwargs)
        self.app_name = name

    def filter(self, record):
        record.app_name = self.app_name
        return True
