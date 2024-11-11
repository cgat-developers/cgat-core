# cgatcore/remote/file_handler.py

import os
from functools import wraps
from .aws import S3RemoteObject


def s3_path_to_local(s3_path, temp_dir='/tmp'):
    """
    Converts an S3 path to a local file path within a temporary directory.
    """
    parts = s3_path[5:].split('/', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 path: {s3_path}")
    bucket, key = parts
    return os.path.join(temp_dir, key)


class S3Pipeline:
    def __init__(self, name=None, temp_dir='/tmp'):
        self.name = name
        self.s3 = S3RemoteObject()
        self.tasks = []
        self.temp_dir = temp_dir

    def configure_s3(self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        """
        Configure AWS credentials for S3 access. If no credentials are provided,
        it uses the default configuration from the environment or AWS config files.
        """
        import boto3
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.s3.S3 = session.resource('s3')

    def _is_s3_path(self, path):
        return isinstance(path, str) and path.startswith('s3://')

    def s3_transform(self, input_file, suffix_pattern, output_file):
        """
        Applies an S3-aware transform with a suffix pattern for output files.
        
        Args:
            input_file (str): The input S3 path or local path.
            suffix_pattern (str): The suffix to apply to the output file.
            output_file (str): The output S3 path or local path.
        """
        def decorator(func):
            @wraps(func)
            def wrapper():
                # Set up local path for input
                local_input = s3_path_to_local(input_file, self.temp_dir) if self._is_s3_path(input_file) else input_file

                # Download if input is in S3
                if self._is_s3_path(input_file):
                    self.s3.download(input_file, local_input, dest_dir=self.temp_dir)

                # Generate local output file path by applying the suffix pattern and original extension
                base, ext = os.path.splitext(local_input)
                local_output = f"{base}{suffix_pattern}{ext}"

                # Call the function with local input and local output paths
                func(local_input, local_output)

                # Upload if output is in S3
                if self._is_s3_path(output_file):
                    self.s3.upload(output_file, local_output)

            self.tasks.append(wrapper)
            return wrapper

        return decorator

    def s3_merge(self, input_files, output_file):
        """
        Merges multiple input files into a single output file.
        """
        def decorator(func):
            @wraps(func)
            def wrapper():
                local_inputs = []
                for input_file in input_files:
                    local_input = s3_path_to_local(input_file, self.temp_dir) if self._is_s3_path(input_file) else input_file

                    # Download each input file from S3 if necessary
                    if self._is_s3_path(input_file):
                        self.s3.download(input_file, local_input, dest_dir=self.temp_dir)
                    local_inputs.append(local_input)

                # Set up local output path
                local_output = s3_path_to_local(output_file, self.temp_dir) if self._is_s3_path(output_file) else output_file
                func(local_inputs, local_output)

                # Upload merged output to S3 if required
                if self._is_s3_path(output_file):
                    self.s3.upload(output_file, local_output)

            self.tasks.append(wrapper)
            return wrapper

        return decorator

    def s3_split(self, input_file, output_files):
        """
        Decorator for splitting a single input file into multiple output files.
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                local_input = s3_path_to_local(input_file, self.temp_dir) if self._is_s3_path(input_file) else input_file
                if self._is_s3_path(input_file):
                    self.s3.download(input_file, local_input, dest_dir=self.temp_dir)

                local_outputs = [s3_path_to_local(f, self.temp_dir) if self._is_s3_path(f) else f for f in output_files]
                func(local_input, local_outputs, *args, **kwargs)

                for local_output, s3_output in zip(local_outputs, output_files):
                    if self._is_s3_path(s3_output):
                        self.s3.upload(local_output, s3_output)

            self.tasks.append(wrapper)
            return wrapper
        return decorator

    def s3_originate(self, output_files):
        """
        Decorator for originating new files without any input files.
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                local_outputs = [s3_path_to_local(f, self.temp_dir) if self._is_s3_path(f) else f for f in output_files]
                func(local_outputs, *args, **kwargs)

                for local_output, s3_output in zip(local_outputs, output_files):
                    if self._is_s3_path(s3_output):
                        self.s3.upload(local_output, s3_output)

            self.tasks.append(wrapper)
            return wrapper
        return decorator

    def s3_follows(self, *args):
        """
        Decorator for tasks that follow other tasks without direct file dependencies.
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            self.tasks.append(wrapper)
            return wrapper
        return decorator

    def run(self):
        """
        Executes all the tasks in the pipeline sequentially.
        """
        for task in self.tasks:
            task()


def suffix(suffix_string):
    """
    Generates a filter function that appends a suffix to a given file path.
    """
    def filter_func(input_path):
        base, ext = os.path.splitext(input_path)
        return f"{base}{suffix_string}{ext}"
    return filter_func


class S3Mapper:
    """
    A mapper class for handling S3 operations.
    """
    def __init__(self):
        self.s3 = S3RemoteObject()


__all__ = ['S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix']
