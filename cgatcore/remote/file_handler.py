# cgatcore/remote/file_handler.py

import os
import hashlib
from cgatcore.remote.aws import S3RemoteObject
from ruffus import transform, merge, split, originate, follows


class S3Mapper:
    def __init__(self):
        self.s3_to_local = {}
        self.local_to_s3 = {}
        self.s3 = S3RemoteObject()

    def get_local_path(self, s3_path):
        if s3_path in self.s3_to_local:
            return self.s3_to_local[s3_path]

        bucket, key = s3_path[5:].split('/', 1)
        local_path = os.path.join('/tmp', hashlib.md5(s3_path.encode()).hexdigest())
        self.s3_to_local[s3_path] = local_path
        self.local_to_s3[local_path] = (bucket, key)
        return local_path

    def download_if_s3(self, path):
        if path.startswith('s3://'):
            local_path = self.get_local_path(path)
            bucket, key = self.local_to_s3[local_path]
            self.s3.download(bucket, key, local_path)
            return local_path
        return path

    def upload_if_s3(self, path):
        if path in self.local_to_s3:
            bucket, key = self.local_to_s3[path]
            self.s3.upload(bucket, key, path)


s3_mapper = S3Mapper()


def s3_aware(func):
    def wrapper(*args, **kwargs):
        # Download S3 files before the task
        local_args = [s3_mapper.download_if_s3(arg) if isinstance(arg, str) else arg for arg in args]

        # Run the original function
        result = func(*local_args, **kwargs)

        # Upload modified files back to S3 after the task
        for arg in local_args:
            if isinstance(arg, str):
                s3_mapper.upload_if_s3(arg)

        return result

    return wrapper


def s3_transform(input_files, filter, output_files, *args, **kwargs):
    def decorator(func):
        @transform(input_files, filter, output_files, *args, **kwargs)
        @s3_aware
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapped_func

    return decorator


def s3_merge(input_files, output_file, *args, **kwargs):
    def decorator(func):
        @merge(input_files, output_file, *args, **kwargs)
        @s3_aware
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapped_func

    return decorator


def s3_split(input_files, output_files, *args, **kwargs):
    def decorator(func):
        @split(input_files, output_files, *args, **kwargs)
        @s3_aware
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapped_func

    return decorator


def s3_originate(output_files, *args, **kwargs):
    def decorator(func):
        @originate(output_files, *args, **kwargs)
        @s3_aware
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapped_func

    return decorator


# The @follows decorator doesn't directly handle files, so we can use it as is
s3_follows = follows
