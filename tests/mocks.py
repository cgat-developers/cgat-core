# tests/mocks.py

import os

class MockS3RemoteObject:
    def __init__(self, *args, **kwargs):
        self.storage = {}

    def upload(self, local_path, s3_path, file_dir=None):  # Added file_dir parameter for compatibility
        with open(local_path, 'r') as f:
            self.storage[s3_path] = f.read()

    def download(self, s3_path, local_path, file_dir=None):  # Added file_dir parameter for compatibility
        content = self.storage.get(s3_path, f"Mock content for {s3_path}")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w') as f:
            f.write(content)

    def exists(self, bucket, key):
        return f"s3://{bucket}/{key}" in self.storage

    def delete(self, s3_path):
        if s3_path in self.storage:
            del self.storage[s3_path]
