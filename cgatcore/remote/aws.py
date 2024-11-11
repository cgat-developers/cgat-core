import os
import boto3
import botocore

from .abstract import AbstractRemoteObject


class S3RemoteObject(AbstractRemoteObject):
    '''Interacts with an AWS object store.'''

    def __init__(self, *args, **kwargs):
        self._S3object = S3Connection(*args, **kwargs)

    def exists(self, bucket_name, key):
        """Check if a file exists in an S3 bucket."""
        try:
            self._S3object.object_exists(bucket_name, key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise S3FileException(
                    "Error checking existence in bucket '{}/{}'".format(bucket_name, key)
                )

    def download(self, bucket_name, key, dest_dir):
        """Download a file from S3."""
        self._S3object.remote_download(bucket_name, key, dest_dir)
        os.sync()  # Ensure flush to disk
        return dest_dir

    def upload(self, bucket_name, key, file_dir):
        """Upload a file to S3."""
        self._S3object.remote_upload(bucket_name, file_dir, key)
        return file_dir

    def delete_file(self, bucket_name, key):
        """Delete a file from S3."""
        self._S3object.remote_delete_file(bucket_name, key)
        return key


class S3Connection:
    '''Connection to a remote S3 bucket using the boto3 API.'''

    def __init__(self, *args, **kwargs):
        self.S3 = boto3.resource("s3", **kwargs)

    def bucket_exists(self, bucket_name):
        """Check if a bucket exists."""
        try:
            self.S3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except botocore.exceptions.ClientError:
            return False

    def object_exists(self, bucket_name, key):
        """Check if an object exists in a bucket."""
        try:
            self.S3.Object(bucket_name, key).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise e

    def remote_download(self, bucket_name, key, dest_dir):
        """Download an object from S3 to a local directory."""
        if not bucket_name or not key:
            raise ValueError("Both bucket name and key are required to download a file.")

        if dest_dir:
            dest_path = os.path.realpath(os.path.expanduser(dest_dir))
        else:
            raise ValueError("Destination directory must be provided.")

        s3_object = self.S3.Object(bucket_name, key)

        try:
            s3_object.download_file(dest_path)
        except Exception as e:
            raise Exception(
                f"Failed to download file from '{bucket_name}/{key}' to '{dest_path}': {str(e)}"
            )

    def remote_upload(self, bucket_name, file_dir, key):
        """Upload a file to S3."""
        file_path = os.path.realpath(os.path.expanduser(file_dir))

        if not bucket_name or not os.path.exists(file_path):
            raise ValueError(f"Bucket name and valid file path are required: '{file_path}'")

        if not self.bucket_exists(bucket_name):
            self.S3.create_bucket(Bucket=bucket_name)

        s3_object = self.S3.Object(bucket_name, key)

        try:
            s3_object.upload_file(file_path)
        except Exception as e:
            raise Exception(f"Failed to upload file '{file_path}' to '{bucket_name}/{key}': {str(e)}")

    def remote_delete_file(self, bucket_name, key):
        """Delete an object from an S3 bucket."""
        if not bucket_name or not key:
            raise ValueError("Both bucket name and key are required to delete a file.")

        s3_object = self.S3.Object(bucket_name, key)
        return s3_object.delete()
