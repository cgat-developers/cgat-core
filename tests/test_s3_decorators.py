import unittest
from unittest.mock import patch, MagicMock
import tempfile
import os
from cgatcore import pipeline as P
from ruffus import Pipeline


class MockS3Client:
    def __init__(self):
        self.storage = {}

    def upload_file(self, local_path, bucket, key):
        with open(local_path, 'r') as f:
            self.storage[f"{bucket}/{key}"] = f.read()

    def download_file(self, bucket, key, local_path):
        content = self.storage.get(f"{bucket}/{key}", f"Mock content for {bucket}/{key}")
        with open(local_path, 'w') as f:
            f.write(content)


class TestS3Decorators(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = MockS3Client()
        self.patcher = patch('cgatcore.remote.aws.boto3.resource')
        self.mock_resource = self.patcher.start()
        self.mock_resource.return_value = self.mock_s3

    def tearDown(self):
        self.patcher.stop()

    def test_s3_transform(self):
        p = Pipeline()

        @P.s3_transform("s3://my-bucket/input.txt", suffix(".txt"), ".processed")
        def process_file(infile, outfile):
            with open(infile, 'r') as f_in, open(outfile, 'w') as f_out:
                f_out.write(f_in.read().upper())

        # Simulate input file
        self.mock_s3.storage["my-bucket/input.txt"] = "hello world"

        p.run()

        self.assertIn("my-bucket/input.processed", self.mock_s3.storage)
        self.assertEqual(self.mock_s3.storage["my-bucket/input.processed"], "HELLO WORLD")

    def test_s3_merge(self):
        p = Pipeline()

        @P.s3_merge(["s3://my-bucket/file1.txt", "s3://my-bucket/file2.txt"], "s3://my-bucket/merged.txt")
        def merge_files(infiles, outfile):
            with open(outfile, 'w') as f_out:
                for infile in infiles:
                    with open(infile, 'r') as f_in:
                        f_out.write(f_in.read() + '\n')

        # Simulate input files
        self.mock_s3.storage["my-bucket/file1.txt"] = "content1"
        self.mock_s3.storage["my-bucket/file2.txt"] = "content2"

        p.run()

        self.assertIn("my-bucket/merged.txt", self.mock_s3.storage)
        self.assertEqual(self.mock_s3.storage["my-bucket/merged.txt"], "content1\ncontent2\n")


if __name__ == '__main__':
    unittest.main()
