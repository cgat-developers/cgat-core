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
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        self.patcher.stop()
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)

    def test_s3_transform(self):
        p = Pipeline("test_s3_transform")

        input_path = os.path.join(self.temp_dir, "input.txt")
        output_path = os.path.join(self.temp_dir, "input.processed")

        @P.s3_transform(input_path, suffix(".txt"), ".processed")
        def process_file(infile, outfile):
            with open(infile, 'r') as f_in, open(outfile, 'w') as f_out:
                f_out.write(f_in.read().upper())

        # Simulate input file
        with open(input_path, 'w') as f:
            f.write("hello world")

        p.run()

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, 'r') as f:
            self.assertEqual(f.read(), "HELLO WORLD")

    def test_s3_merge(self):
        p = Pipeline("test_s3_merge")

        input_files = [os.path.join(self.temp_dir, f"file{i}.txt") for i in range(1, 3)]
        output_file = os.path.join(self.temp_dir, "merged.txt")

        @P.s3_merge(input_files, output_file)
        def merge_files(infiles, outfile):
            with open(outfile, 'w') as f_out:
                for infile in infiles:
                    with open(infile, 'r') as f_in:
                        f_out.write(f_in.read() + '\n')

        # Simulate input files
        for i, file in enumerate(input_files, 1):
            with open(file, 'w') as f:
                f.write(f"content{i}")

        p.run()

        self.assertTrue(os.path.exists(output_file))
        with open(output_file, 'r') as f:
            content = f.read().strip().split('\n')
            self.assertEqual(content, ["content1", "content2"])


if __name__ == '__main__':
    unittest.main()
