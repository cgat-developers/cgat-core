import unittest
from unittest.mock import patch, MagicMock
from cgatcore import pipeline as P


class TestS3Decorators(unittest.TestCase):

    def setUp(self):
        # Setup code for S3 mock or actual S3 connection
        self.P = P
        self.s3_mock = MagicMock()
        patch('cgatcore.remote.aws.S3Connection', return_value=self.s3_mock).start()

    def test_s3_transform(self):
        """
        Test the s3_transform decorator.
        """
        self.P.configure_s3()  # Ensure S3 configuration is set up

        @self.P.s3_transform("my-bucket/input.txt", self.P.suffix(".processed"), "my-bucket/input.processed")
        def process_file(infile, outfile):
            # Simulate getting the content from S3
            input_data = self.s3_mock.Object.return_value.get()['Body'].read().decode()
            processed_data = input_data.upper()

            # Mock the upload method call correctly with the filename
            self.s3_mock.Object.return_value.upload_file(processed_data, outfile)

        # Mock S3 storage for input file
        self.s3_mock.Object.return_value.get.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=b'hello world'))
        }

        # Run the decorator function
        process_file()  # This should trigger the decorator handling internally

        # Verify the upload was called with the correct output file path
        # Change this line
        self.s3_mock.Object.return_value.upload_file.assert_called_with('HELLO WORLD', 'my-bucket/input.processed.txt')

    def test_s3_merge(self):
        """
        Test the s3_merge decorator.
        """
        self.P.configure_s3()  # Ensure S3 configuration is set up

        @self.P.s3_merge(["my-bucket/file1.txt", "my-bucket/file2.txt"], "my-bucket/merged.txt")
        def merge_files(infiles, outfile):
            merged_data = ''
            for infile in infiles:
                # Simulate getting the content from S3
                body = self.s3_mock.Object.return_value.get()['Body'].read().decode()
                merged_data += body

            # Mock the upload method call correctly with the filename
            self.s3_mock.Object.return_value.upload_file(merged_data, outfile)

        # Mock S3 storage for two files
        self.s3_mock.Object.return_value.get.side_effect = [
            {'Body': MagicMock(read=MagicMock(return_value=b'Hello '))},
            {'Body': MagicMock(read=MagicMock(return_value=b'World!'))}
        ]

        # Run the decorator function
        merge_files()  # This should trigger the decorator handling internally

        # Verify the upload was called with the correct merged data and output path
        self.s3_mock.Object.return_value.upload_file.assert_called_with('Hello World!', 'my-bucket/merged.txt')


if __name__ == '__main__':
    unittest.main()
