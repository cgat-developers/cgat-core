"""Test cases for the CGATCore.IOTools module."""

import unittest
import os
import shutil
import tempfile
import time
import CGATCore.IOTools as IOTools


class TestIOToolsTouchFile(unittest.TestCase):

    basename = "test_iotools_touch_file.txt"

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir,
                                     self.basename)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_touch_file_creates_empty_file(self):
        self.assertFalse(os.path.exists(self.filename))
        IOTools.touch_file(self.filename)
        self.assertTrue(os.path.exists(self.filename))
        if self.filename.endswith(".gz"):
            self.assertFalse(IOTools.is_empty(self.filename))
        else:
            self.assertTrue(IOTools.is_empty(self.filename))

        with IOTools.open_file(self.filename) as inf:
            data = inf.read()
        self.assertEqual(len(data), 0)

    def test_touch_file_updates_existing_file(self):
        with IOTools.open_file(self.filename, "w") as outf:
            outf.write("some data\n")
        created = os.stat(self.filename).st_mtime
        time.sleep(1)
        IOTools.touch_file(self.filename)
        modified = os.stat(self.filename).st_mtime
        self.assertGreater(modified, created)
        with IOTools.open_file(self.filename) as inf:
            data = inf.read()
        self.assertEqual(data, "some data\n")


class TestIOToolsTouchFileCompressed(TestIOToolsTouchFile):

    basename = "test_iotools_touch_file.txt.gz"


if __name__ == "__main__":
    unittest.main()
