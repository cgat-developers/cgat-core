"""Test cases for the cgatcore.iotools module."""

import unittest
import os
import shutil
import tempfile
import time
import cgatcore.iotools as iotools


class TestiotoolsTouchFile(unittest.TestCase):

    basename = "test_iotools_touch_file.txt"

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir,
                                     self.basename)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_touch_file_creates_empty_file(self):
        self.assertFalse(os.path.exists(self.filename))
        iotools.touch_file(self.filename)
        self.assertTrue(os.path.exists(self.filename))
        if self.filename.endswith(".gz"):
            self.assertFalse(iotools.is_empty(self.filename))
        else:
            self.assertTrue(iotools.is_empty(self.filename))

        with iotools.open_file(self.filename) as inf:
            data = inf.read()
        self.assertEqual(len(data), 0)

    def test_touch_file_updates_existing_file(self):
        with iotools.open_file(self.filename, "w") as outf:
            outf.write("some data\n")
        created = os.stat(self.filename).st_mtime
        time.sleep(1)
        iotools.touch_file(self.filename)
        modified = os.stat(self.filename).st_mtime
        self.assertGreater(modified, created)
        with iotools.open_file(self.filename) as inf:
            data = inf.read()
        self.assertEqual(data, "some data\n")


class TestiotoolsTouchFileCompressed(TestiotoolsTouchFile):

    basename = "test_iotools_touch_file.txt.gz"


if __name__ == "__main__":
    unittest.main()
