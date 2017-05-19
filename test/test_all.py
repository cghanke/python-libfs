"""
Tests integrity of a libfs-Filesystem
It uses the data in testdata/
to mount a libfs under testmnt/ with a copy of testdata/test.db as db
"""

import unittest

from test.test_id3 import ID3Test
from test.test_exif import EXIFTest

if __name__ == "__main__":
    unittest.main()
