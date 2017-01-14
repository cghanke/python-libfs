#!/usr/bin/python3

"""
Test the creation of a libfs-db
"""

import subprocess
import unittest

LIBFS_BIN = "./libfs.py"
LIBFS_SRC_DIR = "./test/data/src"
LIBFS_DB = "./test/data/testdb"
STARTUP_TIME = 0.5
LIBFS_LOG_CFG = "./test/logging.cfg"

class TestBase:
    
        TYPE ="N/A"
        
        def test_creation(self):
            with subprocess.Popen([LIBFS_BIN, "--logconf", LIBFS_LOG_CFG, "update", "--type",  self.TYPE,  LIBFS_SRC_DIR, LIBFS_DB], stderr=subprocess.PIPE,  stdout=subprocess.PIPE) as proc:
                output,  outerr=proc.communicate()
                self.assertEqual(proc.poll(), 0,  msg="%s, %s" % (output,  outerr))

class ID3Test(TestBase, unittest.TestCase):
    TYPE = "id3"
    
if __name__ == "__main__": 
    unittest.main()
