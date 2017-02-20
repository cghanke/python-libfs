#!/usr/bin/python3
"""
Tests integrity of a libfs-Filesystem
It uses the data in testdata/
to mount a libfs under testmnt/ with a copy of testdata/test.db as db
"""

import os
import platform
import shutil
import subprocess
import time
import unittest

LIBFS_BIN = "scripts/libfs.py"
LIBFS_MNT = "./test/mnt"

class TestBase(unittest.TestCase) :
    
    EXISTING_FILE = "%s/NADA" % LIBFS_MNT
    EXISTING_DIR = "%s/NADA" % LIBFS_MNT
    LIBFS_SRC_DIR = "./test/data/src"
    LIBFS_DB_ORIG = "./test/data/testdb.orig"
    LIBFS_DB = "./test/data/testdb"
    LIBFS_LOG_CFG = "./test/logging.cfg"

    @classmethod
    def setUpClass(self):
        """
        create a copy of the db and mount libfs in the background
        """
        # create copy of db
        shutil.copyfile(self.LIBFS_DB_ORIG,  self.LIBFS_DB)
        # mount libfs
        self.mount_proc = subprocess.Popen([LIBFS_BIN, "--logconf", self.LIBFS_LOG_CFG, "mount", self.LIBFS_DB,  LIBFS_MNT], stderr=subprocess.PIPE,  stdout=subprocess.PIPE)
        # check if mount worked
        time.sleep(0.5)
        
        if self.mount_proc.poll() != None:
            output,  outerr = self.mount_proc.communicate()
            raise RuntimeError("Mount failed with rc=%s. output=%s, outerr=%s" % (self.mount_proc.poll(),  output,  outerr))
    
    @classmethod
    def tearDownClass(self):
        """
        unmount libfs
        """
        if platform.system() == 'Darwin':
            subprocess.check_call(['umount', '-l', LIBFS_MNT])
        else:
            subprocess.check_call(['fusermount', '-z', '-u', LIBFS_MNT])
        assert not os.path.ismount(LIBFS_MNT)
        # close the internal fileobjects
        self.mount_proc.communicate()
        
    def test_walk(self):
        """
        just walk through the whole tree
        """
        for root,  dirs,  files in os.walk(LIBFS_MNT):
            pass
    
    def test_file_mv(self):
        """
        rename a file
        """
        shutil.move(self.EXISTING_FILE,  self.NON_EXISTING_FILE)
        shutil.move(self.NON_EXISTING_FILE, self.EXISTING_FILE)

    @unittest.expectedFailure
    def test_file_cp(self):
        """
        copy a file, expected to fail
        """
        with self.assertRaises(FileNotFoundError) as cm:
            shutil.copyfile(self.EXISTING_FILE,  self.NON_EXISTING_FILE)
        
        the_exception = cm.exception
        self.assertEqual(the_exception.errno,  2)
        
    def test_dir_mv(self):
        """
        Move a dir from, thus changing the metadata in the db.
        """
        shutil.move(self.EXISTING_DIR,  self.NON_EXISTING_DIR)
        shutil.move( self.NON_EXISTING_DIR,  self.EXISTING_DIR)
    
class ID3Test(TestBase):
    LIBFS_SRC_DIR = "./test/data/src"
    LIBFS_DB_ORIG = "./test/data/testdb.orig"
    LIBFS_DB = "./test/data/testdb"
    EXISTING_FILE = "%s/A Cappella/Artist A/2000/Album A/1 -- Track A.mp3" % LIBFS_MNT
    EXISTING_DIR = "%s/A Cappella/Artist A/2000/Album A" % LIBFS_MNT
    NON_EXISTING_FILE = "%s/A Cappella/Artist A/2000/Album A/1 -- Track NON.mp3" % LIBFS_MNT
    NON_EXISTING_DIR = "%s/A Cappella/Artist A/2000/Album NON" % LIBFS_MNT

if __name__ == "__main__": 
    unittest.main()
