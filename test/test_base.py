import os
import platform
import shutil
import subprocess
import time
import unittest


class TestBase(unittest.TestCase):
    """
    Base TestCase to be used by the plugin-TestCases
    """

    LIBFS_BIN = "scripts/libfs.py"
    LIBFS_MNT = "./test/mnt"
    LIBFS_LOG_CFG = "./test/logging.cfg"

    TYPE = "N/A"
    LIBFS_SRC_DIR = "N/A"
    LIBFS_DB = "N/A"
    EXISTING_FILE = "%s/NADA" % LIBFS_MNT
    EXISTING_DIR = "%s/NADA" % LIBFS_MNT
    NON_EXISTING_FILE = "%s/NON_NADA" % LIBFS_MNT
    NON_EXISTING_DIR = "%s/NON_NADA" % LIBFS_MNT

    @classmethod
    def setUpClass(cls):
        """
        create a copy of the db and mount libfs in the background
        """
        # create a database file
        try:
            # remove any previous traces
            os.unlink(cls.LIBFS_DB)
        except OSError:
            pass

        cmd_list = [cls.LIBFS_BIN, "--logconf", cls.LIBFS_LOG_CFG, "update", "--type",
                               cls.TYPE, cls.LIBFS_SRC_DIR, cls.LIBFS_DB]
        with subprocess.Popen(cmd_list, stderr=subprocess.PIPE, stdout=subprocess.PIPE) as create_db_proc:
            output, outerr = create_db_proc.communicate()
            if create_db_proc.poll():
                raise RuntimeError("Create DB command \"%s\" failed with rc=%s. output=%s, outerr=%s" %\
                                   (" ".join(cmd_list), create_db_proc.poll(), output, outerr))

        # mount libfs
        cmd_list = [cls.LIBFS_BIN, "--logconf", cls.LIBFS_LOG_CFG, "mount",
                    cls.LIBFS_DB, cls.LIBFS_MNT]
        cls.mount_proc = subprocess.Popen(cmd_list, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # check if mount worked
        time.sleep(0.5)

        if cls.mount_proc.poll() != None:
            output, outerr = cls.mount_proc.communicate()
            raise RuntimeError("Mount command \"%s\" failed with rc=%s. output=%s, outerr=%s" %\
                               (" ".join(cmd_list), cls.mount_proc.poll(), output, outerr))

    @classmethod
    def tearDownClass(cls):
        """
        unmount libfs
        """
        if platform.system() == 'Darwin':
            subprocess.check_call(['umount', '-l', cls.LIBFS_MNT])
        else:
            subprocess.check_call(['fusermount', '-z', '-u', cls.LIBFS_MNT])
        assert not os.path.ismount(cls.LIBFS_MNT)
        # close the internal fileobjects
        cls.mount_proc.communicate()
        # remove the created database file
        os.unlink(cls.LIBFS_DB)


    def test_walk(self):
        """
        just walk through the whole tree
        """
        for root, dirs, files in os.walk(self.LIBFS_MNT):
            pass

    def test_file_mv(self):
        """
        rename a file
        """
        shutil.move(self.EXISTING_FILE, self.NON_EXISTING_FILE)
        shutil.move(self.NON_EXISTING_FILE, self.EXISTING_FILE)

    def test_file_cp(self):
        """
        copy a file, expected to fail.
        Copying a file into libfs is not supported
        Likewise create.
        """
        with self.assertRaises(Exception) as excep:
            shutil.copyfile(self.EXISTING_FILE, self.NON_EXISTING_FILE)

        the_exception = excep.exception
        self.assertEqual(the_exception.errno, 38)

    def test_dir_mv(self):
        """
        Move a dir from, thus changing the metadata in the db.
        """
        shutil.move(self.EXISTING_DIR, self.NON_EXISTING_DIR)
        shutil.move(self.NON_EXISTING_DIR, self.EXISTING_DIR)

    def test_mkdir_rmdir(self):
        """
        create a dir.
        """
        os.mkdir(self.NON_EXISTING_DIR)
        os.rmdir(self.NON_EXISTING_DIR)

