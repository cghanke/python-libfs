"""
cache implementations
Based on python-llfuse/examples/passthroughfs.py
written by Nicolaus Rath
Copyright ©  Nikolaus Rath <Nikolaus.org>
"""

import logging
from llfuse import ROOT_INODE, FUSEError
from threading import Lock
from collections import defaultdict
import errno

from Libfs.misc import calltrace_logger, filename_has_duplicate_counter

LOGGER = logging.getLogger(__name__)

class Memcache:
    """
    A simple memcache to store inode to path or filedescriptor mappings
    Grows indefinetly.
    """

    @calltrace_logger
    def __init__(self):
        self.fd2inode_map = dict()
        self.inode2fd_map = dict()
        self.inode2vpath_map = {ROOT_INODE: '/'}
        self.lookup_cnt = defaultdict(lambda: 0)
        self.fd_open_count = dict()
        self.lookup_lock = Lock()

    @calltrace_logger
    def get_path_by_inode(self, inode):
        """
        return a path belonging to an inode
        """
        LOGGER.debug("get_path_by_inode: %s", self.inode2vpath_map)
        try:
            val = self.inode2vpath_map[inode]
        except KeyError:
            raise FUSEError(errno.ENOENT)
        if isinstance(val, set):
            # In case of hardlinks, pick any path
            val = next(iter(val))
        return val

    @calltrace_logger
    def get_fd_by_inode(self, inode):
        """
        return a filedeskriptor by inode number
        raise a FUSE-error if it does not exist.
        """
        LOGGER.debug("get_fd_by_inode: %s", self.inode2fd_map)
        try:
            val = self.inode2fd_map[inode]
        except KeyError:
            raise FUSEError(errno.ENOENT)
        return val

    @calltrace_logger
    def add_inode_path_pair(self, inode, path):
        """
        add an inode_path pair into the memcache
        """
        self.lookup_cnt[inode] += 1

        for _inode, _path in self.inode2vpath_map.items():
            if _path == path:
                LOGGER.debug("path %s already in cache with inode %s, got inode '%s'",
                             path, _inode, inode)
                return

        if inode not in self.inode2vpath_map:
            self.inode2vpath_map[inode] = path
        else:
            LOGGER.debug("checking if '%s' == '%s'", path, self.inode2vpath_map[inode])
            # cornercase: if have two files with identical metadata, the second has a
            # counter appended: ' (libfs:%d)'.
            # if the metadata of the first one gets changed, the counter of
            # the second disappears.
            # Therefore we need to arrange for that case.
            if path != self.inode2vpath_map[inode]:
                if filename_has_duplicate_counter(self.inode2vpath_map[inode]):
                    self.inode2vpath_map[inode] = path
            else:
                assert path == self.inode2vpath_map[inode]
            return

    @calltrace_logger
    def update_inode_path_pair(self, inode, path):
        """
        update the entry of an inode_path_pair.
        """
        LOGGER.debug(inode in self.inode2vpath_map)
        assert inode in self.inode2vpath_map
        self.inode2vpath_map[inode] = path

    @calltrace_logger
    def forget(self, inode_list):
        """
        remove a list of inodes from the cache
        """
        for (inode, nlookup) in inode_list:
            if self.lookup_cnt[inode] > nlookup:
                self.lookup_cnt[inode] -= nlookup
                continue
            LOGGER.debug('forgetting about inode %d', inode)
            # XXX We never put sth into inode2fd_map...
            assert inode not in self.inode2fd_map
            self.lookup_lock.acquire()
            # XXX this could fail if inode is not looked up? Could put it in a proper try except:
            del self.lookup_cnt[inode]
            del self.inode2vpath_map[inode]
            self.lookup_lock.release()

    @calltrace_logger
    def forget_path(self, inode, path):
        """
        called by rmdir
        """
        LOGGER.debug('forget %s for %d', path, inode)
        val = self.inode2vpath_map[inode]
        if isinstance(val, set):
            val.remove(path)
            if len(val) == 1:
                self.inode2vpath_map[inode] = next(iter(val))
        else:
            self.lookup_lock.acquire()
            # XXX this could fail if inode is not looked up? Could put it in a proper try except:
            del self.lookup_cnt[inode]
            del self.inode2vpath_map[inode]
            self.lookup_lock.release()

    @calltrace_logger
    def update_maps(self, old_path, new_path):
        """
        update all internal maps in case of a rename of a directory
        """
        # get proper difference between old and new path
        LOGGER.debug("update_maps: %s", self.inode2vpath_map)
        for inode in self.inode2vpath_map:
            LOGGER.debug("inode %s: replace %s by %s for %s",
                         inode, old_path, new_path, self.inode2vpath_map[inode])
            self.inode2vpath_map[inode] = self.inode2vpath_map[inode].replace(old_path, new_path)
        LOGGER.debug("update_maps: %s", self.inode2vpath_map)
        return
