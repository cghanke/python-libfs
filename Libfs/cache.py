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

from Libfs.misc import calltrace_logger

logger = logging.getLogger(__name__)

class Memcache:

    @calltrace_logger
    def __init__(self):
        self.fd2inode_map = dict()
        self.inode2fd_map = dict()
        self.inode2vpath_map = { ROOT_INODE: '/'}
        self.lookup_cnt = defaultdict(lambda : 0)
        self.fd_open_count = dict()
        self.lookup_lock = Lock()

    @calltrace_logger
    def get_path_by_inode(self, inode):
        logger.debug("get_path_by_inode: %s" % self.inode2vpath_map)
        try:
            val = self.inode2vpath_map[inode]
        except KeyError:
            raise FUSEError(errno.ENOENT)
        if isinstance(val, set):
            # In case of hardlinks, pick any path
            val = next(iter(val))
        return val
    
    @calltrace_logger
    def get_fd_by_inode(self,  inode):
        logger.debug("get_path_by_inode: %s" % self.inode2fd_map)
        try:
            val = self.inode2fd_map[inode]
        except KeyError:
            raise FUSEError(errno.ENOENT)
        return val
    
    @calltrace_logger    
    def add_inode_path_pair(self, inode, path):
        self.lookup_cnt[inode] += 1
        
        for _inode,  _path in self.inode2vpath_map.items():
            if _path == path :
                logger.debug("path %s already in cache with inode %s, got inode '%s'",  path, _inode,  inode )
                return
            
        if inode not in self.inode2vpath_map:
            self.inode2vpath_map[inode] = path
        else :
            logger.debug("checking if '%s' == '%s'",  path, self.inode2vpath_map[inode] )
            assert (path == self.inode2vpath_map[inode] )
            return
    
    @calltrace_logger    
    def update_inode_path_pair(self,  inode,  path):
        logger.debug(inode in self.inode2vpath_map)
        assert((inode in self.inode2vpath_map))
        self.inode2vpath_map[inode]  = path
        
    @calltrace_logger
    def forget(self, inode_list):
        for (inode, nlookup) in inode_list:
            if self.lookup_cnt[inode] > nlookup:
                self.lookup_cnt[inode] -= nlookup
                continue
            logger.debug('forgetting about inode %d', inode)
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
        logger.debug('forget %s for %d', path, inode)
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
    def update_maps(self,  old_path ,  new_path):
        """
        update all internal maps in case of a rename of a directory
        """
        # get proper difference between old and new path
        logger.debug("update_maps: %s" % self.inode2vpath_map)
        for inode in self.inode2vpath_map:
            logger.debug("inode %s: replace %s by %s for %s",  inode,  old_path,  new_path, self.inode2vpath_map[inode] )
            self.inode2vpath_map[inode] = self.inode2vpath_map[inode].replace(old_path,  new_path)
        logger.debug("update_maps: %s" % self.inode2vpath_map)
        return
