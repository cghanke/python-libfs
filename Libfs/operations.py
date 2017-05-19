"""
Operations class containing the RequestHandlers for llfuse
"""
import os
import sys
import llfuse
import logging
import errno
from time import localtime, mktime
from llfuse import FUSEError
from os import fsencode, fsdecode
from Libfs.misc import calltrace_logger, get_vpath_list
from Libfs.cache import Memcache
from Libfs.business_logic import BusinessLogic

LOGGER = logging.getLogger(__name__)

class Operations(llfuse.Operations):
    """
    contains request handlers used by llfuse
    """

    @calltrace_logger
    def __init__(self, library, mountpoint, current_view_name):
        """
        set basic config
        """
        super().__init__()
        self.mountpoint = mountpoint
        # we need to get that one here, since accessing anything sth. inside the libfs
        # will deadlock
        self.mountpoint_parent = os.path.dirname(mountpoint)
        self.business_logic = BusinessLogic(library, None, current_view_name)
        self.cache = Memcache()
        self.business_logic.generate_vtree()
        self._pinode_fn2srcpath_map = {}
        self.vdir_stat = llfuse.EntryAttributes()
        self.lib_stat = os.lstat(library)
        # set times
        # mtime from mounting
        self.vdir_stat.st_atime_ns = int(mktime(localtime()) * 10**9)
        # ctime and mtime from lib-file
        self.vdir_stat.st_ctime_ns = self.lib_stat.st_ctime_ns
        self.vdir_stat.st_mtime_ns = self.lib_stat.st_mtime_ns
        # other standard-entries
        self.vdir_stat.generation = 0
        self.vdir_stat.attr_timeout = 5
        self.vdir_stat.entry_timeout = 5
        self.vdir_stat.st_blksize = 512
        self.vdir_stat.st_blocks = 666
        self.vdir_stat.st_gid = os.getgid()
        self.vdir_stat.st_mode = 16877
        self.vdir_stat.st_uid = os.getuid()

    @calltrace_logger
    def lookup(self, parent_inode, name, ctx=None):
        """
        Lookup request handler
        """
        name = fsdecode(name)
        LOGGER.debug('lookup: for %s in %d', name, parent_inode)
        full_path = os.path.join(self.cache.get_path_by_inode(parent_inode), name)
        LOGGER.debug('lookup: path = %s', full_path)
        LOGGER.debug('self._pinode_fn2srcpath_map = %s', self._pinode_fn2srcpath_map)
        if not self.business_logic.is_vdir(full_path):
            try:
                src_path = self._pinode_fn2srcpath_map[parent_inode][name]
                attr = self._get_src_attr(src_path)
            except KeyError:
                # we need to create our _pinode_fn2srcpath_map-cache for this parent_inode
                self._readdir(parent_inode)
                LOGGER.debug('self._pinode_fn2srcpath_map = %s', self._pinode_fn2srcpath_map)
                try:
                    src_path = self._pinode_fn2srcpath_map[parent_inode][name]
                    attr = self._get_src_attr(src_path)
                except: # now, it's really not there
                    raise FUSEError(errno.ENOENT)
        else: # is a dir
            if not self.business_logic.lookup_dir(full_path):
                raise FUSEError(errno.ENOENT)
            attr = self._get_vdir_attr(full_path)
            if name != '.' and name != '..':
                self.cache.add_inode_path_pair(attr.st_ino, full_path)
        return attr

    @calltrace_logger
    def getattr(self, inode, ctx=None):
        """
        get attribute for inode.
        we need to use inode in case the file is still
        open, but already deleted (?).
        """
        if inode in self.cache.inode2fd_map:
            path = None
            file_desc = self.cache.get_fd_by_inode(inode)
            LOGGER.debug("_getattr for file_desc %s", file_desc)
        else:
            file_desc = None
            path = self.cache.get_path_by_inode(inode)
            LOGGER.debug("getattr for path %s", path)
            # first, check if path is a virtual directory
            if self.business_logic.is_vdir(path):
                attr = self._get_vdir_attr(path)
                LOGGER.debug("_getattr: returning attr from db: %s", attr)
                return self._fill_attr_entry(attr)
        # we're dealing with a file here
        try:
            if file_desc is None:
                src_path = self.business_logic.get_srcfilename_by_srcinode(inode)
                this_stat = os.lstat(src_path)
            else:
                this_stat = os.fstat(file_desc)
        except OSError as exc:
            raise FUSEError(exc.errno)
        return self._fill_attr_entry(this_stat)

    @calltrace_logger
    def _get_vdir_attr(self, vpath):
        """
        return the attributes from a virtual directory
        """
        entry = llfuse.EntryAttributes()
        # set normal attrs of vdirs to those of mountpoint
        for attr in ('st_mode', 'st_nlink', 'st_uid', 'st_gid', 'st_rdev',
                     'st_size', 'st_atime_ns', 'st_mtime_ns', 'st_ctime_ns', 'st_blocks'):
            setattr(entry, attr, getattr(self.vdir_stat, attr))
        entry.st_ino = self.business_logic.get_vdir_inode(vpath)
        LOGGER.debug("_get_vdir_attr: returning st_ino=%s", entry.st_ino)
        return entry

    def _get_src_attr(self, src_path):
        """
        return attribute from a src file
        """
        assert not src_path.startswith(self.mountpoint)
        this_stat = os.lstat(src_path)
        return self._fill_attr_entry(this_stat)

    def _fill_attr_entry(self, stat):
        """
        fill in a llfuseEntryAttributes- object from the stat
        and some default attributed
        """
        entry = llfuse.EntryAttributes()
        for attr in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
                     'st_ctime_ns'):
            setattr(entry, attr, getattr(stat, attr))
        entry.generation = 0
        entry.entry_timeout = 5
        entry.attr_timeout = 5
        entry.st_blksize = 512
        entry.st_blocks = ((entry.st_size + entry.st_blksize-1) // entry.st_blksize)
        LOGGER.debug("_fill_attr_entry: returning inode from fs: %s", entry.st_ino)
        return entry

    @calltrace_logger
    def opendir(self, inode, ctx):
        """
        open a dir, return the inode-number as a fh
        """
        LOGGER.debug('opendir %s', inode)
        if not self.business_logic.is_vdir(self.cache.get_path_by_inode(inode)):
            raise FUSEError(errno.ENOTDIR)
        return inode

    @calltrace_logger
    def _readdir(self, inode):
        """
        readdir entries from cache.
        update cache if required.
        """
        vpath = self.cache.get_path_by_inode(inode)
        LOGGER.debug('readdir %s', vpath)
        # XXX
        # check cache first !
        entries = []
        # get files from db for this vdir
        for vnode, vname, src_path in self.business_logic.get_contents_by_vpath(vpath):
            if src_path is None:
                full_path = os.path.join(vpath, vname)
                attr = self._get_vdir_attr(full_path)
                if vnode > 0:
                    setattr(attr, 'st_ino', vnode)
                LOGGER.debug('readdir vnode %s, full_path %s, vname %s, src_path %s'\
                             ', attr.st_ino %s',
                             vnode, full_path, vname, src_path, attr.st_ino)
                entries.append((vnode, vname, attr))
            else:
                if src_path == "MOUNTPOINT_PARENT":
                    attr = self._get_src_attr(self.mountpoint_parent)
                    vnode = attr.st_ino
                else:
                    attr = self._get_src_attr(src_path)
                entries.append((vnode, vname, attr))
                try:
                    self._pinode_fn2srcpath_map[inode][vname] = src_path
                except:
                    self._pinode_fn2srcpath_map[inode] = {vname: src_path}
        for entry in entries:
            if entry[1] == "." or entry[1] == "..": continue
            this_path = os.path.join(vpath, entry[1])
            self.cache.add_inode_path_pair(entry[0], this_path)
        return entries

    @calltrace_logger
    def readdir(self, inode, off):
        """
        read dir-entries. inode should be a file-handle, but
        we just use the inode-number for now
        """
        entries = self._readdir(inode)
        LOGGER.debug('readdir entries: %s', entries)
        LOGGER.debug('readdir read %d entries, starting at %d', len(entries), off)
        LOGGER.debug('inode2vpath_map: %s', self.cache.inode2vpath_map)
        LOGGER.debug('_pinode_fn2srcpath_map: %s', self._pinode_fn2srcpath_map)

        for (ino, name, attr) in sorted(entries):
            if ino <= off:
                continue
            yield (fsencode(name), attr, ino)

    @calltrace_logger
    def rename(self, old_parent_inode, old_name, new_parent_inode, new_name, ctx):
        """
        rename only works within this filesystem.
        It changes the metadata of the file
        and updates the db respectively.
        """
        old_name = fsdecode(old_name)
        new_name = fsdecode(new_name)
        old_parent = self.cache.get_path_by_inode(old_parent_inode)
        new_parent = self.cache.get_path_by_inode(new_parent_inode)
        old_path = os.path.join(old_parent, old_name)
        new_path = os.path.join(new_parent, new_name)
        LOGGER.debug("old_path: %s, new_path:%s", old_path, new_path)

        # rename is only allowed in the same dir-level
        old_vpath_list = get_vpath_list(old_path)
        new_vpath_list = get_vpath_list(new_path)
        if len(old_vpath_list) != len(new_vpath_list):
            LOGGER.error("Rename across vdir levels not allowed.")
            # we canot use EXDEV here, because it would trigger a
            # "cp && rm" in the "mv" command.
            # so let's just choose some other weird errno
            raise FUSEError(errno.EADDRNOTAVAIL)

        # do not move a thing onto itself
        if old_vpath_list == new_vpath_list:
            raise FUSEError(errno.EINVAL)

        # are we renaming a directory or a file ?
        if self.business_logic.is_vdir(old_path): # rename a directory
            # get the key of this dir_level
            key = self.business_logic.get_key_of_vpath(old_parent)
            LOGGER.error("into db: %s = %s ", key, new_vpath_list[-1])
            # check if new_path is valid
            if not self.business_logic.metadata_plugin.is_valid_metadata(key, new_vpath_list[-1]):
                LOGGER.error("New value \"%s\" for key \"%s\" is invalid "\
                   "according to metadata_plugin.", new_vpath_list[-1], key)
                raise FUSEError(errno.EINVAL)
            self.cache.lookup_lock.acquire()
            # update all database entries
            self.business_logic.update_column(old_vpath_list, new_vpath_list)
            # update cache
            self.business_logic.generate_vtree()
            self.cache.update_maps(old_path, new_path)
            self.cache.lookup_lock.release()
        else: # rename a single file
            # get source path of file in question
            src_path = self._pinode_fn2srcpath_map[old_parent_inode][old_name]
            LOGGER.debug("rename: src_path=%s", src_path)
            # to change the name of the file, make sure it fits into the generated filename pattern.
            try:
                new_fn_metadata = self.business_logic.get_metadata_from_gen_filename(new_name)
            except:
                sys.stderr.write("rename: New name %s does not fit into the present "\
                                 "filename-scheme %s.\n" %
                                 (new_name, self.business_logic.current_view["fn_gen"]))
                raise FUSEError(errno.EINVAL)

            # get new metadata from dirs, if we not only change the filename, but also
            # the vdir
            new_metadata = self.business_logic.get_vpath_dict(new_parent)
            # add new metadata from filename
            for k in new_fn_metadata:
                new_metadata[k] = new_fn_metadata[k]

            # change metadata on file first
            try:
                self.business_logic.metadata_plugin.write_metadata(src_path, new_metadata)
            except OSError as exc:
                LOGGER.error("rename: Cannot write metadata to src-file %s. OSERROR=%s",
                             src_path, exc)
                sys.stderr.write("rename: Cannot write metadata to src-file %s. rc=%s" %
                                 (src_path, exc.errno))
                raise FUSEError(exc.errno)
            except Exception as exc:
                LOGGER.error("rename.write_metadata: failed for file %s. pluging threw %s",
                             src_path, exc)
                sys.stderr.write("rename.write_metadata: plugin returned %s" % (exc))
                if hasattr(exc, "errno"):
                    raise FUSEError(exc.errno)
                else:
                    raise FUSEError(errno.EINVAL)
            # then update in-memory cache
            self.cache.lookup_lock.acquire()

            self.business_logic.remove_entry(src_path)
            self.business_logic.add_entry(src_path, new_metadata)
            inode = self.business_logic.get_inode_by_srcfilename(src_path)
            self.cache.update_inode_path_pair(inode, new_path)
            self._pinode_fn2srcpath_map[old_parent_inode][new_name] = src_path
            del self._pinode_fn2srcpath_map[old_parent_inode][old_name]
            self.cache.lookup_lock.release()
        return

    @calltrace_logger
    def mkdir(self, parent_inode, name, mode, ctx):
        """
        a new directory means a new "." entry in the list of the present dirtree.
        """
        full_path = os.path.join(self.cache.get_path_by_inode(parent_inode), fsdecode(name))
        if not self.business_logic.is_vdir(full_path):
            raise FUSEError(errno.ENOLINK)
        # create it
        vnode = self.business_logic.mkdir(full_path)
        if vnode < 0:
            raise FUSEError(-vnode)
        self.cache.add_inode_path_pair(vnode, full_path)
        vattr = self._get_vdir_attr(full_path)
        return vattr

    @calltrace_logger
    def rmdir(self, parent_inode, name, ctx):
        """
        remove an empty dir
        """
        full_path = os.path.join(self.cache.get_path_by_inode(parent_inode), fsdecode(name))
        if not self.business_logic.is_vdir(full_path):
            raise FUSEError(errno.ENOLINK)
        self.business_logic.rmdir(full_path)
        self.cache.forget_path(parent_inode, name)
        return

    @calltrace_logger
    def open(self, inode, flags, ctx):
        """
        open a file.
        Put it in the memcache.
        Increase open count
        """
        if inode in self.cache.inode2fd_map:
            file_desc = self.cache.inode2fd_map[inode]
            self.cache.fd_open_count[file_desc] += 1
            return file_desc
        if flags & os.O_CREAT:
            raise FUSEError(errno.EROFS)
        try:
            file_desc = os.open(self.business_logic.get_srcfilename_by_srcinode(inode), flags)
        except OSError as exc:
            LOGGER.error("Cannot open %s with flags %s",
                         self.business_logic.get_srcfilename_by_srcinode(inode), flags)
            raise FUSEError(exc.errno)
        self.cache.inode2fd_map[inode] = file_desc
        self.cache.fd2inode_map[file_desc] = inode
        self.cache.fd_open_count[file_desc] = 1
        return file_desc

    @calltrace_logger
    def read(self, file_desc, offset, length):
        """
        read from a file descriptor
        """
        os.lseek(file_desc, offset, os.SEEK_SET)
        return os.read(file_desc, length)

    @calltrace_logger
    def release(self, file_desc):
        """
        Release open file.
        This method will be called when the last file descriptor of fh has been closed.
        """

        # XXX This should be removed.
        # Why ??
        if self.cache.fd_open_count[file_desc] > 1:
            self.cache.fd_open_count[file_desc] -= 1
            return

        del self.cache.fd_open_count[file_desc]
        inode = self.cache.fd2inode_map[file_desc]
        del self.cache.inode2fd_map[inode]
        del self.cache.fd2inode_map[file_desc]
        try:
            os.close(file_desc)
        except OSError as exc:
            raise FUSEError(exc.errno)

    def statfs(self, ctx):
        """
        used for e.g. "df"
        used number of indes should represent number of files etc
        """
        # XXX should be fed from the DB.
        # on mount scan the DB and put in into a global
        stat_ = llfuse.StatvfsData()
        stat_.f_bsize = 666
        stat_.f_frsize = 666
        stat_.f_blocks = 666777
        stat_.f_bfree = 777
        stat_.f_bavail = 666
        stat_.f_files = 666
        stat_.f_ffree = 777
        stat_.f_favail = 777
        return stat_
