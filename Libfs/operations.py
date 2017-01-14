import os
import sys
import llfuse
import errno
from time import localtime, mktime
from llfuse import FUSEError
from os import fsencode, fsdecode
from Libfs.misc import *
from Libfs.cache import Memcache
from Libfs.business_logic import BusinessLogic

logger = logging.getLogger(__name__)

class Operations(llfuse.Operations):

    @calltrace_logger
    def __init__(self, library,  mountpoint,  current_view_name):
        super().__init__()
        self.mountpoint = mountpoint
        self.business_logic = BusinessLogic(library, None, current_view_name)
        self.cache = Memcache()
        self.business_logic.generate_vtree()
        self._pinode_fn2srcpath_map={}
        self.vdir_stat = llfuse.EntryAttributes()
        mpt_stat = os.lstat(mountpoint)
        for attr in dir(mpt_stat):
           if attr.startswith('st_'):
               try:
                    setattr(self.vdir_stat,  attr,  getattr(mpt_stat,  attr))
               except:
                   pass
        
        lib_stat = os.lstat(library)
        # set times
        # mtime from mounting
        self.vdir_stat.st_atime_ns = int(mktime(localtime()) * 10**9)
        # ctime and mtime from lib-file
        self.vdir_stat.st_ctime_ns = lib_stat.st_ctime_ns
        self.vdir_stat.st_mtime_ns = lib_stat.st_mtime_ns
        # other standard- entries
        self.vdir_stat.generation = 0
        self.vdir_stat.entry_timeout = 5
        self.vdir_stat.attr_timeout = 5
        self.vdir_stat.st_blksize = 512
        self.vdir_stat.st_blocks = 666
        # mode is taken from mountpoint

    @calltrace_logger
    def lookup(self, inode_p, name, ctx=None):
        """
        Lookup request handler
        """
        name = fsdecode(name)
        logger.debug('lookup: for %s in %d', name, inode_p)
        full_path = os.path.join(self.cache.get_path_by_inode(inode_p), name)
        logger.debug('lookup: path = %s', full_path)
        logger.debug('self._pinode_fn2srcpath_map = %s', self._pinode_fn2srcpath_map)
        if not self.business_logic.is_vdir(full_path) :
            try :
                real_path = self._pinode_fn2srcpath_map[inode_p][name]
                attr = self._getattr(path=real_path)
            except KeyError :
                # we need to create our _pinode_fn2srcpath_map-cache for this inode_p
                # so just call readdir. 
                # it is a generator, therefore call it in a for-loop
                for gg in self.readdir(inode_p, 0) :
                    break
                logger.debug('self._pinode_fn2srcpath_map = %s', self._pinode_fn2srcpath_map)
                try :
                    real_path = self._pinode_fn2srcpath_map[inode_p][name]
                    attr = self._getattr(path=real_path)
                except : # now, it's really not there
                    raise FUSEError(errno.ENOENT)
        else : # is a dir
            if not self.business_logic.lookup_dir(full_path) :
                raise FUSEError(errno.ENOENT)
            attr = self._get_vdir_attr(full_path)
            if name != '.' and name != '..':
                self.cache.add_inode_path_pair(attr.st_ino, full_path)
        return attr
        
    @calltrace_logger
    def getattr(self, inode, ctx=None):
        """
        Entry function.
        """
        if inode in self.cache.inode2fd_map:
            return self._getattr(fd=self.cache.get_fd_by_inode(inode))
        else:
            return self._getattr(path=self.cache.get_path_by_inode(inode))
    
    @calltrace_logger
    def _get_vdir_attr(self, vpath) :
        entry = llfuse.EntryAttributes()
        # set normal attrs of vdirs to those of mountpoint 
        for attr in ('st_mode', 'st_nlink', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns', 'st_ctime_ns' ):
            setattr(entry, attr, getattr(self.vdir_stat, attr))

        entry.st_ino = self.business_logic.get_dir_vnode(vpath)
        
        logger.debug("_get_vdir_attr: returning st_ino=%s", entry.st_ino)
        return entry
        
    @calltrace_logger
    def _getattr(self, path=None, fd=None):
        assert fd is None or path is None
        assert not(fd is None and path is None)
        if not path is None:
            logger.debug("_getattr for path %s" , path)
            # first, check if path is a virtual directory
            if self.business_logic.is_vdir(path) :
                attr = self._get_vdir_attr(path) 
                logger.debug("_getattr: returning inode from db : %s", attr.st_ino)
                return attr
        else :
            # fd always points to a file
            logger.debug("_getattr for fd %s" , fd)
        # we're dealing with a file here
        entry = llfuse.EntryAttributes()
        try:
            if fd is None:
                stat = os.lstat(path)
            else:
                stat = os.fstat(fd)
        except OSError as exc:
            raise FUSEError(exc.errno)

        for attr in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
                     'st_ctime_ns'):
            setattr(entry, attr, getattr(stat, attr))
        entry.generation = 0
        entry.entry_timeout = 5
        entry.attr_timeout = 5
        entry.st_blksize = 512
        entry.st_blocks = ((entry.st_size + entry.st_blksize-1) // entry.st_blksize)
        logger.debug("_getattr: returning inode from fs : %s", entry.st_ino)
        return entry

    @calltrace_logger
    def opendir(self, inode, ctx):
        """
        opena a dir 
        """
        logger.debug('opendir %s' , inode)
        if not self.business_logic.is_vdir(self.cache.get_path_by_inode(inode)) :
            raise FUSEError(errno.ENOTDIR)
        return inode

    @calltrace_logger
    def readdir(self, inode, off):
        path = self.cache.get_path_by_inode(inode)
        logger.debug('readdir %s, off %s' , path, off)
        entries = []
        # get files from db for this vdir
        for vnode, vname, src_path in self.business_logic.get_contents_by_vpath(path) :         
            if src_path is None :
                full_path = os.path.join(path,  vname)
                attr = self._get_vdir_attr(full_path)
                if vnode > 0 :
                    setattr(attr, 'st_ino', vnode)
                logger.debug('readdir vnode %s, full_path %s, vname %s, src_path %s, attr.st_ino %s', vnode, full_path,  vname, src_path, attr.st_ino)
                entries.append((vnode, vname, attr))
            else :
                if src_path == "MOUNTPOINT_PARENT" :
                    attr = self._getattr(path="/")
                    vnode = attr.st_ino
                else :
                    attr = self._getattr(path=src_path)
                entries.append((vnode, vname, attr))
                try :
                    self._pinode_fn2srcpath_map[inode][vname] = src_path
                except :
                    self._pinode_fn2srcpath_map[inode] = {vname : src_path}
            
        for entry in entries:
            if entry[1] == "." or entry[1] == ".." : continue
            this_path = os.path.join(path,  entry[1])
            self.cache.add_inode_path_pair(entry[0], this_path)
        logger.debug('readdir entries : %s', entries)
        logger.debug('readdir read %d entries, starting at %d', len(entries), off)
        logger.debug('inode2path_map: %s', self.cache.inode2path_map)
        logger.debug('_pinode_fn2srcpath_map: %s', self._pinode_fn2srcpath_map)

        for (ino, name, attr) in sorted(entries):
            if ino <= off:
                continue
            yield (fsencode(name), attr, ino)

    @calltrace_logger
    def rename(self, old_inode_p, old_name, new_inode_p, new_name, ctx):
        """
        rename only works within this filesystem.
        It changes the metadata of the file 
        and updates the db respectively.
        """
        old_name = fsdecode(old_name)
        new_name = fsdecode(new_name)
        old_parent = self.cache.get_path_by_inode(old_inode_p)
        new_parent = self.cache.get_path_by_inode(new_inode_p)
        old_path = os.path.join(old_parent, old_name)
        new_path = os.path.join(new_parent, new_name)
        logger.debug("old_path: %s, new_path:%s",  old_path,  new_path)
      
        # rename is only allowed in the same dir-level
        old_vpath_list = get_vpath_list(old_path)
        new_vpath_list = get_vpath_list(new_path)
        if len(old_vpath_list) !=  len(new_vpath_list) :
                logger.error("Rename across vdir levels not allowed.")
                # we canot use EXDEV here, because it would trigger a 
                # "cp && rm" in the "mv" command.
                # so let's just choose some other weird errno
                raise FUSEError(errno.EADDRNOTAVAIL)
        
        # do not move a thing onto itself
        if (old_vpath_list == new_vpath_list):
            raise FUSEError(errno.EINVAL)
        
        # are we renaming a directory or a file ?
        if self.business_logic.is_vdir(old_path) : # rename a directory
            # get the key of this dir_level
            key = self.business_logic.get_key_of_vpath(old_parent)
            logger.error("into db: %s = %s ",  key,  new_vpath_list[-1])
            # check if new_path is valid
            if not self.business_logic.metadata_plugin.is_valid_metadata(key,  new_vpath_list[-1]):
                logger.error("New value \"%s\" for key \"%s\" is invalid according to metadata_plugin.",  new_vpath_list[-1],  key )
                raise FUSEError(errno.EINVAL)
            self.cache.lookup_lock.acquire()
            # update all database entries
            self.business_logic.update_column(old_vpath_list,  new_vpath_list)
            # update cache
            self.business_logic.generate_vtree()
            self.cache.update_maps(old_path,  new_path)
            self.cache.lookup_lock.release()
        else : # rename a single file
            # get source path of file in question
            src_path = self._pinode_fn2srcpath_map[old_inode_p][old_name]
            logger.debug("rename: src_path=%s", src_path) 
            # to change the name of the file, make sure it fits into the generated filename pattern.
            try : 
                new_fn_metadata = self.business_logic.get_metadata_from_gen_filename(new_name)
            except :
                sys.stderr.write("rename: New name %s does not fit into the present filename-scheme %s.\n" % (new_name, self.business_logic.current_view["fn_gen"]))
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
                sys.stderr.write("rename: Cannot write metadata to src-file %s. rc=%s" % (src_path, exc.errno))
                raise FUSEError(exc.errno)
            except Exception as exc:
                sys.stderr.write("rename.write_metadata : plugin returned %s" % (exc))
                if hasattr(exc, "errno") :
                    raise FUSEError(exc.errno)
                else :
                    raise FUSEError(errno.EINVAL)
            # then update in-memory cache 
            self.cache.lookup_lock.acquire()        
             
            self.business_logic.remove_entry(src_path)
            self.business_logic.add_entry(src_path, new_metadata)
            inode = self.business_logic.get_inode_by_srcfilename(src_path)
            self.cache.update_inode_path_pair(inode,  new_path)
            self._pinode_fn2srcpath_map[old_inode_p][new_name] = src_path
            del(self._pinode_fn2srcpath_map[old_inode_p][old_name])
            self.cache.lookup_lock.release()
        return

    @calltrace_logger
    def mkdir(self, inode_p, name, mode, ctx):
        """
        a new directory means a new "." entry in the list of the present dirtree.
        """
        full_path = os.path.join(self.cache.get_path_by_inode(inode_p), fsdecode(name))
        if not self.business_logic.is_vdir(full_path) :
            raise FUSEError(errno.ENOLINK)
        # create it 
        vnode = self.business_logic.mkdir(full_path)
        if vnode < 0 :
            raise FUSEError(-vnode)
        self.cache.add_inode_path_pair(vnode, full_path)
        vattr = self._get_vdir_attr(full_path)
        return vattr

    @calltrace_logger
    def open(self, inode, flags, ctx):
        if inode in self.cache.inode2fd_map:
            fd = self.cache.inode2fd_map[inode]
            self.cache.fd_open_count[fd] += 1
            return fd
        if flags & os.O_CREAT :
            raise FUSEError(errno.EROFS)
        try:
            fd = os.open(self.business_logic.get_srcfilename_by_inode(inode), flags)
        except OSError as exc:
            logger.error("Cannot open %s with flags %s", self.business_logic.get_srcfilename_by_inode(inode), flags )
            raise FUSEError(exc.errno)
        self.cache.inode2fd_map[inode] = fd
        self.cache.fd2inode_map[fd] = inode
        self.cache.fd_open_count[fd] = 1
        return fd

    @calltrace_logger
    def read(self, fd, offset, length):
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    @calltrace_logger
    def release(self, fd):
        if self.cache.fd_open_count[fd] > 1:
            self.cache.fd_open_count[fd] -= 1
            return

        del self.cache.fd_open_count[fd]
        inode = self.cache.fd2inode_map[fd]
        del self.cache.inode2fd_map[inode]
        del self.cache.fd2inode_map[fd]
        try:
            os.close(fd)
        except OSError as exc:
            raise FUSEError(exc.errno)
    
    def statfs(self, ctx):
        """
        used for e.g. "df"
        used number of indes should represent number of files etc
        """
        # XXX disabled for now
        raise FUSEError(errno.ENOSYS)
        stat_ = llfuse.StatvfsData()
        for attr in ('f_bsize', 'f_frsize', 'f_blocks', 'f_bfree', 'f_bavail',
                     'f_files', 'f_ffree', 'f_favail'):
            setattr(stat_, attr, getattr(statfs, attr))
        return stat_


@calltrace_logger
def main():
    parser = get_default_parser()
    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--view', type=str,
                        help='CSV-string defining the directory structure')
    parser.add_argument('--debug_fuse', action='store_true', 
                        help='debug fuse')
    
    options = parser.parse_args(sys.argv[1:])
    if options.logconf:
        init_logging(options.logconf)
    if options.debug_fuse :
            fuse_options.add('debug') 
    
    bl = BusinessLogic(options.library, None, options.view)
    logger.debug('Mounting...')
    fuse_options = set(llfuse.default_options)
    fuse_options.add('fsname=libraryfs')
    fuse_options.add('default_permissions')
    
    operations = Operations(options.source, options.mountpoint, bl)
    llfuse.init(operations, options.mountpoint, fuse_options)

    try:
        logger.debug('Entering main loop..')
        llfuse.main(workers=1)
    except:
        llfuse.close(unmount=False)
        raise

    logger.debug('Umounting..')
    llfuse.close()
