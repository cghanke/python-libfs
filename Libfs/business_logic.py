"""
business-logic for libfs

The database contains 3 tables:
"views", "files" and "defaults".
"views" defines how the vitrual directory structure is created
"files" stores the actual information.

views has three columns:
view_name, directory_structure, filename_generator

- view_name is a string-identifier
- directory_structure is a csv-list in which order the metadata
are used to create the directory-structure.
- filename_generator is string, where %{key} is replaced by the corresponding
metadata, e.g. %{src_filename} just passes the original filename
whereas %{title} uses the title field.
"""

import errno
import logging
import os
import re

from importlib import import_module

from Libfs.misc import calltrace_logger,  get_vpath_list
import json
import sys

logger = logging.getLogger(__name__)

class BusinessLogic:
    """
    Accessing the actual DB for the library.
    This one is sqlite, but could be replaced by others.
    """
    FILES_TABLE = "files"
    VTREE_TABLE = "trees"
    VIEWS_TABLE = "views"
    MAGIX_TABLE = "defaults"
    MAGIX_FIELD = "json"
    MAGIC_KEYS = ["valid_keys", "default_view"]
    DEFAULT_VIEW_NAME = "default"
    SRC_FILENAME_KEY = "src_filename"
    SRC_INODE_KEY = "src_inode"
    UNKNOWN = "Unknown"

    @calltrace_logger
    def __init__(self, db_connection, magix=None, current_view_name=None):
        """
        opens a sqlite file, does some checks,
        creates tables if required.
        """
        try:
            db_type,  user, password, host, database = re.match('(\S+)://(?:(.*?):(.*?))?(?:@(.*?)/)?(.*)', db_connection).groups()
        except:
            # make sqlite3 with path the default
            if os.path.exists(db_connection) or os.path.isdir(os.path.dirname(db_connection)) :
                db_type = "sqlite3"
                user = host = password = None
                database = db_connection
            else:
                sys.stderr.write("Cannot parse db-connection string.\n" )
                sys.exit(1)
    
        try:
            db_module =  import_module("Libfs.%s_be" % db_type)
            self.DB_BE = db_module.db_backend()
        except:
            sys.stderr.write("Sorry, database type %s not supported.\n" % db_type)
            sys.exit(2)
        try:
            self.DB_BE.open(user, password, host, database)
            self.check_db()
            do_setup_db = False
        except: # setup a new db
            assert(current_view_name in [self.DEFAULT_VIEW_NAME,  None])
            assert(magix != None)
            do_setup_db = True
        
        if magix is None:
            self.magix = self.get_magix_from_db()
        else:
            self.magix = magix
            
        if current_view_name is None:
            self.current_view_name = self.DEFAULT_VIEW_NAME 
            self.current_view = self.magix["default_view"]
        else:
            self.current_view_name = current_view_name
            self.current_view = self.get_view(self.current_view_name)

        self.setup_filename_parsing() 
        
        if do_setup_db:
            self.setup_db()
 
        
        self.metadata_plugin = import_module("Libfs.plugins.%s" % (self.magix["plugin"]))
        
        self.ordered_files_keys = self.DB_BE. get_columns(self.FILES_TABLE )

        self.check_tables()
        logger.debug("init: self.current_view = %s" % self.current_view)
        
        self.max_dir_level = len(self.current_view["dirtree"])

        # in-memory cache for bookkeeping 
        self.vdirs = []
        self.vtree = self.generate_vtree()
        # still in operations
        # pinode_fn2srcpath_map
        
    @calltrace_logger
    def lookup_dir(self, vpath) :
        """
        use vtree
        """
        @calltrace_logger
        def do_lookup_dir(vtree, vpath_list, result) :
            logger.debug("do_lookup_dir: vtree=%s vpath_list=%s, result=%s", vtree, vpath_list, result)
            if type(vpath_list) == type(""):
                logger.debug("do_lookup_dir: encountered string.-> returning true")
                return result
            if len(vpath_list) == 0:
                logger.debug("do_lookup_dir: vpath_list empty.")
                return True
            if vpath_list[0] in vtree.keys():
                return  do_lookup_dir(vtree[vpath_list[0]], vpath_list[1:], result)
            else :
                return False
    
        if do_lookup_dir(self.vtree, get_vpath_list(vpath), True) :
            result = self.get_dir_vnode(vpath)
        else :
            result = False
        return result

    @calltrace_logger
    def seek_vtree(self, vpath="", vpath_list=[]) :
        def do_seek_vtree(vtree, vpath_list) :
            if len(vpath_list) == 0 :
                return vtree
            if vpath_list[0] in vtree.keys() :
                return do_seek_vtree(vtree[vpath_list[0]], vpath_list[1:]) 
            else :
                sys.stderr.write("Internal Error: cannot find %s in vtree %s.\n" % (vpath_list,  vtree) )
                return False

        if vpath != "" :
            vpath_list = get_vpath_list(vpath)
        if vpath_list == [] :
            return self.vtree    

        if len(vpath_list) > self.max_dir_level :
            raise RuntimeError("seek_vtree: vpath_list=%s, self.max_dir_level=%s called on leaf-level", vpath_list, self.max_dir_level)
        result = do_seek_vtree(self.vtree, vpath_list)
        if not result :
            sys.stderr.write("Internal Error: cannot find %s or %s in vtree.\n" % (vpath,  vpath_list) )
        return result

    @calltrace_logger
    def mkdir(self, vpath) :
        """
        add dir to vtree. It stays only in memory and will only be commited to the
        db whenever a file is actually moved there.
        """
        vpath_list = get_vpath_list(vpath)
        dir_level = len(vpath_list) - 1
        logger.debug("mkdir: vpath_list=%s, dir_level=%s", vpath,  dir_level) 
        # check validity of new metadata
        key = self.current_view["dirtree"][dir_level]
        value = vpath_list[-1]
        logger.debug("checking key=\"%s\", value=\"%s\"",  key,  value)
        if not self.metadata_plugin.is_valid_metadata(key, value) :
            return -errno.EINVAL
        # add this directory in the in-memory structures
        logger.debug("vpath_list[:-1]=%s",  vpath_list[:-1])
        logger.debug("vtree=%s",  self.vtree)
        this_vtree = self.seek_vtree(vpath_list=vpath_list[:-1])
        vnode = self.get_dir_vnode(vpath)
        this_vtree[vpath_list[-1]] = {}
        return vnode
    
    @calltrace_logger
    def get_dir_vnode(self, canon_path) :
        """
        put vpath in a cache 
        """
        if not canon_path in self.vdirs :
            self.vdirs.append(canon_path)
        vnode = self.vdirs.index(canon_path) + 1
        return vnode 

    @calltrace_logger
    def walk_vtree(self, node) :
        """ 
        iterate tree in pre-order depth-first search order
        """
        yield node
        for child in node.children:
            for n in self.walk_vtree(child):
                yield n

    @calltrace_logger
    def get_magix_from_db(self):
        """
        read the magic constants from the db
        """
        res = self.DB_BE.execute_statment("select %s from %s" % (self.MAGIX_FIELD, self.MAGIX_TABLE))
        res = res[0][0]
        magix = json.loads(res)
        logger.debug("magix=%s" % magix)
        return magix

    @calltrace_logger
    def setup_db(self):
        """
        creates a new db.
        Creates tables "views" and "files"
        Sets defaults views.
        """
 
        self.DB_BE.execute_statment("create table %s ('%s' text)"  %  (self.MAGIX_TABLE, self.MAGIX_FIELD))
        self.DB_BE.execute_statment("insert into %s (%s) values('%s')" % (self.MAGIX_TABLE, self.MAGIX_FIELD, json.dumps(self.magix)))
        self.DB_BE.execute_statment("create table %s (name varchar unique, json text)" %
                              (self.VIEWS_TABLE))
        self.DB_BE.execute_statment("insert into %s (name, json) values ('%s', '%s')" %
                              (self.VIEWS_TABLE, self.DEFAULT_VIEW_NAME, json.dumps(self.current_view)))
        self.DB_BE.execute_statment("create table %s (%s varchar unique, %s integer unique, %s)" %
                              (self.FILES_TABLE, self.SRC_FILENAME_KEY, self.SRC_INODE_KEY, ",".join(self.magix["valid_keys"])))
        self.DB_BE.commit()
        return

    @calltrace_logger
    def generate_vtree(self) :
        """
        vtree is a dict representing the tree in the current view
        """
        def build_dict(vtree, tpl) :
            logger.debug("build_vtree : %s %s", vtree, tpl)
            if len(tpl) == 0 :
                return 
            if not tpl[0] in vtree.keys() :
                vtree[tpl[0]] = {}
            build_dict(vtree[tpl[0]], tpl[1:])
            return vtree
            
        self.vtree = {}
        res = self.DB_BE.execute_statment("SELECT DISTINCT %s from %s;" % (",".join(self.current_view["dirtree"]), self.FILES_TABLE))
        for tpl in res:
            self.vtree = build_dict(self.vtree, tpl)
        return 

    @calltrace_logger
    def check_db(self):
        """
        checks views for consistency.
        """
        # get tables
        res = self.DB_BE.execute_statment("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [tpl[0] for tpl in res]
        for _tab in [self.VIEWS_TABLE, self.FILES_TABLE]:
            if not _tab in tables:
                sys.stderr.write("Internal Error: Table %s does not exist in library %s.\nDelete and recreate the library.\n" % (self.VIEWS_TABLE, self.FILES_TABLE))
                sys.exit(1)
        return
        
    @calltrace_logger
    def check_tables(self):
        # check FILES_TABLE for valid_keys
        for k in [self.SRC_FILENAME_KEY, self.SRC_INODE_KEY]:
            if not k in self.ordered_files_keys:
                sys.stderr.write("Internal Error: Mandatory key %s does not exist in library %s.\nDelete and recreate the library.\n" % (k, self.db_path))
                sys.exit(1)

        logger.debug("self.ordered_keys=%s" % self.ordered_files_keys)
        logger.debug("self.magix[valid_keys]=%s" % self.magix["valid_keys"])
        for k in self.ordered_files_keys:
            if k in [self.SRC_FILENAME_KEY, self.SRC_INODE_KEY]:
                continue
            if not k in self.magix["valid_keys"]:
                sys.stderr.write("Internal Error: Key %s is not valid.\nDid you choose the right library-type ?\n Otherwise delete and recreate the library.\n" % (k))
                sys.exit(1)

        for k in self.magix["valid_keys"]:
            if not k in self.ordered_files_keys:
                sys.stderr.write("Internal Error: Valid key %s does not exist in library %s.\nDid you choose the right library-type ?\n Otherwise delete and recreate the library.\n" % (k, self.db_path))
                sys.exit(1)
        return

    @calltrace_logger
    def is_vdir(self, path) :
        """
        return true if we have a virtual directory
        """
        logger.debug("is_vdir, got '%s'" % path)
        # path must be canonicalized : start with a single /
        vpath_list = get_vpath_list(path)
        if len(vpath_list) > self.max_dir_level :
            return False
        logger.debug("is_vdir: returning True")
        return True

    @calltrace_logger
    def get_key_of_vpath(self,  vpath):
       """
       return the db field-name of this vpath
       """ 
       vpath_list = get_vpath_list(vpath)
       return self.current_view["dirtree"][len(vpath_list)]
    
    @calltrace_logger
    def add_entry(self, src_filename, metadata):
        """
        Adds a file-entry.
        """
        src_statinfo = os.stat(src_filename)
        metadata[self.SRC_FILENAME_KEY] = src_filename
        metadata[self.SRC_INODE_KEY] = src_statinfo.st_ino
        logger.debug("metadata=%s", metadata)
        values = [ "%s" % metadata.get(k, self.UNKNOWN) for k in self.ordered_files_keys ]
        # changing a list within a loop over itself,
        # huuu, but this should work
        for i, item  in enumerate(values):
            if len(item) == 0:
                values[i] = self.UNKNOWN
        values_param_str = ",".join(["?" for x in values])

        try:
            logger.debug("ordered_files_keys = %s" % self.ordered_files_keys)
            query_str = "INSERT INTO %s VALUES (%s)" % (self.FILES_TABLE, values_param_str)
            self.DB_BE.execute_statment(query_str,  *values)
        except self.DB_BE.IntegrityError:
            update_str = ""
            for k in self.ordered_files_keys:
                update_str += "%s=?, " % (k)
            update_str = update_str[:-2]
            query_str = "UPDATE %s SET %s WHERE src_filename=?" % (self.FILES_TABLE, update_str)
            self.DB_BE.execute_statment(query_str,  *values,  src_filename)
        self.DB_BE.commit()
        # revert modifications to metadata
        metadata.pop(self.SRC_FILENAME_KEY)
        metadata.pop(self.SRC_INODE_KEY)
        return

    @calltrace_logger
    def remove_entry(self, src_filename):
        """
        removes a file-entry
        """
        self.DB_BE.execute_statment("DELETE from %s WHERE src_filename='%s'" % (self.FILES_TABLE, src_filename))
        self.DB_BE.commit()
        return

    @calltrace_logger
    def get_entry(self, src_filename):
        """
        returns the metadata to a src_filename
        """
        try:
            query_str = "SELECT %s FROM %s WHERE src_filename=?;" % (",".join(["?" for x in self.ordered_files_keys]),   self.FILES_TABLE,  src_filename)
            res = self.DB_BE.execute_statment( query_str, self.ordered_files_keys, src_filename)
            return res[0]
        except IndexError:
            return None

    @calltrace_logger
    def update_column(self,  old_vpath_list,  new_vpath_list):
        """
        when renaming a vdir, we have to update all concerned rows
        """
        assert(len(old_vpath_list) ==  len(new_vpath_list))
        assert(old_vpath_list != new_vpath_list)
        where = ""
        update = ""
        for i, old_item in enumerate(old_vpath_list) :
            new_item = new_vpath_list[i]
            logger.debug("comparing old %s to new %s",  old_item,  new_item)
            where += "%s='%s' AND " % (self.current_view["dirtree"][i], old_item) 
            if old_item != new_item:
                update += "%s='%s', " % (self.current_view["dirtree"][i], new_item)
        where = where[:-len("AND ")]
        update = update[:-len(", ")]    
        self.DB_BE.execute_statment("UPDATE %s set %s WHERE %s" % (self.FILES_TABLE, update,  where))
        self.DB_BE.commit()
        return
        
    @calltrace_logger
    def get_view(self, view_name):
        """
        returns the order in which virtual directories
        are created.
        """
        try:
            res = self.DB_BE.execute_statment("select name, json from %s WHERE name='%s';" % (self.VIEWS_TABLE, view_name))
            return json.loads(res[0])
        except IndexError:
            return None
        return

    @calltrace_logger
    def set_view(self, view_name, view):
        """
        sets directory creation order
        """
        # check if dirtree is valid
        for subdir in view["dirtree"]:
            if not subdir in self.magix["valid_keys"]:
                raise RuntimeError("set_view: Key %s is not valid." % subdir)
        self.DB_BE.execute_statment("insert into %s (name, json) values (%s, '%s')" % (self.VIEWS_TABLE, view_name, json.dumps(view)))
        self.DB_BE.commit()
        return

    @calltrace_logger
    def get_all_src_names(self):
        """
        return list of all src_names in db
        """
        res = self.DB_BE.execute_statment("SELECT %s FROM %s;" % (self.SRC_FILENAME_KEY,  self.FILES_TABLE))
        return [tpl[0] for tpl in res]
    
    @calltrace_logger
    def get_srcfilename_by_inode(self,  inode):
        """
        return src_filename
        """
        res = self.DB_BE.execute_statment("SELECT %s FROM %s WHERE %s=%s;" % (self.SRC_FILENAME_KEY,  self.FILES_TABLE,  self.SRC_INODE_KEY,  inode))
        return res[0][0]

    @calltrace_logger
    def get_vpath_dict(self,  vpath) :
        """
        get a dict view_level = path
        """
        vpath_dict = {}
        vpath_list = get_vpath_list(vpath) 
        for i, item in enumerate(vpath_list) :
            vpath_dict[self.current_view["dirtree"][i]] = item
        logger.debug("get_vpath_dict: %s -> %s", vpath, vpath_dict)
        return vpath_dict

    #
    # actually used for FUSE
    #

    def get_gen_filename(self, src_filename):
        """
        generate a virtual filename
        """
        gen_fn = self.current_view["fn_gen"]
        gen_fn = gen_fn.replace("%{src_filename}", os.path.basename(src_filename))
        query_str = "SELECT %s FROM %s WHERE src_filename=?;" % ( ",".join(self.magix["valid_keys"]), self.FILES_TABLE)
        res = self.DB_BE.execute_statment( query_str,  src_filename)
        all_file_keys = res[0]
        logger.debug("get_gen_filename src_filename:%s all_file_keys:%s", src_filename, all_file_keys)
        for i, item in enumerate(self.magix["valid_keys"]) :
            gen_fn = gen_fn.replace("%%{%s}" % (item), all_file_keys[i])
        return gen_fn

    def setup_filename_parsing(self) :
        """
        return a metadata-dict from a virtual filename
        """
        i=0
        inside_key=False
        this_key=""
        self.fn_gen_keys = []
        fn_generator = self.current_view["fn_gen"]
        RX = ""
        # create a regex and key mapping  
        while i < len(fn_generator) :
            if inside_key:
                if fn_generator[i] == "}" :
                    inside_key=False
                    self.fn_gen_keys.append(this_key)
                else :
                    this_key += fn_generator[i]
                i += 1
                continue

            if fn_generator[i] == "%" :
                try :
                    if fn_generator[i+1] == "{" :
                        inside_key = True
                        i += 2
                        this_key = ""
                        RX += "(.*)"
                        continue
                except IndexError :
                    pass
            RX += fn_generator[i]
            i += 1
        self.fn_regex = re.compile(RX)
        return
   
    @calltrace_logger
    def get_metadata_from_gen_filename(self, gen_filename):
        """
        return a metadata-dict from a virtual filename
        """
        try:
            values = self.fn_regex.match(gen_filename).groups()
        except AttributeError:
            raise RuntimeError("get_metadata_from_gen_filename: Given filename does not match pattern %s" % self.fn_regex.pattern)

        if len(values) != len(self.fn_gen_keys) :
            raise RuntimeError("get_metadata_from_gen_filename: Given filename match pattern %s, but gives incorrect number of items. keys %s != values %s" % (self.fn_regex.pattern, self.fn_gen_keys, values))
        fn_metadata = {}
        for i, item in enumerate(self.fn_gen_keys):
            fn_metadata[item] = values[i]
        return fn_metadata
 
    @calltrace_logger
    def get_contents_by_vpath(self, vpath):
        """
        returns contents of virtual directory
         XXX  SHOULD: all inode_numbers are increased by 1000, so that we have 1000 virtual directories
        files are only returned and the very leaf of the current_view
        """
        logger.debug("get_contents_by_vpath got vpath: %s" % (vpath))
        vpath_list = get_vpath_list(vpath)
        dir_level = len(vpath_list)
        logger.debug("get_contents_by_vpath got tokens: %s" % (vpath_list))
        contents = []

        # add "." and ".." entries
        vnode = self.get_dir_vnode(vpath)
        contents.append((vnode, ".", None))
        if dir_level > 0 :
            upper_vpath = "/".join(vpath_list[:-1])
            vnode = self.get_dir_vnode(upper_vpath)
            contents.append((vnode, "..", None))
        else :
            contents.append((-1, "..", "MOUNTPOINT_PARENT"))

        # we are at the end of the tree
        if dir_level == self.max_dir_level :
            where = ""
            for i, item in enumerate(vpath_list) :
                where += "%s='%s' AND " % (self.current_view["dirtree"][i], item) 
            where = where[:-len("AND ")]
            res = self.DB_BE.execute_statment("SELECT src_inode, src_filename FROM %s WHERE %s;" % (self.FILES_TABLE, where))
            file_name_occurrences = {}
            for src_inode, src_filename in res :
                file_vname = self.get_gen_filename(src_filename)
                if file_vname in file_name_occurrences :
                    file_name_occurrences[file_vname] += 1
                    file_vname = "%s (%d)" % ( file_vname, file_name_occurrences[file_vname])
                else :
                    file_name_occurrences[file_vname] = 0
                contents.append((src_inode, file_vname, src_filename))
        else : # in vtree
            for val in self.seek_vtree(vpath_list=vpath_list) :
                # path within a vdir must not be empty, 
                # otherwise it is assinged to the dirvnode of the parent vdir
                assert(len(val) > 0)
                contents.append( (self.get_dir_vnode(os.path.join(vpath, val)), val, None))
            
        logger.debug("get_contents_by_vpath returning: %s" % contents)
        # return vnode for contents
        return contents
        
    def get_inode_by_srcfilename(self,  src_filename):
        """
        return the inode from a src_filename, callend by rename
        """
        res =self.DB_BE.execute_statment("SELECT src_inode FROM %s WHERE src_filename='%s';" % (self.FILES_TABLE, src_filename))
        logger.debug("result = %s",  res)
        assert (len(res) == 1)
        return res[0][0]

