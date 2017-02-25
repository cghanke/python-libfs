#!/usr/bin/python3
"""
libraryfs - Library-like filesystems for Python-LLFUSE

Copyright ©  Christof Hanke <christof.hanke@induhviduals.de>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."""

from argparse import ArgumentParser
import llfuse
import yaml
from Libfs.misc import *
from Libfs.business_logic import BusinessLogic
from Libfs.operations import Operations
import faulthandler

faulthandler.enable()

logger = logging.getLogger(__name__)

def main():
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser_name',  help='sub-command help')
    parser_mount = subparsers.add_parser('mount',  help='mount a libfs')
    parser_update = subparsers.add_parser('update',  help='update a library')
    #
    # options for mount subcommand
    #
    parser_mount.add_argument('library', type=str,
                        help='Library file for the views')
    parser_mount.add_argument('--debug_fuse', action='store_true', 
                        help='debug fuse')
    parser_mount.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    #
    # options for update subcommand
    #
   
    parser_update.add_argument('source', type=str, 
                        help='Data directory to scan')
    parser_update.add_argument('library', type=str,
                        help='Library file for the views') 
    parser_update.add_argument("--type", type=str, required=True,
                        choices=get_available_plugins(), help="type of library fos scanning.")
    parser_update.add_argument("--remove_obsolete", action='store_true', 
                        help="remove entries from db which are not found under source")
    #
    # common options
    #
    parser.add_argument('--logconf', type=str,   
                        help='path to a YAML logging configuration file')
    parser.add_argument('--view', type=str,
                        help='name of the view (virtual directory structure) to use.')
    
    options = parser.parse_args()

    if options.logconf:
        with open(options.logconf,  "r") as f :
            logging_dict = yaml.load(f)
        logging.config.dictConfig(logging_dict)
    
    #
    # mount libfs
    # 
    
    if options.subparser_name == 'mount':
        logger.debug('Mounting...')
        fuse_options = set(llfuse.default_options)
        fuse_options.add('fsname=libraryfs')
        fuse_options.add('default_permissions')
        if options.debug_fuse :
            fuse_options.add('debug') 
    
        operations = Operations(options.library, options.mountpoint,  options.view)
        llfuse.init(operations, options.mountpoint, fuse_options)
        try:
            logger.debug('Entering main loop..')
            llfuse.main(workers=1)
        except:
            llfuse.close(unmount=False)
            raise
        logger.debug('Umounting..')
        llfuse.close()
    elif options.subparser_name == 'update':
      
    #
    # update library
    #
        from importlib import import_module
        
        plugin = import_module("Libfs.plugins.%s" % options.type)
        magix = {}
        magix["valid_keys"] = plugin.get_valid_keys()
        magix["default_view"] = plugin.get_default_view()
        magix["plugin"] =  options.type
        bl = BusinessLogic(options.library, magix=magix) 
        for root, dirs, files in os.walk(options.source):
            for f in files :
                    full_path = os.path.abspath("%s/%s" % (root, f))
                    try:
                        metadata = plugin.read_metadata(full_path)
                    except :
                        logger.warn("cannot read metadata of file: %s" % full_path)
                        continue
                    bl.add_entry(full_path, metadata)
        # if we're updating, delete entries in 
        if options.remove_obsolete:
            for src_name in bl.get_all_src_names():
                if not os.path.exists(src_name):
                    bl.remove_entry(src_name)
    else:    # should never arrive here
        assert(0)
if __name__ == '__main__':
    main()
