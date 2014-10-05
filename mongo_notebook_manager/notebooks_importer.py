#!/usr/bin/env python
import argparse
import datetime
import fnmatch
import os
import logging

from pymongo import MongoClient


def insert_or_update(db, name, content):
    now = datetime.datetime.now()
    sname = name.strip('/').split('/')
    data = {'path': '' if len(sname) < 2 else sname[0],
            'type': 'notebook',
            'name': sname[-1], 'content': content,
            'created': now, 'lastModified': now}
    if not db.find_one({'name': name, 'type': 'notebook'}):
        db.insert(data)
        return 'Notebook "{}" created'.format(name)
    else:
        db.update({'name': sname[-1], 'type': 'notebook'}, data)
        return 'Notebook "{}" updated'.format(name)


def prepare_directories(db, path, root, dirnames):
    root1 = root.replace(path, '').strip('/')
    for dirname in dirnames:
        data = {'name': dirname,
                'type': 'directory',
                'created': datetime.datetime.now(),
                'lastModified': datetime.datetime.now(),
                'path': root1}
        if not db.find_one({'path': root1, 'name': dirname}):
            db.insert(data)
            logging.info('Directory "{}" created'.format(dirname))


def get_notebooks(db, path, ext):
    for root, dirnames, filenames in os.walk(path):
        prepare_directories(db, path, root, dirnames)
        for filename in fnmatch.filter(filenames, ext):
            filepath = os.path.join(root, filename)
            yield [filepath.replace(path, ''), open(filepath).read()]


def import_notebooks(db, path, ext):
    files = get_notebooks(db, path, ext)
    for notebook in files:
        message = insert_or_update(db, *notebook)
        logging.info(message)
    else:
        logging.error('No notebooks found')


def main():
    parser = argparse.ArgumentParser(description='FS2MongoDB importer')

    parser.add_argument('--mongodb', required=True, type=str,
                        help='MongoDB connection string')

    parser.add_argument('--path', required=True, default='.',
                        type=str,
                        help='Source directory with notbeooks')

    parser.add_argument('--database', default='notebooks',
                        type=str,
                        help='MongoDB Database name (default: notebooks)')

    parser.add_argument('--collection', default='notebooks',
                        type=str,
                        help='MongoDB Collection name (default: notebooks)')

    parser.add_argument('--ext', default='*.ipynb',
                        type=str,
                        help='Notbeooks extension (default: *.ipynb)')

    args = parser.parse_args()
    conn = MongoClient(args.mongodb)[args.database][args.collection]
    import_notebooks(conn, args.path, args.ext)


if __name__ == '__main__':
    main()
