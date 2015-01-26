"""A notebook manager for IPython with MongoDB as the backend."""

from tornado import web
import os

from io import StringIO
import datetime

import pymongo

try:
    from mongodb_proxy import MongoProxy
except:
    from .mongodb_proxy import MongoProxy

from IPython.html.services.notebooks.nbmanager import NotebookManager
from IPython.nbformat import current
from IPython.utils.traitlets import Unicode, CBool


def sort_key(item):
    """Case-insensitive sorting."""
    return item['name'].lower()

#-----------------------------------------------------------------------------
# Classes
#-----------------------------------------------------------------------------


class MongoNotebookManager(NotebookManager):
    #Useless variable that is required unfortunately
    notebook_dir = Unicode(u"", config=True)

    mongo_uri = Unicode('mongodb://localhost:27017/', config=True,
        help="The URI to connect to the MongoDB instance. Defaults to 'mongodb://localhost:27017/'"
    )

    replica_set = Unicode('', config=True,
        help="Replica set for mongodb, if any"
    )

    database_name = Unicode('ipython', config=True,
        help="Defines the database in mongodb in which to store the collections"
    )

    notebook_collection = Unicode('notebooks', config=True,
        help="Defines the collection in mongodb in which to store the notebooks"
    )

    checkpoint_collection = Unicode('checkpoints', config=True,
        help="The collection name in which to keep notebook checkpoints"
    )

    checkpoints_history = CBool('checkpoints_history', config=True,
        help="Save all checkpoints or keep only last"
    )

    def __init__(self, **kwargs):
        super(MongoNotebookManager, self).__init__(**kwargs)
        if len(self.replica_set) == 0:
            self._conn = self._connect_server()
        else:
            self._conn = self._connect_replica_set()

    def get_notebook_names(self, path=''):
        """List all notebook names in the notebook dir and path."""
        path = path.strip('/')
        spec = {'path': path,
                'type': 'notebook'}
        fields = {'name': 1}
        notebooks = list(self._connect_collection(self.notebook_collection).find(spec,fields))
        names = [n['name'] for n in notebooks]
        return names

    def path_exists(self, path):
        """Does the API-style path (directory) actually exist?

        Parameters
        ----------
        path : string
            The path to check. This is an API path (`/` separated,
            relative to base notebook-dir).

        Returns
        -------
        exists : bool
            Whether the path is indeed a directory.
        """

        path = path.strip('/')
        if path != '':
            spec = {'path': path}
            count = self._connect_collection(self.notebook_collection).find(spec).count()
        else:
            count = 1
        return count > 0

    def is_hidden(self, path):
        #Nothing is hidden
        return False

    def notebook_exists(self, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name,
            'type': 'notebook'
        }

        count = self._connect_collection(self.notebook_collection).find(spec).count()
        return count == 1

    def list_dirs(self, path):
        path = path.strip('/')
        if path == '':
            prefix = ''
        else:
            prefix = path + '/'

        spec = {
            'path': prefix,
            'type': 'directory'
        }
        fields = {'name': 1}
        notebooks = list(self._connect_collection(self.notebook_collection).find(spec,fields))
        names = [n['name'].lstrip(prefix) for n in notebooks if '/' not in n['name']]

        dirs = [self.get_dir_model(name, path) for name in names]
        dirs = sorted(dirs, key=sort_key)
        return dirs

    def get_dir_model(self, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name,
            'type': 'directory'
        }
        fields = {
            'lastModified': 1,
            'created': 1
        }

        notebook = self._connect_collection(self.notebook_collection).find_one(spec,fields)
        if notebook == None:
            raise IOError('directory does not exist: %r' % (path + '|' + name))

        last_modified = notebook['lastModified']
        created = notebook['created']
        # Create the notebook model.
        model = {}
        model['name'] = name
        model['path'] = path
        model['last_modified'] = last_modified
        model['created'] = created
        model['type'] = 'directory'
        return model

    def list_notebooks(self, path):
        path = path.strip('/')
        notebook_names = self.get_notebook_names(path)
        notebooks = [self.get_notebook(name, path, content=False)
                     for name in notebook_names if self.should_list(name)]
        notebooks = sorted(notebooks, key=sort_key)
        return notebooks

    def get_notebook(self, name, path='', content=True):
        path = path.strip('/')
        if not self.notebook_exists(name=name, path=path):
            raise web.HTTPError(404, u'Notebook does not exist: %s' % name)

        spec = {
            'path': path,
            'name': name,
            'type': 'notebook'
        }
        fields = {
            'lastModified': 1,
            'created': 1
        }
        if content:
            fields['content'] = 1

        notebook = self._connect_collection(self.notebook_collection).find_one(spec,fields)

        last_modified = notebook['lastModified']
        created = notebook['created']
        # Create the notebook model.
        model = {}
        model['name'] = name
        model['path'] = path
        model['last_modified'] = last_modified
        model['created'] = created
        model['type'] = 'notebook'
        if content:
            with StringIO(notebook['content']) as f:
                nb = current.read(f, u'json')
            self.mark_trusted_cells(nb, name, path)
            model['content'] = nb
        return model

    def create_notebook(self, model=None, path=''):
        """Create a new notebook and return its model with no content."""
        path = path.strip('/')
        if model is None:
            model = {}
        if 'content' not in model:
            metadata = current.new_metadata(name=u'')
            model['content'] = current.new_notebook(metadata=metadata)
        if 'name' not in model:
            model['name'] = self.increment_filename('Untitled', path)

        model['path'] = path
        model['type'] = 'notebook'
        model = self.save_notebook(model, model['name'], model['path'])

        return model

    def save_notebook(self, model, name='', path=''):
        path = path.strip('/')

        if 'content' not in model:
            raise web.HTTPError(400, u'No notebook JSON data provided')

        # One checkpoint should always exist
        if self.notebook_exists(name, path) and not self.list_checkpoints(name, path):
            self.create_checkpoint(name, path)

        new_path = model.get('path', path).strip('/')
        new_name = model.get('name', name)

        if path != new_path or name != new_name:
            self.rename_notebook(name, path, new_name, new_path)

        # Save the notebook file
        nb = current.to_notebook_json(model['content'])

        self.check_and_sign(nb, new_name, new_path)

        if 'name' in nb['metadata']:
            nb['metadata']['name'] = u''
        try:
            with StringIO() as f:
                current.write(nb, f, u'json')
                spec = {
                    'path': path,
                    'name': name
                }
                data = {
                    '$set': {
                        'type': 'notebook',
                        'content': f.getvalue(),
                        'lastModified': datetime.datetime.now(),
                    }
                }
                f.close()
                if 'created' in model:
                    data['$set']['created'] = model['created']
                else:
                    data['$set']['created'] = datetime.datetime.now()
                notebook = self._connect_collection(self.notebook_collection).update(spec,data, upsert=True)
        except Exception as e:
            raise web.HTTPError(400, u'Unexpected error while autosaving notebook: %s' % (e))
        model = self.get_notebook(new_name, new_path, content=False)

        return model

    def update_notebook(self, model, name, path=''):
        path = path.strip('/')
        new_name = model.get('name', name)
        new_path = model.get('path', path).strip('/')
        if path != new_path or name != new_name:
            self.rename_notebook(name, path, new_name, new_path)
        model = self.get_notebook(new_name, new_path, content=False)
        return model

    def delete_notebook(self, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name
        }
        fields = {
            'name': 1,
        }

        notebook = self._connect_collection(self.notebook_collection).find_one(spec,fields)
        if not notebook:
            raise web.HTTPError(404, u'Notebook does not exist: %s' % name)

        # clear checkpoints
        self._connect_collection(self.checkpoint_collection).remove(spec)
        self._connect_collection(self.notebook_collection).remove(spec)

    def rename_notebook(self, old_name, old_path, new_name, new_path):
        old_path = old_path.strip('/')
        new_path = new_path.strip('/')
        if new_name == old_name and new_path == old_path:
            return

        # Should we proceed with the move?
        spec = {
            'path': new_path,
            'name': new_name
        }
        fields = {
            'name': 1,
        }
        notebook = self._connect_collection(self.notebook_collection).find_one(spec,fields)
        if notebook != None:
            raise web.HTTPError(409, u'Notebook with name already exists: %s' % new_name)

        # Move the notebook file
        try:
            spec = {
                'path': old_path,
                'name': old_name
            }
            modify = {
                '$set': {
                    'path': new_path,
                    'name': new_name
                }
            }
            self._connect_collection(self.notebook_collection).update(spec, modify)
        except Exception as e:
            raise web.HTTPError(500, u'Unknown error renaming notebook: %s %s' % (old_os_path, e))

        # Move the checkpoints
        spec = {
            'path': old_path,
            'name': old_name
        }
        modify = {
            '$set': {
                'path': new_path,
                'name': new_name
            }
        }
        self._connect_collection(self.checkpoint_collection).update(spec, modify, multi=True)

    # public checkpoint API
    def create_checkpoint(self, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name
        }

        notebook = self._connect_collection(self.notebook_collection).find_one(spec)
        chid = notebook['_id']
        del notebook['_id']
        cp_id = str(self._connect_collection(self.checkpoint_collection).find(spec).count())

        if self.checkpoints_history:
            spec['cp'] = cp_id
        else:
            notebook['cp'] = cp_id
            spec['id'] = chid

        newnotebook = {'$set': notebook}

        last_modified = notebook["lastModified"]
        self._connect_collection(self.checkpoint_collection).update(spec, newnotebook, upsert=True)

        # return the checkpoint info
        return dict(id=cp_id, last_modified=last_modified)

    def list_checkpoints(self, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name,
        }
        checkpoints = list(self._connect_collection(self.checkpoint_collection).find(spec))
        return [dict(id=c['cp'], last_modified=c['lastModified']) for c in checkpoints]

    def restore_checkpoint(self, checkpoint_id, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name,
            'cp': checkpoint_id
        }

        checkpoint = self._connect_collection(self.checkpoint_collection).find_one(spec)

        if checkpoint == None:
            raise web.HTTPError(
                404, u'Notebook checkpoint does not exist: %s-%s' % (name, checkpoint_id)
            )
        del spec['cp']
        del checkpoint['cp']
        del checkpoint['_id']
        checkpoint = {'$set': checkpoint}
        self._connect_collection(self.notebook_collection).update(spec, checkpoint, upsert=True)

    def delete_checkpoint(self, checkpoint_id, name, path=''):
        path = path.strip('/')
        spec = {
            'path': path,
            'name': name,
            'cp': checkpoint_id
        }
        checkpoint = self._connect_collection(self.checkpoint_collection).find_one(spec)
        if checkpoint == None:
            raise web.HTTPError(404,
                u'Notebook checkpoint does not exist: %s%s-%s' % (path, name, checkpoint_id)
            )
        self._connect_collection(self.checkpoint_collection).remove(spec)

    def info_string(self):
        return "Serving notebooks from mongodb"

    def get_kernel_path(self, name, path='', model=None):
        return os.path.join(self.notebook_dir, path)

    #mongodb related functions
    def _connect_server(self):
        return MongoProxy(pymongo.MongoClient(self.mongo_uri))

    def _connect_replica_set(self):
        return MongoProxy(pymongo.MongoReplicaSetClient(self.mongo_uri, self._replicaSet))

    def _connect_collection(self, collection):
        if not self._conn.alive():
            if len(self.replica_set) == 0:
                self._conn = self._connect_server()
            else:
                self._conn = self._connectReplicaSet()
        return self._conn[self.database_name][collection]
