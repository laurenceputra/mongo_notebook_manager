MongoNotebookManager
====================

IPython Notebook Manager in MongoDB

required
~~~~~~~~

::

    NotebookApp.notebook_manager_class=mongo_notebook_manager.MongoNotebookManager

optional
~~~~~~~~

Below are the arguments, with their default values

mongo\_uri
^^^^^^^^^^

::

    MongoNotebookManager.mongo_uri='mongodb://localhost:27017/'

replica\_set
^^^^^^^^^^^^

::

    MongoNotebookManager.replica_set=''

database\_name
^^^^^^^^^^^^^^

::

    MongoNotebookManager.database_name='ipython'

notebook\_collection
^^^^^^^^^^^^^^^^^^^^

::

    MongoNotebookManager.notebook_collection='notebooks'

checkpoint\_collection
^^^^^^^^^^^^^^^^^^^^^^

::

    MongoNotebookManager.checkpoint_collection='checkpoints'

