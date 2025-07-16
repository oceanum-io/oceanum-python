======================
Command line interface
======================

The library has a command line interface to interact with the datamesh and storage system for bulk operations and administration tasks.

Top level commands
------------------

.. command-output:: oceanum --help


Auth commands
------------------

.. command-output:: oceanum auth --help

Datamesh commands
-----------------

.. command-output:: oceanum datamesh --help

Datasources commands
=========================

List datasources

.. command-output:: oceanum datamesh list datasources --help

Storage commands
----------------

.. command-output:: oceanum storage --help

List content in storage system

.. command-output:: oceanum storage ls --help

Copy content from storage system

.. command-output:: oceanum storage get --help

Upload content to storage system

.. command-output:: oceanum storage put --help

Remove content from storage system

.. command-output:: oceanum storage rm --help

Check if path exists in storage system

.. command-output:: oceanum storage exists --help

Check if path is a file in storage system

.. command-output:: oceanum storage isfile --help

Check if path is a directory in storage system

.. command-output:: oceanum storage isdir --help

Oceanum PRAX commands
---------------------

Please refer to the `Oceanum PRAX documentation <https://oceanum-prax-cli.readthedocs.io/en/latest/>`_ for more details.
