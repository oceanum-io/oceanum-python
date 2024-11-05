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

List Datasources

.. command-output:: oceanum datamesh list datasources --help

Deploy-manager commands
-----------------------

.. command-output:: oceanum PRAX --help

Project commands
================

Validate project specification

.. command-output:: oceanum PRAX validate --help

Create or update a project specification

.. command-output:: oceanum PRAX deploy --help

Descbribe project status and resources

.. command-output:: oceanum PRAX describe project --help

List project specifications

.. command-output:: oceanum PRAX list project --help

Delete project

.. command-output:: oceanum PRAX delete project --help


Route commands
==============

List services and apps routes

.. command-output:: oceanum PRAX list routes --help

Upload route thumbnail

....

Storage commands
----------------

.. command-output:: oceanum storage --help

List content in storage system

.. command-output:: oceanum storage ls --help

Copy content from storage system

.. command-output:: oceanum storage get --help

Upload content to storage system

.. command-output:: oceanum storage put --help

Remove content from storage system (not implemented yet)

.. command-output:: oceanum storage rm --help