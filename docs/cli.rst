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

PRAX commands
-----------------------

.. command-output:: oceanum prax --help

Project specification file commands
===================================

Validate project specification file

.. command-output:: oceanum prax validate --help

Create or update a project specification file

.. command-output:: oceanum prax deploy --help

Project management commands
===========================

List project specifications

.. command-output:: oceanum prax list projects --help


Describe project

.. command-output:: oceanum prax describe project --help

Update project

.. command-output:: oceanum prax update project --help


Manage project permissions

.. command-output:: oceanum prax allow project --help


Delete project

.. command-output:: oceanum prax delete project --help


Route commands
==============

List services and apps routes

.. command-output:: oceanum prax list routes --help

Describe a service or an app route

.. command-output:: oceanum prax describe route --help

Update service or apps route thumbnail

.. command-output:: oceanum prax update route thumbnail --help

Manage service or app access permissions

.. command-output:: oceanum prax allow route --help


Pipeline commands
=================

List pipelines

.. command-output:: oceanum prax list pipelines --help

Describe pipeline

.. command-output:: oceanum prax describe pipeline --help

Submit pipeline run

.. command-output:: oceanum prax submit pipeline --help

Terminate Pipeline run

.. command-output:: oceanum prax terminate pipeline --help

Retry pipeline run

.. command-output:: oceanum prax retry pipeline --help


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