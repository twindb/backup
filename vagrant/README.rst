===========================================
Using Vagrant for TwinDB backup development
===========================================

Versions
--------

We use vagrant with VirtualBox_. Some version combinations do not work as expected. If you experience try exactly
same versions we used for development.

Vagrant
~~~~~~~
::

    $ vagrant --version
    Vagrant 1.9.1

Virtualbox
~~~~~~~~~~

::

    $ VBoxManage --version
    5.1.14r112924



Vagrant boxes
~~~~~~~~~~~~~

====== ================
 OS    Version
====== ================
CentOS bento/centos-7.3
====== ================


Check Vagrantfile if this README is outdated


Starting vagrant machines
-------------------------

::

    $ cd vagrant
    $ vagrant up


A directory with the source code will be mounted on ``/twindb_backup``

A sample config file is installed in ``/etc/twindb/twindb-backup.cfg``

Editable python module can be installed with following command:

::

    # pip install -e /twindb_backup


.. _VirtualBox: https://www.virtualbox.org/wiki/Downloads
