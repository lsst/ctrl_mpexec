Multi-Processing Executor v22.0 2021-04-01
==========================================

New Features
------------

* ``pipetask run`` can now execute a subset of a graph. This allows a single graph file to be created with an entire workflow and then only part of it to be executed. This is very important for large scale workflow execution. [DM-27667]

Performance Enhancement
-----------------------

* Multi-processing execution performance has been significantly improved for large graphs. [DM-28418]

Other
-----

* Ignore ``--input`` instead of rejecting it if it hasn't changed. [DM-28101]
* The graph file format has been changed from a pickle file to a form that can efficiently be accessed from an object store. This new format has a ``.qgraph`` file extension. [DM-27784]
* A full URI can now be used to specify the location of the quantum graph. [DM-27682]
