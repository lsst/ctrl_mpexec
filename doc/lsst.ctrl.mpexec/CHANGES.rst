lsst-ctrl-mpexec v25.0.0 (2023-02-28)
=====================================

New Features
------------

- * Added support for transferring files into execution butler. (`DM-35494 <https://jira.lsstcorp.org/browse/DM-35494>`_)
- * Added documentation on how to use ``--show`` and ``--config``.
  * A pipeline will now never execute if ``--show`` option is used with ``pipetask run``.
  * The ``--config`` option can now accept list configuration values (with or without square brackets), for example ``--config task:listItem=a,b`` or ``--config "task:listItem=[a,b]"``.
  * The ``--config-file`` option can now take comma-separated file names for multiple config files. (`DM-35917 <https://jira.lsstcorp.org/browse/DM-35917>`_)
- * added additional quanta information to be displayed by the logger, showing number of quanta per task (`DM-36145 <https://jira.lsstcorp.org/browse/DM-36145>`_)
- If ``pipetask`` is run with multiple processes and if a butler datastore cache is configured, all subprocesses will now share the same cache.
  For large numbers of simultaneous processes it may be necessary to significantly increase the number of datasets in the cache to make the cache usable.
  This can be done by using the ``$DAF_BUTLER_CACHE_EXPIRATION_MODE`` environment variable.

  Previously each subprocess would get its own cache and if ``fork`` start method was used these cache directories would not be cleaned up. (`DM-36412 <https://jira.lsstcorp.org/browse/DM-36412>`_)
- Always disable implicit threading (e.g. in OpenBLAS) by default in ``pipetask run``, even when not using ``-j``.

  The new ``--enable-implicit-threading`` can be used to turn it back on. (`DM-36831 <https://jira.lsstcorp.org/browse/DM-36831>`_)


API Changes
-----------

- ``SimplePipelineExecutor`` factory methods add ``bind`` parameter for bind values to use with the user expression. (`DM-36487 <https://jira.lsstcorp.org/browse/DM-36487>`_)


lsst-ctrl-mpexec v24.0.0 (2022-08-26)
=====================================

New Features
------------

- Added ``--dataset-query-constraint`` option to ``pipetask qgraph`` command (and thus downstream commands) that allows a
  user to control how `~lsst.pipe.base.QuantumGraph` creation is constrained by dataset existence. (`DM-31769 <https://jira.lsstcorp.org/browse/DM-31769>`_)
- Builds using ``setuptools`` now calculate versions from the Git repository, including the use of alpha releases for those associated with weekly tags. (`DM-32408 <https://jira.lsstcorp.org/browse/DM-32408>`_)
- Added ``--summary`` option to ``pipetask run`` command, it produces JSON report for execution status of the whole process and individual quanta. (`DM-33481 <https://jira.lsstcorp.org/browse/DM-33481>`_)
- Added ``pipetask`` CLI commands ``purge`` and ``cleanup``. (`DM-33634 <https://jira.lsstcorp.org/browse/DM-33634>`_)
- Removed dependency on the ``obs_base`` and ``afw`` packages. Now only depends on ``pipe_base`` and ``daf_butler`` (along with ``pex_config`` and ``utils``). (`DM-34105 <https://jira.lsstcorp.org/browse/DM-34105>`_)
- Replaced the unused ``--do-raise`` option with ``--pdb``,
  which drops the user into the debugger
  (``pdb`` by default, but ``--pdb=ipdb`` also works if you have ``ipdb`` installed)
  on an exception. (`DM-34215 <https://jira.lsstcorp.org/browse/DM-34215>`_)


Bug Fixes
---------

- The `click.Path` API should mostly be used with keyword arguments, changed from ordered arguments to keyword arguments when calling it. (`DM-34261 <https://jira.lsstcorp.org/browse/DM-34261>`_)
- Fixed a bug where dot graphs of pipelines did not correctly render edges between composite and component dataset types. (`DM-34811 <https://jira.lsstcorp.org/browse/DM-34811>`_)


Other Changes and Additions
---------------------------

- Added `lsst.ctrl.mpexec.SimplePipelineExecutor`, a minimal high-level Python interface for `~lsst.pipe.base.PipelineTask` execution intended primarily for unit testing. (`DM-31966 <https://jira.lsstcorp.org/browse/DM-31966>`_)


lsst-ctrl-mpexec v23.0.1 (2022-02-02)
=====================================

Miscellaneous Changes of Minor Interest
---------------------------------------

- Allow ``pipetask run`` execution to continue in single-process mode after failure of one or more tasks. Previously execution stopped on an exception from any task. (`DM-33339 <https://jira.lsstcorp.org/browse/DM-33339>`_)


lsst-ctrl-mpexec v23.0.0 (2021-12-10)
=====================================

New Features
------------

- Several improvements in ``pipetask`` execution options:

   - New option ``--skip-existing-in`` which takes collection names(s), if output datasets already exist in those collections corresponding quanta is skipped.
   - A ``--skip-existing`` option is now equivalent to appending output run collection to the ``--skip-existing-in`` list.
   - An ``--extend-run`` option implicitly enables ``--skip-existing`` option.
   - A ``--prune-replaced=unstore`` option only removes regular output datasets; InitOutputs, task configs, and package versions are not removed. (`DM-27492 <https://jira.lsstcorp.org/browse/DM-27492>`_)
- GraphViz dot files generated by pipetask now include more information (``RUN`` collection for datasets, dimensions for tasks, data IDs for quanta). (`DM-28111 <https://jira.lsstcorp.org/browse/DM-28111>`_)
- ``pipetask qgraph`` can now generate a standalone "execution butler" which is a SQLite registry with all the expected outputs pre-filled in registry.  Using this registry allow ``pipetask run`` to execute without touching the main registry whilst still writing file artifacts to the standard location.  It is not yet configured to allow a completely detached processing using a local datastore but this can be changed manually after creation to use a chained datastore. (`DM-28649 <https://jira.lsstcorp.org/browse/DM-28649>`_)
- * Log messages issued during quantum execution are now collected and stored in butler as ``tasklabel_log`` dataset types.
  * New command line options for logging have been added to ``pipetask``. These include ``--log-file`` to write log messages to a file and ``--no-log-tty`` to disable log output to the terminal. (`DM-30977 <https://jira.lsstcorp.org/browse/DM-30977>`_)
- * Add the output run to the log record.
  * Add ``--log-label`` option to ``pipetask`` command to allow extra information to be injected into the log record. (`DM-31884 <https://jira.lsstcorp.org/browse/DM-31884>`_)


Bug Fixes
---------

- Improve exception handling in ``ExecFixupDataId`` (`DM-29384 <https://jira.lsstcorp.org/browse/DM-29384>`_)
- Fix config comparison with ``--skip-existing``. (`DM-29580 <https://jira.lsstcorp.org/browse/DM-29580>`_)
- Include output collection in call to ``buildExecutionButler``. (`DM-31691 <https://jira.lsstcorp.org/browse/DM-31691>`_)
- Fix call to ``buildExecutionButler`` when chained input collection. (`DM-31711 <https://jira.lsstcorp.org/browse/DM-31711>`_)


Miscellaneous Changes of Minor Interest
---------------------------------------

- Add some of the pipetask command line options to QuantumGraph metadata (`DM-30702 <https://jira.lsstcorp.org/browse/DM-30702>`_)


lsst-ctrl-mpexec v22.0 (2021-04-01)
===================================

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
