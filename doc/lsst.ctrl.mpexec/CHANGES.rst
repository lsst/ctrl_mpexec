lsst-ctrl-mpexec v29.0.0 (2025-03-25)
=====================================

New Features
------------

- Quantum metadata outputs now record the IDs of all output datasets.
  This is stored in an ``outputs`` key in the metadata dataset. (`DM-35396 <https://rubinobs.atlassian.net/browse/DM-35396>`_)
- Added new command line options ``--pipeline-mermaid`` and ``--qgraph-mermaid`` to store Mermaid representations of a pipeline and a quantum graph, respectively. (`DM-46503 <https://rubinobs.atlassian.net/browse/DM-46503>`_)
- We now track "success caveats" like ``NoWorkFound`` in execution.

  This adds additional information to the task metadata and additional summary reporting to ``pipetask report --force-v2``.
  It relies on changes in ``lsst.pipe.base`` for the ``QuantumSuccessCaveats`` flag enum and new logic in ``QuantumProvenanceGraph``. (`DM-47730 <https://rubinobs.atlassian.net/browse/DM-47730>`_)


API Changes
-----------

- The Quantum ID is now passed through the executors so it can be recorded in the provenance by ``QuantumContext``. (`DM-35396 <https://rubinobs.atlassian.net/browse/DM-35396>`_)


Bug Fixes
---------

- The coverage package is now optional and lazy-loads it only when needed.
  Declares packaging extra for coverage, i.e., ``pip install lsst-ctrl-mpexec[coverage]``.
  Updated minimum supported Python to 3.11 (dependency-driven change). (`DM-47159 <https://rubinobs.atlassian.net/browse/DM-47159>`_)


Other Changes and Additions
---------------------------

- Input datasets that were already found to exist during QG generation will no longer be re-checked for existence during execution.

  This mitigates a problem in which butler misconfiguration (e.g., datastore name mismatches) would lead to hard-to-spot ``NoWorkFound`` conditions in the first step in a pipeline.  Those errors should now result in a ``FileNotFoundError`` with more helpful information. (`DM-40242 <https://rubinobs.atlassian.net/browse/DM-40242>`_)
- Removed explicit calls to garbage collector during in-process execution.
  Garbage collection can be slow and it is not useful in our common use cases. (`DM-47482 <https://rubinobs.atlassian.net/browse/DM-47482>`_)


lsst-ctrl-mpexec v28.0.0 (2024-11-21)
=====================================

New Features
------------

- Aggregated multiple ``pipetask report`` outputs into one holistic ``Summary``.

  While the ``QuantumProvenanceGraph`` was designed to resolve processing over dataquery-identified groups, ``pipetask aggregate-reports`` is designed to combine multiple group-level reports into one which totals the successes, issues and failures over the same section of pipeline. (`DM-41605 <https://rubinobs.atlassian.net/browse/DM-41605>`_)
- Upgraded ``QuantumGraphExecutionReport`` to handle multiple overlapping graphs.

  Updated the ``pipetask report`` command-line interface to accommodate the new
  ``QuantumProvenanceGraph``.
  This allows for resolving outcomes of processing over multiple attempts (and associated graphs) over the same dataquery, providing information on recoveries, persistent issues and mismatch errors. (`DM-41711 <https://rubinobs.atlassian.net/browse/DM-41711>`_)
- Displayed successful and expected quanta in ``pipetask report``. (`DM-44368 <https://rubinobs.atlassian.net/browse/DM-44368>`_)
- Modified executors to now handle ``lsst.pipe.base.AnnotatedPartialOutputsError``.

  This exception can be considered either a failure or a success, depending
  on how the executor is configured. (`DM-44488 <https://rubinobs.atlassian.net/browse/DM-44488>`_)
- Moved ``pipeline-dot`` build from ``cmdLineFwk`` to builder.

  This was done to make the ``pipeline-dot`` build more accessible to other packages.
  As part of this change, output ``pipeline-dot`` files contain dimensions and storage classes for each dataset.
  This change also includes updates to existing unit tests to reflect the new output types. (`DM-44647 <https://rubinobs.atlassian.net/browse/DM-44647>`_)


Bug Fixes
---------

- The previous version of the human-readable report only reported the first failed quantum by exiting the loop upon finding it.
  Following changes in ``pipe_base``, now display only counts of failures for ``len(failed_quanta)>5``.
  Data IDs can be found in the YAML file listing error messages. (`DM-44091 <https://rubinobs.atlassian.net/browse/DM-44091>`_)
- Fixed the bug in ``pipetask run-qbb -j`` which crashed if database dimension version is different from default version. (`DM-45894 <https://rubinobs.atlassian.net/browse/DM-45894>`_)
- Now compare timestamps properly between two graphs when using ``pipetask report`` on multiple graphs.

  (The bug was looping back to say the "previous" graph was the end of the list when "count" was 0).
  Also fix the wording in the associated `RuntimeError`. (`DM-46689 <https://rubinobs.atlassian.net/browse/DM-46689>`_)


Other Changes and Additions
---------------------------

- Set the default for raising on partial output error to `True`.

  Allowing processing to proceed when we encounter an error that may not be fatal is functionality we'll still want eventually, but enabling it by default was premature, since our processing-status reporting tools are yet able to distinguish these cases from unqualified successes. (`DM-46525 <https://rubinobs.atlassian.net/browse/DM-46525>`_)


An API Removal or Deprecation
-----------------------------

- Removed the deprecated support for ``TaskDef`` in some APIs. (`DM-40443 <https://rubinobs.atlassian.net/browse/DM-40443>`_)
- The ``lsst.ctrl.mpexec.dotTools`` package has been relocated to ``lsst.pipe.base.dot_tools``. (`DM-45701 <https://rubinobs.atlassian.net/browse/DM-45701>`_)


lsst-ctrl-mpexec 27.0.0 (2024-05-29)
====================================

New Features
------------

- Be more permissive about input/output collection consistency, and provided a ``--rebase`` option to ``pipetask run`` and ``pipetask qgraph`` to force consistency.

  An existing output collection is now considered consistent with a given sequence of input collections if the latter is a contiguous subsequence of the former.
  When this is not the case, ``--rebase`` redefines the output collection such that it will be. (`DM-37140 <https://rubinobs.atlassian.net/browse/DM-37140>`_)
- Updated the open-source license to allow for the code to be distributed with either GPLv3 or BSD 3-clause license. (`DM-37231 <https://rubinobs.atlassian.net/browse/DM-37231>`_)
- Adde ``pipeline-graph`` and ``task-graph`` options for ``pipetask build --show``, which provide text-art visualization of pipeline graphs. (`DM-39779 <https://rubinobs.atlassian.net/browse/DM-39779>`_)
- Added ``pipetask report`` which reads a quantum graph and reports on the outputs of failed, produced and missing quanta.
  This is a command-line incarnation of
  ``QuantumGraphExecutionReport.make_reports`` in combination with
  ``QuantumGraphExecutionReport.write_summary_yaml``. (`DM-41131 <https://rubinobs.atlassian.net/browse/DM-41131>`_)
- Added ``--summary`` option to ``pipetask qgraph``. (`DM-41542 <https://rubinobs.atlassian.net/browse/DM-41542>`_)
- Made option to output ``pipetask report`` information to the command-line using Astropy tables and set to default.
  Now unpack a more human-readable dictionary from
  ``lsst.pipe.base.QuantumGraphExecutionReports.to_summary_dict`` and print summary tables of quanta and datasets to the command-line.
  Can now save error messages and associated data ids to a YAML file in the working directory, or optionally print them to screen as well. (`DM-41606 <https://rubinobs.atlassian.net/browse/DM-41606>`_)


API Changes
-----------

- ``SeparablePipelineExecutor.run_pipeline`` has been modified to take a ``num_proc`` parameter to specify how many subprocesses can be used to execute the pipeline.
  The default is now ``1`` (no spawning), which is a change from the previous behavior of using 80% of the available cores. (`DM-42751 <https://rubinobs.atlassian.net/browse/DM-42751>`_)


Bug Fixes
---------

- Removed shadowing of ``pipetask build -t`` by ``pipetask qgraph -t``.
  ``-t`` now means ``--task`` (the original meaning) rather than ``--transfer``. (`DM-35599 <https://rubinobs.atlassian.net/browse/DM-35599>`_)
- Fixed a storage class bug in registering dataset types in ``pipetask run``.

  Prior to this fix, the presence of multiple storage classes being associated with the same dataset type in a pipeline could cause the registered dataset type's storage class to be random and nondeterministic in regular ``pipetask run`` execution (but not quantum-backed butler execution).
  It now follows the rules set by ``PipelineGraph``, in which the definition in the task that produces the dataset wins. (`DM-41962 <https://rubinobs.atlassian.net/browse/DM-41962>`_)
- Ensured that the implicit threading options for ``run-qbb`` is used so that implicit threading can be disabled. (`DM-42118 <https://rubinobs.atlassian.net/browse/DM-42118>`_)
- Fixed ``dump_kwargs`` `TypeError` caused by migration to Pydantic 2. (`DM-42376 <https://rubinobs.atlassian.net/browse/DM-42376>`_)
- Fixed the ``--show-errors`` option in ``pipetask report``.

  Correctly pass the option to the function as a flag.
  Then, in testing, use the ``--show-errors`` option to avoid saving YAML files to disk without adequate cleanup. (`DM-43363 <https://rubinobs.atlassian.net/browse/DM-43363>`_)
- Fixed BPS auto-retry functionality broken on `DM-43060 <https://rubinobs.atlassian.net/browse/DM-43060>`_, by restoring support for repeated execution of already-successful quanta in ``pipetask run-qbb``. (`DM-43484 <https://rubinobs.atlassian.net/browse/DM-43484>`_)


Other Changes and Additions
---------------------------

- Dropped support for Pydantic 1.x. (`DM-42302 <https://rubinobs.atlassian.net/browse/DM-42302>`_)


An API Removal or Deprecation
-----------------------------

- Support for fork option in ``pipetask run`` has been removed as unsafe.
  Default start option now is ``spawn``, and ``forkserver`` is also available.
  The ``fork`` option is still present in CLI for compatibility, but is deprecated and replaced by ``spawn`` if specified. (`DM-41832 <https://rubinobs.atlassian.net/browse/DM-41832>`_)


lsst-ctrl-mpexec v26.0.0 (2023-09-23)
=====================================

New Features
------------

- Added support for executing quantum graph using Quantum-backed butler.
  ``pipetask`` adds two new commands to support execution with Quantum-backed butler, mostly useful for BPS:

  * ``pre-exec-init-qbb`` which runs ``PreExecInit`` step of the execution to produce InitOutputs.
  * ``run-qbb`` which executes ``QuantumGraph`` (or individual quanta) using Quantum-backed butler. (`DM-33497 <https://rubinobs.atlassian.net/browse/DM-33497>`_)
- Added ``--coverage`` and ``--cov-packages`` to ``pipetask`` commands to allow for code coverage calculations when running (`DM-34420 <https://rubinobs.atlassian.net/browse/DM-34420>`_)
- Added ``SeparablePipelineExecutor``, a pipeline executor midway in capability between ``SimplePipelineExecutor`` and ``CmdLineFwk``.
  ``SeparablePipelineExecutor`` is designed to be run from Python, and lets the caller decide when each pipeline processing step is carried out.
  It also allows certain pipeline steps to be customized by passing alternate implementations of execution strategies (e.g., custom graph builder). (`DM-36162 <https://rubinobs.atlassian.net/browse/DM-36162>`_)
- ``pipetask`` will now produce ``QuantumGraph`` with resolved output references, even with execution butler option. (`DM-37582 <https://rubinobs.atlassian.net/browse/DM-37582>`_)
- Added new command ``update-graph-run`` to ``pipetask``.
  It updates existing quantum graph with new output run name and re-generates output dataset IDs. (`DM-38780 <https://rubinobs.atlassian.net/browse/DM-38780>`_)
- Added new command line options ``--cores-per-quantum`` and ``--memory-per-quantum``.
  These can be used to pass some execution context into a quantum, allowing that quantum to change how it executes (maybe by using multiple threads). (`DM-39661 <https://rubinobs.atlassian.net/browse/DM-39661>`_)
- Made it possible to force failures in mocked pipelines from the command-line. (`DM-39672 <https://rubinobs.atlassian.net/browse/DM-39672>`_)
- The output of the ``pipetask ... --show=graph`` now includes extended information about dataset references and their related datastore records. (`DM-40254 <https://rubinobs.atlassian.net/browse/DM-40254>`_)


API Changes
-----------

- Several modification to multiple classes to support execution with Quantum-backed butler:

  * ``CmdLineFwk`` class adds two new methods: ``preExecInitQBB``, which only runs ``PreExecInit`` step of the execution to produce InitOutputs; and ``runGraphQBB``, which executes ``QuantumGraph`` using Quantum-backed butler.
  * Abstract classes ``QuantumExecutor`` and ``QuantumGraphExecutor`` do not accept ``Butler`` instance in their ``execute()`` methods.
  * ``MPGraphExecutor`` and ``SingleQuantumExecutor`` methods updated to reflect above change and support execution with either full ``Butler`` or ``LimitedButler``.
  * New class ``PreExecInitLimited`` which performs pre-exec-init in case of Quantum-backed butler.
    The code that it shares with a regular ``PreExecInit`` class is now in their common base class ``PreExecInitBase``. (`DM-33497 <https://rubinobs.atlassian.net/browse/DM-33497>`_)
- Added new ``resources`` parameter to ``SingleQuantumExecutor``, ``SimplePipelineExecutor``, and ``SeparablePipelineExecutor`` constructors.
  This optional parameter is a `~lsst.pipe.base.ExecutionResources` object and allows the execution context to be passed into the `~lsst.pipe.base.PipelinesTask.runQuantum` method. (`DM-39661 <https://rubinobs.atlassian.net/browse/DM-39661>`_)


Bug Fixes
---------

- Fixed ``SingleQuantumExecutor`` class to correctly handle the case with ``clobberOutputs=True`` and ``skipExistingIn=None``.
  Documentation says that complete quantum outputs should be removed in this case, but they were not removed. (`DM-38601 <https://rubinobs.atlassian.net/browse/DM-38601>`_)


Other Changes and Additions
---------------------------

- * ``SingleQuantumExecutor`` has been modified such that it no longer unresolves ``DatasetRef`` when putting the non- ``PipelineTask`` datasets (such as packages and configs).
    This has been done so that the refs in the quantum graph are preserved when they are written to a normal Butler.
  * Fixed a race condition when ``pipetask run`` creates the graph with a timestamped output run and then executes it.
    Previously the graph creation and run execution phases calculated their own timestamped output run and it would be possible for the execution output run to be one second later than the graph run.
    Previously this did not matter (the graph run was being ignored) but with the change to always use the ``DatasetRef`` from the graph it becomes critical that they match. (`DM-38779 <https://rubinobs.atlassian.net/browse/DM-38779>`_)
- Revive the previously-bitrotted pipeline mocking system.

  Most of the implementation has been moved to `pipe_base`, and the point at which mocking occurs has moved from execution to just before `QuantumGraph` generation, which changes which `pipetask` subcommands the `--mock` option is valid for. (`DM-38952 <https://rubinobs.atlassian.net/browse/DM-38952>`_)
- Updated the directed graph color scheme with an aim towards making node text easier to read.
  The previous pipeline directed graph nodes used dark gray as their background color.
  It had been reported that it is difficult to read the black text on the dark gray background.
  In the process of exploring what color schemes would be optimal to satisfy the aim of this ticket, it emerged that making use of the Rubin visual identity colors may be desirable.
  This will help to make LSST pipeline graphs more instantly recognizable as Rubin-associated products.
  Colors: https://rubin.canto.com/g/RubinVisualIdentity (`DM-39294 <https://rubinobs.atlassian.net/browse/DM-39294>`_)
- The ``saveMetadata`` configuration field is now ignored by executors in this package, metadata is assumed to be saved for each task. (`DM-39377 <https://rubinobs.atlassian.net/browse/DM-39377>`_)
- Improved logging and removed some obsolete code paths in ``SingleQuantumExecutor``. (`DM-40332 <https://rubinobs.atlassian.net/browse/DM-40332>`_)
- Command line help for ``pipetask run`` has been updated to reflect its correct clobbering behavior.


lsst-ctrl-mpexec v25.0.0 (2023-02-28)
=====================================

New Features
------------

- * Added support for transferring files into execution butler. (`DM-35494 <https://rubinobs.atlassian.net/browse/DM-35494>`_)
- * Added documentation on how to use ``--show`` and ``--config``.
  * A pipeline will now never execute if ``--show`` option is used with ``pipetask run``.
  * The ``--config`` option can now accept list configuration values (with or without square brackets), for example ``--config task:listItem=a,b`` or ``--config "task:listItem=[a,b]"``.
  * The ``--config-file`` option can now take comma-separated file names for multiple config files. (`DM-35917 <https://rubinobs.atlassian.net/browse/DM-35917>`_)
- * added additional quanta information to be displayed by the logger, showing number of quanta per task (`DM-36145 <https://rubinobs.atlassian.net/browse/DM-36145>`_)
- If ``pipetask`` is run with multiple processes and if a butler datastore cache is configured, all subprocesses will now share the same cache.
  For large numbers of simultaneous processes it may be necessary to significantly increase the number of datasets in the cache to make the cache usable.
  This can be done by using the ``$DAF_BUTLER_CACHE_EXPIRATION_MODE`` environment variable.

  Previously each subprocess would get its own cache and if ``fork`` start method was used these cache directories would not be cleaned up. (`DM-36412 <https://rubinobs.atlassian.net/browse/DM-36412>`_)
- Always disable implicit threading (e.g. in OpenBLAS) by default in ``pipetask run``, even when not using ``-j``.

  The new ``--enable-implicit-threading`` can be used to turn it back on. (`DM-36831 <https://rubinobs.atlassian.net/browse/DM-36831>`_)


API Changes
-----------

- ``SimplePipelineExecutor`` factory methods add ``bind`` parameter for bind values to use with the user expression. (`DM-36487 <https://rubinobs.atlassian.net/browse/DM-36487>`_)


lsst-ctrl-mpexec v24.0.0 (2022-08-26)
=====================================

New Features
------------

- Added ``--dataset-query-constraint`` option to ``pipetask qgraph`` command (and thus downstream commands) that allows a
  user to control how `~lsst.pipe.base.QuantumGraph` creation is constrained by dataset existence. (`DM-31769 <https://rubinobs.atlassian.net/browse/DM-31769>`_)
- Builds using ``setuptools`` now calculate versions from the Git repository, including the use of alpha releases for those associated with weekly tags. (`DM-32408 <https://rubinobs.atlassian.net/browse/DM-32408>`_)
- Added ``--summary`` option to ``pipetask run`` command, it produces JSON report for execution status of the whole process and individual quanta. (`DM-33481 <https://rubinobs.atlassian.net/browse/DM-33481>`_)
- Added ``pipetask`` CLI commands ``purge`` and ``cleanup``. (`DM-33634 <https://rubinobs.atlassian.net/browse/DM-33634>`_)
- Removed dependency on the ``obs_base`` and ``afw`` packages. Now only depends on ``pipe_base`` and ``daf_butler`` (along with ``pex_config`` and ``utils``). (`DM-34105 <https://rubinobs.atlassian.net/browse/DM-34105>`_)
- Replaced the unused ``--do-raise`` option with ``--pdb``,
  which drops the user into the debugger
  (``pdb`` by default, but ``--pdb=ipdb`` also works if you have ``ipdb`` installed)
  on an exception. (`DM-34215 <https://rubinobs.atlassian.net/browse/DM-34215>`_)


Bug Fixes
---------

- The `click.Path` API should mostly be used with keyword arguments, changed from ordered arguments to keyword arguments when calling it. (`DM-34261 <https://rubinobs.atlassian.net/browse/DM-34261>`_)
- Fixed a bug where dot graphs of pipelines did not correctly render edges between composite and component dataset types. (`DM-34811 <https://rubinobs.atlassian.net/browse/DM-34811>`_)


Other Changes and Additions
---------------------------

- Added `lsst.ctrl.mpexec.SimplePipelineExecutor`, a minimal high-level Python interface for `~lsst.pipe.base.PipelineTask` execution intended primarily for unit testing. (`DM-31966 <https://rubinobs.atlassian.net/browse/DM-31966>`_)


lsst-ctrl-mpexec v23.0.1 (2022-02-02)
=====================================

Miscellaneous Changes of Minor Interest
---------------------------------------

- Allow ``pipetask run`` execution to continue in single-process mode after failure of one or more tasks. Previously execution stopped on an exception from any task. (`DM-33339 <https://rubinobs.atlassian.net/browse/DM-33339>`_)


lsst-ctrl-mpexec v23.0.0 (2021-12-10)
=====================================

New Features
------------

- Several improvements in ``pipetask`` execution options:

   - New option ``--skip-existing-in`` which takes collection names(s), if output datasets already exist in those collections corresponding quanta is skipped.
   - A ``--skip-existing`` option is now equivalent to appending output run collection to the ``--skip-existing-in`` list.
   - An ``--extend-run`` option implicitly enables ``--skip-existing`` option.
   - A ``--prune-replaced=unstore`` option only removes regular output datasets; InitOutputs, task configs, and package versions are not removed. (`DM-27492 <https://rubinobs.atlassian.net/browse/DM-27492>`_)
- GraphViz dot files generated by pipetask now include more information (``RUN`` collection for datasets, dimensions for tasks, data IDs for quanta). (`DM-28111 <https://rubinobs.atlassian.net/browse/DM-28111>`_)
- ``pipetask qgraph`` can now generate a standalone "execution butler" which is a SQLite registry with all the expected outputs pre-filled in registry.  Using this registry allow ``pipetask run`` to execute without touching the main registry whilst still writing file artifacts to the standard location.  It is not yet configured to allow a completely detached processing using a local datastore but this can be changed manually after creation to use a chained datastore. (`DM-28649 <https://rubinobs.atlassian.net/browse/DM-28649>`_)
- * Log messages issued during quantum execution are now collected and stored in butler as ``tasklabel_log`` dataset types.
  * New command line options for logging have been added to ``pipetask``. These include ``--log-file`` to write log messages to a file and ``--no-log-tty`` to disable log output to the terminal. (`DM-30977 <https://rubinobs.atlassian.net/browse/DM-30977>`_)
- * Add the output run to the log record.
  * Add ``--log-label`` option to ``pipetask`` command to allow extra information to be injected into the log record. (`DM-31884 <https://rubinobs.atlassian.net/browse/DM-31884>`_)


Bug Fixes
---------

- Improve exception handling in ``ExecFixupDataId`` (`DM-29384 <https://rubinobs.atlassian.net/browse/DM-29384>`_)
- Fix config comparison with ``--skip-existing``. (`DM-29580 <https://rubinobs.atlassian.net/browse/DM-29580>`_)
- Include output collection in call to ``buildExecutionButler``. (`DM-31691 <https://rubinobs.atlassian.net/browse/DM-31691>`_)
- Fix call to ``buildExecutionButler`` when chained input collection. (`DM-31711 <https://rubinobs.atlassian.net/browse/DM-31711>`_)


Miscellaneous Changes of Minor Interest
---------------------------------------

- Add some of the pipetask command line options to QuantumGraph metadata (`DM-30702 <https://rubinobs.atlassian.net/browse/DM-30702>`_)


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
