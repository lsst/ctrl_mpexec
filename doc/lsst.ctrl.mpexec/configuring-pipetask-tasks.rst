.. _command-line-config-howto:

#######################################
Configuring tasks from the command line
#######################################

Generally, configuring tasks for pipeline processing should happen either in the pipeline definition itself or in obs package overrides.
There are, though, situations where a specific override needs to be used when running using the :ref:`pipetask-command` command-line tool.

This page describes how to review configurations with ``--show config`` and ``--show tasks``, and change configurations on the command line with ``--config`` or ``--config-file``

.. _command-line-config-howto-show:

How to show a task's current configuration
==========================================

The ``--show`` argument allows you to review the current configuration details for a pipeline.

.. tip::

   ``--show`` works for the ``build``, ``qgraph``, and ``run`` subcommands but will stop processing once the show options have been handled.
   If ``run`` or ``qgraph`` are used but with ``--show`` options that only refer to the ``build`` stage then only ``pipetask build`` will execute.
   If any ``show`` option is given with ``pipetask run``, the command will finish after either the pipeline build or quantum graph construction phase.

.. _command-line-config-howto-show-all:

Viewing all configurations
--------------------------

Use ``--show config`` to see all current configurations for a command-line.
For example, using the RC2 subset test repository from the tutorial the first few lines of output are:

.. code-block:: bash

   pipetask build  -p $DRP_PIPE_DIR/pipelines/HSC/DRP-RC2_subset.yaml#singleFrame --show config

   ### Configuration for task `isr'
   # Flag to enable/disable metadata saving for a task, enabled by default.
   config.saveMetadata=True

   # Flag to enable/disable saving of log output for a task, enabled by default.
   config.saveLogOutput=True

   # Dataset type for input data; users will typically leave this alone, but camera-specific ISR tasks will override it
   config.datasetType='raw'

   # Fallback default filter name for calibrations.
   config.fallbackFilterName='HSC-R'

   ...

This pipeline does know about the instrument overrides from obs packages because it is defined to work specifically for HSC.

The ``--show dump-config`` option reports the full config as reported by ``--show config`` but also includes all the associated import statements.

.. _command-line-config-howto-show-subset:

Viewing a subset of configurations
----------------------------------

To filter the ``--show config`` output, include a search term with wildcard matching (``*``) characters (quoting to escape the wildcard from the shell).
For example, this will show any configuration that start with the string ``mesh``:

.. code-block:: bash

   pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show config="mesh*"

   ### Configuration for task `isr'
   # Mesh size in X for flatness statistics
   config.qa.flatness.meshX=256

   # Mesh size in Y for flatness statistics
   config.qa.flatness.meshY=256

   ### Configuration for task `characterizeImage'
   ### Configuration for task `calibrate'

This has checked all the configurations for all tasks, but if you want to restrict your search to a specific task use ``task::``:

.. code-block:: bash

   pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show config="isr::mesh*"

   ### Configuration for task `isr'
   # Mesh size in X for flatness statistics
   config.qa.flatness.meshX=256

   # Mesh size in Y for flatness statistics
   config.qa.flatness.meshY=256

.. _command-line-config-howto-pipeline:

How to view the current pipeline
--------------------------------

A pipeline can be built up from many ingredients and it can be hard to determine exactly the pipeline that you are going to execute.
For example the pipeline used for the ``pipeline_check`` package, ``$DRP_PIPE_DIR/pipelines/HSC/pipelines_check.yaml``, currently looks like:

.. code-block:: yaml

   description: |
     A tiny subset of the DRP pipeline used by the pipelines_check CI package.
   instrument: lsst.obs.subaru.HyperSuprimeCam
   imports:
     location: "$DRP_PIPE_DIR/pipelines/_ingredients/HSC/DRP.yaml"
     include:
       - processCcd

but you have to look in another place to see what is really going to be executed.
The ``--show pipeline`` option can be used to see exactly the pipeline that will be run:

.. code-block:: bash

   $ pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show pipeline

which currently results in this more detailed output:

.. code-block:: yaml

   description: 'A tiny subset of the DRP pipeline used by the pipelines_check CI package.'
   instrument: lsst.obs.subaru.HyperSuprimeCam
   parameters:
     band: i
   tasks:
     isr:
       class: lsst.ip.isr.IsrTask
     characterizeImage:
       class: lsst.pipe.tasks.characterizeImage.CharacterizeImageTask
       config:
       - python: |
           import lsst.meas.extensions.piff.piffPsfDeterminer
           config.measurePsf.psfDeterminer = "piff"
     calibrate:
       class: lsst.pipe.tasks.calibrate.CalibrateTask
   subsets:
     processCcd:
       subset:
       - isr
       - calibrate
       - characterizeImage
       description: 'Set of tasks to run when doing single frame processing, without
         any conversions to Parquet/DataFrames or visit-level summaries.'

showing that this pipeline consists of three main tasks: ``isr``, ``characterizeImage`` and ``calibrate``, and that ``characterizeImage`` has been configured to use ``piff``.

.. _command-line-config-howto-subtasks:

How to view retargeted subtasks
-------------------------------

To see what subtasks are currently configured to run with the specified pipeline, use the ``--show tasks`` argument.
For example:

.. code-block:: bash

   pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show tasks

An example of the printed output is:

.. code-block:: text

   ### Subtasks for task `lsst.ip.isr.isrTask.IsrTask'
   ampOffset: lsst.obs.subaru.ampOffset.SubaruAmpOffsetTask
   ...
   ### Subtasks for task `lsst.pipe.tasks.characterizeImage.CharacterizeImageTask'
   applyApCorr: lsst.meas.base.applyApCorr.ApplyApCorrTask
   background: lsst.meas.algorithms.subtractBackground.SubtractBackgroundTask
   ...
   ### Subtasks for task `lsst.pipe.tasks.calibrate.CalibrateTask'
   applyApCorr: lsst.meas.base.applyApCorr.ApplyApCorrTask
   astrometry: lsst.meas.astrom.astrometry.AstrometryTask
   astrometry.matcher: lsst.meas.astrom.matchPessimisticB.MatchPessimisticBTask
   ...

This subtask hierarchy is interpreted as follows:

- The ``calibrate`` task in this pipeline is configured to use `lsst.pipe.tasks.calibrate.CalibrateTask`.
- ``calibrate`` (again, implemented by `~lsst.pipe.tasks.calibrate.CalibrateTask`) has a subtask named ``astrometry``, which is currently configured to use the `lsst.meas.astrom.astrometry.AstrometryTask` task.
- ``calibrate.astrometry`` has a subtask named ``matcher``, which is implemented by `~lsst.meas.astrom.matchPessimisticB.MatchPessimisticBTask`.

Note that if the ``calibrate.astrometry`` task is retargeted to a different task class, the subtask of ``calibrate.astrometry`` *may* change (for example, ``calibrate.astrometry.matcher`` may no longer exist).

.. _command-line-config-howto-config:

How to set configurations with command-line arguments
=====================================================

Pipelines can be configured through a combination of two mechanisms: arguments on the command line (``--config``) or through configuration files (``--config-file``).
In general, simple configurations can be made through the command line, while complex configurations and subtask retargeting must done through configuration files (see :ref:`command-line-config-howto-configfile`).

To change a configuration value on the command line, pass that configuration task label, name and value to the ``--config`` argument.
For example, change the mesh value from the previous example:

.. code-block:: bash

   pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show config="isr::mesh*" --config isr:qa.flatness.meshY=512

   ### Configuration for task `isr'
   # Mesh size in X for flatness statistics
   config.qa.flatness.meshX=256

   # Mesh size in Y for flatness statistics
   config.qa.flatness.meshY=512

You can provide multiple ``--config`` arguments on the same command line as distinct ``--config`` options:

.. code-block:: bash

   pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show config="isr::mesh*" --config isr:qa.flatness.meshY=512 -c isr:qa.flatness.meshX=128

   ### Configuration for task `isr'
   # Mesh size in X for flatness statistics
   config.qa.flatness.meshX=128

   # Mesh size in Y for flatness statistics
   config.qa.flatness.meshY=512

Only simple configuration values can be set through ``--config`` arguments, such as:

- **String values**. For example: ``--config task:configName="value"``.
- **Scalar numbers**. For example: ``--config task:configName=2.5``.
- **Lists of integers**. For example: ``--config task:intList=[2,4,-87]``.
- **Lists of floating point numbers**. For example: ``--config task:floatList=[3.14,-5.6e7]``.
- **Lists of strings**, For example: ``--config task:strList=[BAD,GOOD]``.
- **Boolean values**. For example: ``--config task:configName=True configName2=False``.

The ``[]`` are optional when specifying lists.

Specific types of configurations you **cannot** perform with the ``--config`` argument are:

- You cannot retarget a subtask specified by a `lsst.pex.config.ConfigurableField` (which is the most common case).
- For items in registries, you can only specify values for the active (current) item.
- You cannot specify a subset of a list.
  You must specify all values at once.

For these more complex configuration types you must use configuration files, which are evaluated as Python code.

.. _command-line-config-howto-configfile:

How to use configuration files
==============================

You can also provide configurations to a command-line through a *configuration file*.
In fact, configuration files are Python modules; anything you can do in Python you can do in a configuration file.

Configuration files give you full access to the configuration API, allowing you to import and retarget subtasks, and set configurations with complex types.
These configurations can only be done through configuration files, not through command-line arguments.

Use a configuration file by providing its file path through a ``-C`` / ``--config-file`` argument:

.. code-block:: bash

   pipetask build --config-file isr:myisr.py

The task label must be associated with the configuration file to let the command know which task is being over-ridden.

Multiple configuration files can be provided through the same ``--config-file`` argument by separating with commas, and the ``--config-file`` argument itself can be repeated.
The configuration parameters are applied in the order they appear on the command line.

In a configuration file, configurations are attributes of a ``config`` object.
If on the command line you set a configuration with a ``--config task:skyMap.projection="TAN"`` argument, in a configuration file the equivalent statement is:

.. code-block:: python

   config.skyMap.projection = "TAN"

``config`` is the root configuration object for the pipeline task.
Settings for the task itself are attributes of ``config``.
In that example, ``config.skyMap`` is a subtask and ``projection`` is a configuration of that ``skyMap`` subtask.

.. _command-line-config-howto-obs:

About configuration defaults and instrument configuration override files
========================================================================

Command-line configurations are a combination of configurations you provide and defaults from the observatory package and the task itself.

When a command-line is run, it loads two instrument-specific configuration files, if found: one for the observatory package, and one for a specific camera defined in that observatory package.
For an example observatory package named ``obs_package``, these configuration override files are, in order:

- ``obs_package/config/taskName.py`` (overrides for an observatory package in general).
- ``obs_package/config/cameraName/taskName.py`` (overrides for a specific instrument, named “\ ``cameraName``\ ”).

The ``taskName`` is the pipeline task label and can either come from the name set in the task itself, or from an override in the pipeline definition.

Here are two examples:

- :file:`obs_lsst/config/makeWarp.py`: specifies which parameters are preferred when warping images using the ``obs_lsst`` observatory package.
- :file:`obs_lsst/config/latiss/isr.py``: provides overrides for the instrument signature removal (aka detrending) task for the ``latiss`` camera in the ``obs_lsst`` observatory package.

Overall, the priority order for setting task configurations is configurations is (highest priority first):

#. User-provided ``--config`` and ``--config-file`` arguments (computed left-to-right).
#. Directly in a pipeline YAML definition.
#. Camera specific configuration override file in an observatory package.
#. General configuration override file in an observatory package.
#. Task defaults.

.. _command-line-config-howto-history:

Determining the history of a config parameter
=============================================

Sometimes you aren't sure where a value for a configuration parameter is coming from, given all the many places where a parameter can be overridden.
To investigate the history of a parameter you can use the ``--show history`` option.
This option takes a task label and a pattern to use to match a configuration parameter.
The output is quite verbose since it reports the entire code hierarchy involved in setting the parameter.
In this example we show how the ``assembleCcd.keysToRemove`` parameter defaults to an empty list, then values are set from the ``obs_subaru`` override, and then finally the command-line value overrides everything:

.. code-block:: bash

   pipetask build -p ${DRP_PIPE_DIR}/pipelines/HSC/pipelines_check.yaml --show "history=isr::*keysToRemove" -c "isr:assembleCcd.keysToRemove=[A,B]"
   ### Configuration field for task `isr'
   assembleCcd.keysToRemove
   []                                               $CTRL_MPEXEC_DIR/bin/pipetask:29                                                     sys.exit(main())
                                                   ctrl/mpexec/cli/pipetask.py:51
                                                   daf/butler/cli/utils.py:1069
                                                   ctrl/mpexec/cli/cmd/commands.py:106
                                                   ctrl/mpexec/cli/script/build.py:89
                                                   ctrl/mpexec/showInfo.py:138
                                                   ctrl/mpexec/showInfo.py:226
                                                   ctrl/mpexec/util.py:143
                                                   pipe/base/pipeline.py:668
                                                   pipe/base/pipeline.py:691
                                                   pex/config/configurableField.py:421
                                                   pex/config/configurableField.py:421
                                                   ip/isr/assembleCcdTask.py:39
   []                                               $CTRL_MPEXEC_DIR/bin/pipetask:29                                                     sys.exit(main())
                                                   ctrl/mpexec/cli/pipetask.py:51
                                                   daf/butler/cli/utils.py:1069
                                                   ctrl/mpexec/cli/cmd/commands.py:106
                                                   ctrl/mpexec/cli/script/build.py:89
                                                   ctrl/mpexec/showInfo.py:138
                                                   ctrl/mpexec/showInfo.py:226
                                                   ctrl/mpexec/util.py:143
                                                   pipe/base/pipeline.py:668
                                                   pipe/base/pipeline.py:691
                                                   pex/config/configurableField.py:421
                                                   pex/config/configurableField.py:421
   ['PC001001', 'PC001002', 'PC002001', 'PC002002'] $CTRL_MPEXEC_DIR/bin/pipetask:29                                                     sys.exit(main())
                                                   ctrl/mpexec/cli/pipetask.py:51
                                                   daf/butler/cli/utils.py:1069
                                                   ctrl/mpexec/cli/cmd/commands.py:106
                                                   ctrl/mpexec/cli/script/build.py:89
                                                   ctrl/mpexec/showInfo.py:138
                                                   ctrl/mpexec/showInfo.py:226
                                                   ctrl/mpexec/util.py:143
                                                   pipe/base/pipeline.py:668
                                                   pipe/base/pipeline.py:711
                                                   pipe/base/configOverrides.py:299
                                                   pipe/base/_instrument.py:298
                                                   pex/config/config.py:1167
                                                   pex/config/config.py:1251
                                                   $EUPS_PATH/Darwin/obs_subaru/g56afc215e3+576f275b99/config/isr.py:33    config.assembleCcd.keysToRemove = ["PC001001", "PC001002", "PC002001", "PC002002"]
   ['A', 'B']                                       $CTRL_MPEXEC_DIR/bin/pipetask:29                                                     sys.exit(main())
                                                   ctrl/mpexec/cli/pipetask.py:51
                                                   daf/butler/cli/utils.py:1069
                                                   ctrl/mpexec/cli/cmd/commands.py:106
                                                   ctrl/mpexec/cli/script/build.py:89
                                                   ctrl/mpexec/showInfo.py:138
                                                   ctrl/mpexec/showInfo.py:226
                                                   ctrl/mpexec/util.py:143
                                                   pipe/base/pipeline.py:668
                                                   pipe/base/pipeline.py:711
                                                   pipe/base/configOverrides.py:285
