"""Module which defines TaskFactory class and related methods.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["TaskFactory"]

from builtins import object

from .taskLoader import KIND_PIPELINETASK
import lsst.log

_LOG = lsst.log.Log.getLogger(__name__)


class TaskFactory(object):
    """Class instantiating super-tasks.

    Parameters
    ----------
    taskLoder : TaskLoader
        Instance of task loader responsible for imports of task classes.
    """
    def __init__(self, taskLoader):
        self.taskLoader = taskLoader

    def loadTaskClass(self, taskName):
        """Locate and import PipelineTask class.

        Returns tuple of task class and its full name, `None` is returned
        for both if loading fails.

        Parameters
        ----------
        taskName : `str`
            Name of the PipelineTask class, interpretation depends entirely on
            activator, e.g. it may or may not include dots.

        Returns
        -------
        taskClass : `type`
            PipelineTask class object, or None on failure.
        taskName : `str`
            Full task class name including package and module, or None on
            failure.

        Raises
        ------
        `ImportError` is raised if task classes cannot be imported.
        `TypeError` is raised if imported task is not a PipelineTask.
        """

        # load the class, this will raise ImportError on failure
        taskClass, fullTaskName, taskKind = self.taskLoader.loadTaskClass(taskName)
        if taskKind != KIND_PIPELINETASK:
            raise TypeError("Task class {} is not a PipelineTask".format(fullTaskName))

        return taskClass, fullTaskName

    def makeTask(self, taskClass, config, overrides, butler):
        """Create new PipelineTask instance from its class.

        Parameters
        ----------
        taskClass : type
            PipelineTask class.
        config : `pex.Config` or None
            Configuration object, if ``None`` then use task-defined
            configuration class to create new instance.
        overrides : `ConfigOverrides` or None
            Configuration overrides, this should contain all overrides to be
            applied to a default task config, including instrument-specific,
            obs-package specific, and possibly command-line overrides.
        butler : `lsst.daf.butler.Butler` or None
            Butler instance used to obtain initialization inputs for
            PipelineTasks.  If None, some PipelineTasks will not be usable

        Returns
        -------
        Instance of a PipelineTask class or None on errors.

        Raises
        ------
        Any exceptions that are raised by PipelineTask constructor or its
        configuration class are propagated back to caller.
        """

        # configuration
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        elif overrides is not None:
            _LOG.waring("Both config and overrides are specified for task %s, overrides are ignored",
                        taskClass.__name__)

        # if we don't have a butler, try to construct without initInputs;
        # let PipelineTasks raise if that's impossible
        if butler is None:
            initInputs = None
        else:
            initInputs = {k: butler.get(v) for k, v in taskClass.getInitInputDatasetTypes(config).items()}

        # make task instance
        task = taskClass(config=config, initInputs=initInputs)

        return task
