"""Module which defines TaskFactory class and related methods.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["TaskFactory"]

from builtins import object

import lsst.log as lsstLog

_LOG = lsstLog.Log.getLogger(__name__)


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
        """Locate and import SuperTask class.

        Returns tuple of task class and its full name, `None` is returned
        for both if loading fails.

        Parameters
        ----------
        taskName : `str`
            Name of the SuperTask class, interpretation depends entirely on
            activator, e.g. it may or may not include dots.

        Returns
        -------
        taskClass : `type`
            SuperTask class object, or None on failure.
        taskName : `str`
            Full task class name including package and module, or None on
            failure.

        Raises
        ------
        `ImportError` is raised if task classes cannot be imported.
        `TypeError` is raised if imported task is not a SuperTask.
        """

        # load the class, this will raise ImportError on failure
        taskClass, fullTaskName, taskKind = self.taskLoader.loadTaskClass(taskName)
        if taskKind != 'SuperTask':
            raise TypeError("Task class {} is not a SuperTask".format(fullTaskName))

        return taskClass, fullTaskName

    def makeTask(self, taskClass, config, overrides):
        """Create new SuperTask instance from its class.

        Parameters
        ----------
        taskClass : type
            SuperTask class.
        config : `pex.Config` or None
            Configuration object, if ``None`` then use task-defined
            configuration class to create new instance.
        overrides : `ConfigOverrides` or None
            Configuration overrides, this should contain all overrides to be
            applied to a default task config, including camera-specific,
            obs-package specific, and possibly command-line overrides.

        Returns
        -------
        Instance of a SuperTask class or None on errors.

        Raises
        ------
        Any exceptions that are raised by SuperTask constructor or its
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

        # make task instance
        task = taskClass(config=config)

        return task
