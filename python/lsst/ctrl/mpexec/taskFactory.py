# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module which defines TaskFactory class and related methods.
"""

__all__ = ["TaskFactory"]

import logging

from .taskLoader import KIND_PIPELINETASK
from lsst.pipe.base import TaskFactory as BaseTaskFactory

_LOG = logging.getLogger(__name__.partition(".")[2])


class TaskFactory(BaseTaskFactory):
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
            Name of the PipelineTask class, this class requires it to include
            module and optionally package names (dot-separated).

        Returns
        -------
        taskClass : `type`
            PipelineTask class object, or None on failure.
        taskName : `str`
            Full task class name including package and module, or None on
            failure.

        Raises
        ------
        ImportError
            Raised if task classes cannot be imported.
        TypeError
            Raised if imported task is not a PipelineTask.
        ValueError
            Raised if ``taskName`` does not include module name.
        """
        if "." not in taskName:
            raise ValueError("Module name must be included in task name")

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
            descriptorMap = taskClass.getInitInputDatasetTypes(config)
            initInputs = {k: butler.get(v.datasetType) for k, v in descriptorMap.items()}

        # make task instance
        task = taskClass(config=config, initInputs=initInputs)

        return task
