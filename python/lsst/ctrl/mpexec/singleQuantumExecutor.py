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

__all__ = ['SingleQuantumExecutor']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
from itertools import chain

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.log import Log

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


class SingleQuantumExecutor:
    """Executor class which runs one Quantum at a time.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Instance of a task factory.
    """
    def __init__(self, butler, taskFactory):
        self.butler = butler
        self.taskFactory = taskFactory

    def execute(self, taskClass, config, quantum):
        """Execute PipelineTask on a single Quantum.

        Parameters
        ----------
        taskClass : `type`
            Sub-class of `~lsst.pipe.base.PipelineTask`.
        config : `~lsst.pipe.base.PipelineTaskConfig`
            Configuration object for this task
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        """
        self.setupLogging(taskClass, config, quantum)
        self.updateQuantumInputs(quantum)
        task = self.makeTask(taskClass, config)
        self.runQuantum(task, quantum)

    def setupLogging(self, taskClass, config, quantum):
        """Configure logging system for execution of this task.

        Ths method can setup logging to attach task- or
        quantum-specific information to log messages. Potentially this can
        take into accout some info from task configuration as well.

        Parameters
        ----------
        taskClass : `type`
            Sub-class of `~lsst.pipe.base.PipelineTask`.
        config : `~lsst.pipe.base.PipelineTaskConfig`
            Configuration object for this task
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        """
        # include input dataIds into MDC
        dataIds = set(ref.dataId for ref in chain.from_iterable(quantum.predictedInputs.values()))
        if dataIds:
            if len(dataIds) == 1:
                Log.MDC("LABEL", str(dataIds.pop()))
            else:
                Log.MDC("LABEL", '[' + ', '.join([str(dataId) for dataId in dataIds]) + ']')

    def makeTask(self, taskClass, config):
        """Make new task instance.

        Parameters
        ----------
        taskClass : `type`
            Sub-class of `~lsst.pipe.base.PipelineTask`.
        config : `~lsst.pipe.base.PipelineTaskConfig`
            Configuration object for this task

        Returns
        -------
        task : `~lsst.pipe.base.PipelineTask`
            Instance of ``taskClass`` type.
        """
        # call task factory for that
        return self.taskFactory.makeTask(taskClass, config, None, self.butler)

    def updateQuantumInputs(self, quantum):
        """Update quantum with extra information.

        Some methods may require input DatasetRefs to have non-None
        ``dataset_id``, but in case of intermediate dataset it may not be
        filled during QuantumGraph construction. This method will retrieve
        missing info from registry.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        """
        butler = self.butler
        for refs in quantum.predictedInputs.values():
            for ref in refs:
                if ref.id is None:
                    storedRef = butler.registry.find(butler.collection, ref.datasetType, ref.dataId)
                    ref._id = storedRef.id
                    _LOG.debug("Updated dataset ID for %s", ref)

    def runQuantum(self, task, quantum):
        """Execute task on a single quantum.

        Parameters
        ----------
        task : `~lsst.pipe.base.PipelineTask`
            Task object.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        """
        # Call task runQuantum() method. Any exception thrown by the task
        # propagates to caller.
        task.runQuantum(quantum, self.butler)
