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
from lsst.pipe.base import ButlerQuantumContext

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
    skipExisting : `bool`, optional
        If True then quanta with all existing outputs are not executed.
    clobberOutput : `bool`, optional
        It `True` then override all existing output datasets in an output
        collection.
    """
    def __init__(self, butler, taskFactory, skipExisting=False, clobberOutput=False):
        self.butler = butler
        self.taskFactory = taskFactory
        self.skipExisting = skipExisting
        self.clobberOutput = clobberOutput

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
        if self.clobberOutput:
            self.doClobberOutputs(quantum)
        if self.skipExisting and self.quantumOutputsExist(quantum):
            _LOG.info("Quantum execution skipped due to existing outputs, "
                      f"task={taskClass.__name__} dataId={quantum.dataId}.")
            return
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

    def doClobberOutputs(self, quantum):
        """Delete any outputs that already exist for a Quantum.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Quantum to check for existing outputs.
        """
        collection = self.butler.run.collection
        registry = self.butler.registry

        existingRefs = []
        for datasetRefs in quantum.outputs.values():
            for datasetRef in datasetRefs:
                ref = registry.find(collection, datasetRef.datasetType, datasetRef.dataId)
                if ref is not None:
                    existingRefs.append(ref)
        for ref in existingRefs:
            _LOG.debug("Removing existing dataset: %s", ref)
            self.butler.remove(ref)

    def quantumOutputsExist(self, quantum):
        """Decide whether this quantum needs to be executed.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Quantum to check for existing outputs

        Returns
        -------
        exist : `bool`
            True if all quantum's outputs exist in a collection, False
            otherwise.

        Raises
        ------
        RuntimeError
            Raised if some outputs exist and some not.
        """
        collection = self.butler.run.collection
        registry = self.butler.registry

        existingRefs = []
        missingRefs = []
        for datasetRefs in quantum.outputs.values():
            for datasetRef in datasetRefs:
                ref = registry.find(collection, datasetRef.datasetType, datasetRef.dataId)
                if ref is None:
                    missingRefs.append(datasetRefs)
                else:
                    existingRefs.append(datasetRefs)
        if existingRefs and missingRefs:
            # some outputs exist and same not, can't do a thing with that
            raise RuntimeError(f"Registry inconsistency while checking for existing outputs:"
                               f" collection={collection} existingRefs={existingRefs}"
                               f" missingRefs={missingRefs}")
        else:
            return bool(existingRefs)

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
                    if storedRef is None:
                        raise ValueError(
                            f"Cannot find {ref.datasetType.name} with id {ref.dataId} "
                            f"in collection {butler.collection}."
                        )
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
        # Create a butler that operates in the context of a quantum
        butlerQC = ButlerQuantumContext(self.butler, quantum)

        # Get the input and output references for the task
        connectionInstance = task.config.connections.ConnectionsClass(config=task.config)
        inputRefs, outputRefs = connectionInstance.buildDatasetRefs(quantum)
        # Call task runQuantum() method. Any exception thrown by the task
        # propagates to caller.
        task.runQuantum(butlerQC, inputRefs, outputRefs)
