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
from .quantumGraphExecutor import QuantumExecutor
from lsst.log import Log
from lsst.pipe.base import ButlerQuantumContext

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


class SingleQuantumExecutor(QuantumExecutor):
    """Executor class which runs one Quantum at a time.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Instance of a task factory.
    skipExisting : `bool`, optional
        If True then quanta with all existing outputs are not executed.
    enableLsstDebug : `bool`, optional
        Enable debugging with ``lsstDebug`` facility for a task.
    """
    def __init__(self, taskFactory, skipExisting=False, enableLsstDebug=False):
        self.taskFactory = taskFactory
        self.skipExisting = skipExisting
        self.enableLsstDebug = enableLsstDebug

    def execute(self, taskDef, quantum, butler):
        # Docstring inherited from QuantumExecutor.execute
        taskClass, config = taskDef.taskClass, taskDef.config
        self.setupLogging(taskClass, config, quantum)
        if self.skipExisting and self.quantumOutputsExist(quantum, butler):
            _LOG.info("Quantum execution skipped due to existing outputs, "
                      f"task={taskClass.__name__} dataId={quantum.dataId}.")
            return
        self.updateQuantumInputs(quantum, butler)

        # enable lsstDebug debugging
        if self.enableLsstDebug:
            try:
                _LOG.debug("Will try to import debug.py")
                import debug  # noqa:F401
            except ImportError:
                _LOG.warn("No 'debug' module found.")

        task = self.makeTask(taskClass, config, butler)
        self.runQuantum(task, quantum, taskDef, butler)

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

    def quantumOutputsExist(self, quantum, butler):
        """Decide whether this quantum needs to be executed.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Quantum to check for existing outputs
        butler : `~lsst.daf.butler.Butler`
            Data butler.

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
        collection = butler.run
        registry = butler.registry

        existingRefs = []
        missingRefs = []
        for datasetRefs in quantum.outputs.values():
            for datasetRef in datasetRefs:
                ref = registry.findDataset(datasetRef.datasetType, datasetRef.dataId,
                                           collections=butler.run)
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

    def makeTask(self, taskClass, config, butler):
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
        butler : `~lsst.daf.butler.Butler`
            Data butler.
        """
        # call task factory for that
        return self.taskFactory.makeTask(taskClass, config, None, butler)

    def updateQuantumInputs(self, quantum, butler):
        """Update quantum with extra information.

        Some methods may require input DatasetRefs to have non-None
        ``dataset_id``, but in case of intermediate dataset it may not be
        filled during QuantumGraph construction. This method will retrieve
        missing info from registry.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        butler : `~lsst.daf.butler.Butler`
            Data butler.
        """
        for refsForDatasetType in quantum.predictedInputs.values():
            newRefsForDatasetType = []
            for ref in refsForDatasetType:
                if ref.id is None:
                    resolvedRef = butler.registry.findDataset(ref.datasetType, ref.dataId,
                                                              collections=butler.collections)
                    if resolvedRef is None:
                        raise ValueError(
                            f"Cannot find {ref.datasetType.name} with id {ref.dataId} "
                            f"in collections {butler.collections}."
                        )
                    newRefsForDatasetType.append(resolvedRef)
                    _LOG.debug("Updating dataset ID for %s", ref)
                else:
                    newRefsForDatasetType.append(ref)
            refsForDatasetType[:] = newRefsForDatasetType

    def runQuantum(self, task, quantum, taskDef, butler):
        """Execute task on a single quantum.

        Parameters
        ----------
        task : `~lsst.pipe.base.PipelineTask`
            Task object.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.
        butler : `~lsst.daf.butler.Butler`
            Data butler.
        """
        # Create a butler that operates in the context of a quantum
        butlerQC = ButlerQuantumContext(butler, quantum)

        # Get the input and output references for the task
        connectionInstance = task.config.connections.ConnectionsClass(config=task.config)
        inputRefs, outputRefs = connectionInstance.buildDatasetRefs(quantum)
        # Call task runQuantum() method. Any exception thrown by the task
        # propagates to caller.
        task.runQuantum(butlerQC, inputRefs, outputRefs)

        if taskDef.metadataDatasetName is not None:
            # DatasetRef has to be in the Quantum outputs, can lookup by name
            try:
                ref = quantum.outputs[taskDef.metadataDatasetName]
            except LookupError as exc:
                raise LookupError(
                    f"Quantum outputs is missing metadata dataset type {taskDef.metadataDatasetName},"
                    f" it could happen due to inconsistent options between Quantum generation"
                    f" and execution") from exc
            butlerQC.put(task.metadata, ref[0])
