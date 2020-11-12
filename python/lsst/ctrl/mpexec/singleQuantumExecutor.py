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
from collections import defaultdict
import logging
from itertools import chain
import time

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .quantumGraphExecutor import QuantumExecutor
from lsst.log import Log
from lsst.obs.base import Instrument
from lsst.pipe.base import ButlerQuantumContext
from lsst.daf.butler import Quantum

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
    clobberPartialOutputs : `bool`, optional
        If True then delete any partial outputs from quantum execution. If
        complete outputs exists then exception is raise if ``skipExisting`` is
        False.
    enableLsstDebug : `bool`, optional
        Enable debugging with ``lsstDebug`` facility for a task.
    """
    def __init__(self, taskFactory, skipExisting=False, clobberPartialOutputs=False, enableLsstDebug=False):
        self.taskFactory = taskFactory
        self.skipExisting = skipExisting
        self.enableLsstDebug = enableLsstDebug
        self.clobberPartialOutputs = clobberPartialOutputs

    def execute(self, taskDef, quantum, butler):
        # Docstring inherited from QuantumExecutor.execute
        self.setupLogging(taskDef, quantum)
        taskClass, config = taskDef.taskClass, taskDef.config

        # check whether to skip or delete old outputs
        if self.checkExistingOutputs(quantum, butler, taskDef):
            _LOG.info("Quantum execution skipped due to existing outputs, "
                      f"task={taskClass.__name__} dataId={quantum.dataId}.")
            return

        quantum = self.updatedQuantumInputs(quantum, butler)

        # enable lsstDebug debugging
        if self.enableLsstDebug:
            try:
                _LOG.debug("Will try to import debug.py")
                import debug  # noqa:F401
            except ImportError:
                _LOG.warn("No 'debug' module found.")

        # initialize global state
        self.initGlobals(quantum, butler)

        # Ensure that we are executing a frozen config
        config.freeze()

        task = self.makeTask(taskClass, config, butler)
        self.runQuantum(task, quantum, taskDef, butler)

    def setupLogging(self, taskDef, quantum):
        """Configure logging system for execution of this task.

        Ths method can setup logging to attach task- or
        quantum-specific information to log messages. Potentially this can
        take into account some info from task configuration as well.

        Parameters
        ----------
        taskDef : `lsst.pipe.base.TaskDef`
            The task definition.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        """
        # include quantum dataId and task label into MDC
        label = taskDef.label
        if quantum.dataId:
            label += f":{quantum.dataId}"
        Log.MDC("LABEL", label)

    def checkExistingOutputs(self, quantum, butler, taskDef):
        """Decide whether this quantum needs to be executed.

        If only partial outputs exist then they are removed if
        ``clobberPartialOutputs`` is True, otherwise an exception is raised.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Quantum to check for existing outputs
        butler : `~lsst.daf.butler.Butler`
            Data butler.
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.

        Returns
        -------
        exist : `bool`
            True if all quantum's outputs exist in a collection and
            ``skipExisting`` is True, False otherwise.

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
                    missingRefs.append(datasetRef)
                else:
                    existingRefs.append(ref)
        if existingRefs and missingRefs:
            # some outputs exist and some don't, either delete existing ones or complain
            _LOG.debug("Partial outputs exist for task %s dataId=%s collection=%s "
                       "existingRefs=%s missingRefs=%s",
                       taskDef, quantum.dataId, collection, existingRefs, missingRefs)
            if self.clobberPartialOutputs:
                _LOG.info("Removing partial outputs for task %s: %s", taskDef, existingRefs)
                butler.pruneDatasets(existingRefs, disassociate=True, unstore=True, purge=True)
                return False
            else:
                raise RuntimeError(f"Registry inconsistency while checking for existing outputs:"
                                   f" collection={collection} existingRefs={existingRefs}"
                                   f" missingRefs={missingRefs}")
        elif existingRefs:
            # complete outputs exist, this is fine only if skipExisting is set
            return self.skipExisting
        else:
            # no outputs exist
            return False

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

    def updatedQuantumInputs(self, quantum, butler):
        """Update quantum with extra information, returns a new updated Quantum.

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

        Returns
        -------
        update : `~lsst.daf.butler.Quantum`
            Updated Quantum instance
        """
        updatedInputs = defaultdict(list)
        for key, refsForDatasetType in quantum.inputs.items():
            newRefsForDatasetType = updatedInputs[key]
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
        return Quantum(taskName=quantum.taskName,
                       taskClass=quantum.taskClass,
                       dataId=quantum.dataId,
                       initInputs=quantum.initInputs,
                       inputs=updatedInputs,
                       outputs=quantum.outputs
                       )

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
        inputRefs, outputRefs = taskDef.connections.buildDatasetRefs(quantum)

        startTime = time.time()

        # Call task runQuantum() method. Any exception thrown by the task
        # propagates to caller.
        task.runQuantum(butlerQC, inputRefs, outputRefs)

        stopTime = time.time()
        _LOG.info("Execution of task '%s' on quantum %s took %.3f seconds",
                  taskDef.label, quantum.dataId, stopTime - startTime)

        if taskDef.metadataDatasetName is not None:
            # DatasetRef has to be in the Quantum outputs, can lookup by name
            try:
                ref = quantum.outputs[taskDef.metadataDatasetName]
            except LookupError as exc:
                raise LookupError(
                    f"Quantum outputs is missing metadata dataset type {taskDef.metadataDatasetName},"
                    f" it could happen due to inconsistent options between Quantum generation"
                    f" and execution") from exc
            butlerQC.put(task.getFullMetadata(), ref[0])

    def initGlobals(self, quantum, butler):
        """Initialize global state needed for task execution.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        butler : `~lsst.daf.butler.Butler`
            Data butler.

        Notes
        -----
        There is an issue with initializing filters singleton which is done
        by instrument, to avoid requiring tasks to do it in runQuantum()
        we do it here when any dataId has an instrument dimension. Also for
        now we only allow single instrument, verify that all instrument
        names in all dataIds are identical.

        This will need revision when filter singleton disappears.
        """
        oneInstrument = None
        for datasetRefs in chain(quantum.inputs.values(), quantum.outputs.values()):
            for datasetRef in datasetRefs:
                dataId = datasetRef.dataId
                instrument = dataId.get("instrument")
                if instrument is not None:
                    if oneInstrument is not None:
                        assert instrument == oneInstrument, \
                            "Currently require that only one instrument is used per graph"
                    else:
                        oneInstrument = instrument
                        Instrument.fromName(instrument, butler.registry)
