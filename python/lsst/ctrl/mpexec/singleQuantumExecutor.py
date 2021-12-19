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

__all__ = ["SingleQuantumExecutor"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import os
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from contextlib import contextmanager
from itertools import chain
from logging import FileHandler
from typing import List

from lsst.daf.butler import DatasetRef, DatasetType, FileDataset, NamedKeyDict, Quantum
from lsst.daf.butler.core.logging import ButlerLogRecordHandler, ButlerLogRecords, ButlerMDC, JsonLogFormatter
from lsst.obs.base import Instrument
from lsst.pipe.base import (
    AdjustQuantumHelper,
    ButlerQuantumContext,
    InvalidQuantumError,
    NoWorkFound,
    RepeatableQuantumError,
)

# During metadata transition phase, determine metadata class by
# asking pipe_base
from lsst.pipe.base.task import _TASK_FULL_METADATA_TYPE, _TASK_METADATA_TYPE
from lsst.utils.timer import logInfo

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .quantumGraphExecutor import QuantumExecutor

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)


class _LogCaptureFlag:
    """Simple flag to enable/disable log-to-butler saving."""

    store: bool = True


class SingleQuantumExecutor(QuantumExecutor):
    """Executor class which runs one Quantum at a time.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Instance of a task factory.
    skipExistingIn : `list` [ `str` ], optional
        Accepts list of collections, if all Quantum outputs already exist in
        the specified list of collections then that Quantum will not be rerun.
    clobberOutputs : `bool`, optional
        If `True`, then existing outputs in output run collection will be
        overwritten.  If ``skipExistingIn`` is defined, only outputs from
        failed quanta will be overwritten.
    enableLsstDebug : `bool`, optional
        Enable debugging with ``lsstDebug`` facility for a task.
    exitOnKnownError : `bool`, optional
        If `True`, call `sys.exit` with the appropriate exit code for special
        known exceptions, after printing a traceback, instead of letting the
        exception propagate up to calling.  This is always the behavior for
        InvalidQuantumError.
    """

    stream_json_logs = True
    """If True each log record is written to a temporary file and ingested
    when quantum completes. If False the records are accumulated in memory
    and stored in butler on quantum completion."""

    def __init__(
        self,
        taskFactory,
        skipExistingIn=None,
        clobberOutputs=False,
        enableLsstDebug=False,
        exitOnKnownError=False,
    ):
        self.taskFactory = taskFactory
        self.skipExistingIn = skipExistingIn
        self.enableLsstDebug = enableLsstDebug
        self.clobberOutputs = clobberOutputs
        self.exitOnKnownError = exitOnKnownError
        self.log_handler = None

    def execute(self, taskDef, quantum, butler):
        # Docstring inherited from QuantumExecutor.execute
        startTime = time.time()

        with self.captureLogging(taskDef, quantum, butler) as captureLog:

            # Save detailed resource usage before task start to metadata.
            quantumMetadata = _TASK_METADATA_TYPE()
            logInfo(None, "prep", metadata=quantumMetadata)

            taskClass, label, config = taskDef.taskClass, taskDef.label, taskDef.config

            # check whether to skip or delete old outputs, if it returns True
            # or raises an exception do not try to store logs, as they may be
            # already in butler.
            captureLog.store = False
            if self.checkExistingOutputs(quantum, butler, taskDef):
                _LOG.info(
                    "Skipping already-successful quantum for label=%s dataId=%s.", label, quantum.dataId
                )
                return
            captureLog.store = True

            try:
                quantum = self.updatedQuantumInputs(quantum, butler, taskDef)
            except NoWorkFound as exc:
                _LOG.info(
                    "Nothing to do for task '%s' on quantum %s; saving metadata and skipping: %s",
                    taskDef.label,
                    quantum.dataId,
                    str(exc),
                )
                # Make empty metadata that looks something like what a
                # do-nothing task would write (but we don't bother with empty
                # nested PropertySets for subtasks).  This is slightly
                # duplicative with logic in pipe_base that we can't easily call
                # from here; we'll fix this on DM-29761.
                logInfo(None, "end", metadata=quantumMetadata)
                fullMetadata = _TASK_FULL_METADATA_TYPE()
                fullMetadata[taskDef.label] = _TASK_METADATA_TYPE()
                fullMetadata["quantum"] = quantumMetadata
                self.writeMetadata(quantum, fullMetadata, taskDef, butler)
                return

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
            logInfo(None, "init", metadata=quantumMetadata)
            task = self.makeTask(taskClass, label, config, butler)
            logInfo(None, "start", metadata=quantumMetadata)
            try:
                self.runQuantum(task, quantum, taskDef, butler)
            except Exception as e:
                _LOG.exception(
                    "Execution of task '%s' on quantum %s failed. Exception %s: %s",
                    taskDef.label,
                    quantum.dataId,
                    e.__class__.__name__,
                    str(e),
                )
                raise
            logInfo(None, "end", metadata=quantumMetadata)
            fullMetadata = task.getFullMetadata()
            fullMetadata["quantum"] = quantumMetadata
            self.writeMetadata(quantum, fullMetadata, taskDef, butler)
            stopTime = time.time()
            _LOG.info(
                "Execution of task '%s' on quantum %s took %.3f seconds",
                taskDef.label,
                quantum.dataId,
                stopTime - startTime,
            )
        return quantum

    @contextmanager
    def captureLogging(self, taskDef, quantum, butler):
        """Configure logging system to capture logs for execution of this task.

        Parameters
        ----------
        taskDef : `lsst.pipe.base.TaskDef`
            The task definition.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        butler : `~lsst.daf.butler.Butler`
            Butler to write logs to.

        Notes
        -----
        Expected to be used as a context manager to ensure that logging
        records are inserted into the butler once the quantum has been
        executed:

        .. code-block:: py

           with self.captureLogging(taskDef, quantum, butler):
               # Run quantum and capture logs.

        Ths method can also setup logging to attach task- or
        quantum-specific information to log messages. Potentially this can
        take into account some info from task configuration as well.
        """
        # Add a handler to the root logger to capture execution log output.
        # How does it get removed reliably?
        if taskDef.logOutputDatasetName is not None:
            # Either accumulate into ButlerLogRecords or stream
            # JSON records to file and ingest that.
            tmpdir = None
            if self.stream_json_logs:
                # Create the log file in a temporary directory rather than
                # creating a temporary file. This is necessary because
                # temporary files are created with restrictive permissions
                # and during file ingest these permissions persist in the
                # datastore. Using a temp directory allows us to create
                # a file with umask default permissions.
                tmpdir = tempfile.mkdtemp(prefix="butler-temp-logs-")

                # Construct a file to receive the log records and "touch" it.
                log_file = os.path.join(tmpdir, f"butler-log-{taskDef.label}.json")
                with open(log_file, "w"):
                    pass
                self.log_handler = FileHandler(log_file)
                self.log_handler.setFormatter(JsonLogFormatter())
            else:
                self.log_handler = ButlerLogRecordHandler()

            logging.getLogger().addHandler(self.log_handler)

        # include quantum dataId and task label into MDC
        label = taskDef.label
        if quantum.dataId:
            label += f":{quantum.dataId}"

        ctx = _LogCaptureFlag()
        try:
            with ButlerMDC.set_mdc({"LABEL": label, "RUN": butler.run}):
                yield ctx
        finally:
            # Ensure that the logs are stored in butler.
            self.writeLogRecords(quantum, taskDef, butler, ctx.store)
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

    def checkExistingOutputs(self, quantum, butler, taskDef):
        """Decide whether this quantum needs to be executed.

        If only partial outputs exist then they are removed if
        ``clobberOutputs`` is True, otherwise an exception is raised.

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
            `True` if ``self.skipExistingIn`` is defined, and a previous
            execution of this quanta appears to have completed successfully
            (either because metadata was written or all datasets were written).
            `False` otherwise.

        Raises
        ------
        RuntimeError
            Raised if some outputs exist and some not.
        """
        if self.skipExistingIn and taskDef.metadataDatasetName is not None:
            # Metadata output exists; this is sufficient to assume the previous
            # run was successful and should be skipped.
            ref = butler.registry.findDataset(
                taskDef.metadataDatasetName, quantum.dataId, collections=self.skipExistingIn
            )
            if ref is not None:
                if butler.datastore.exists(ref):
                    return True

        # Previously we always checked for existing outputs in `butler.run`,
        # now logic gets more complicated as we only want to skip quantum
        # whose outputs exist in `self.skipExistingIn` but pruning should only
        # be done for outputs existing in `butler.run`.

        def findOutputs(collections):
            """Find quantum outputs in specified collections."""
            existingRefs = []
            missingRefs = []
            for datasetRefs in quantum.outputs.values():
                for datasetRef in datasetRefs:
                    ref = butler.registry.findDataset(
                        datasetRef.datasetType, datasetRef.dataId, collections=collections
                    )
                    if ref is not None and butler.datastore.exists(ref):
                        existingRefs.append(ref)
                    else:
                        missingRefs.append(datasetRef)
            return existingRefs, missingRefs

        existingRefs, missingRefs = findOutputs(self.skipExistingIn)
        if self.skipExistingIn:
            if existingRefs and not missingRefs:
                # everything is already there
                return True

        # If we are to re-run quantum then prune datasets that exists in
        # output run collection, only if `self.clobberOutputs` is set.
        if existingRefs:
            existingRefs, missingRefs = findOutputs(butler.run)
            if existingRefs and missingRefs:
                _LOG.debug(
                    "Partial outputs exist for task %s dataId=%s collection=%s "
                    "existingRefs=%s missingRefs=%s",
                    taskDef,
                    quantum.dataId,
                    butler.run,
                    existingRefs,
                    missingRefs,
                )
                if self.clobberOutputs:
                    # only prune
                    _LOG.info("Removing partial outputs for task %s: %s", taskDef, existingRefs)
                    # Do not purge registry records if this looks like
                    # an execution butler. This ensures that the UUID
                    # of the dataset doesn't change.
                    if butler._allow_put_of_predefined_dataset:
                        purge = False
                        disassociate = False
                    else:
                        purge = True
                        disassociate = True
                    butler.pruneDatasets(existingRefs, disassociate=disassociate, unstore=True, purge=purge)
                    return False
                else:
                    raise RuntimeError(
                        f"Registry inconsistency while checking for existing outputs:"
                        f" collection={butler.run} existingRefs={existingRefs}"
                        f" missingRefs={missingRefs}"
                    )

        # need to re-run
        return False

    def makeTask(self, taskClass, name, config, butler):
        """Make new task instance.

        Parameters
        ----------
        taskClass : `type`
            Sub-class of `~lsst.pipe.base.PipelineTask`.
        name : `str`
            Name for this task.
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
        return self.taskFactory.makeTask(taskClass, name, config, None, butler)

    def updatedQuantumInputs(self, quantum, butler, taskDef):
        """Update quantum with extra information, returns a new updated
        Quantum.

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
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.

        Returns
        -------
        update : `~lsst.daf.butler.Quantum`
            Updated Quantum instance
        """
        anyChanges = False
        updatedInputs = defaultdict(list)
        for key, refsForDatasetType in quantum.inputs.items():
            newRefsForDatasetType = updatedInputs[key]
            for ref in refsForDatasetType:
                if ref.id is None:
                    resolvedRef = butler.registry.findDataset(
                        ref.datasetType, ref.dataId, collections=butler.collections
                    )
                    if resolvedRef is None:
                        _LOG.info("No dataset found for %s", ref)
                        continue
                    else:
                        _LOG.debug("Updated dataset ID for %s", ref)
                else:
                    resolvedRef = ref
                # We need to ask datastore if the dataset actually exists
                # because the Registry of a local "execution butler" cannot
                # know this (because we prepopulate it with all of the datasets
                # that might be created).
                if butler.datastore.exists(resolvedRef):
                    newRefsForDatasetType.append(resolvedRef)
            if len(newRefsForDatasetType) != len(refsForDatasetType):
                anyChanges = True
        # If we removed any input datasets, let the task check if it has enough
        # to proceed and/or prune related datasets that it also doesn't
        # need/produce anymore.  It will raise NoWorkFound if it can't run,
        # which we'll let propagate up.  This is exactly what we run during QG
        # generation, because a task shouldn't care whether an input is missing
        # because some previous task didn't produce it, or because it just
        # wasn't there during QG generation.
        updatedInputs = NamedKeyDict[DatasetType, List[DatasetRef]](updatedInputs.items())
        helper = AdjustQuantumHelper(updatedInputs, quantum.outputs)
        if anyChanges:
            helper.adjust_in_place(taskDef.connections, label=taskDef.label, data_id=quantum.dataId)
        return Quantum(
            taskName=quantum.taskName,
            taskClass=quantum.taskClass,
            dataId=quantum.dataId,
            initInputs=quantum.initInputs,
            inputs=helper.inputs,
            outputs=helper.outputs,
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

        # Call task runQuantum() method.  Catch a few known failure modes and
        # translate them into specific
        try:
            task.runQuantum(butlerQC, inputRefs, outputRefs)
        except NoWorkFound as err:
            # Not an error, just an early exit.
            _LOG.info("Task '%s' on quantum %s exited early: %s", taskDef.label, quantum.dataId, str(err))
            pass
        except RepeatableQuantumError as err:
            if self.exitOnKnownError:
                _LOG.warning("Caught repeatable quantum error for %s (%s):", taskDef, quantum.dataId)
                _LOG.warning(err, exc_info=True)
                sys.exit(err.EXIT_CODE)
            else:
                raise
        except InvalidQuantumError as err:
            _LOG.fatal("Invalid quantum error for %s (%s): %s", taskDef, quantum.dataId)
            _LOG.fatal(err, exc_info=True)
            sys.exit(err.EXIT_CODE)

    def writeMetadata(self, quantum, metadata, taskDef, butler):
        if taskDef.metadataDatasetName is not None:
            # DatasetRef has to be in the Quantum outputs, can lookup by name
            try:
                ref = quantum.outputs[taskDef.metadataDatasetName]
            except LookupError as exc:
                raise InvalidQuantumError(
                    f"Quantum outputs is missing metadata dataset type {taskDef.metadataDatasetName};"
                    f" this could happen due to inconsistent options between QuantumGraph generation"
                    f" and execution"
                ) from exc
            butler.put(metadata, ref[0])

    def writeLogRecords(self, quantum, taskDef, butler, store):
        # If we are logging to an external file we must always try to
        # close it.
        filename = None
        if isinstance(self.log_handler, FileHandler):
            filename = self.log_handler.stream.name
            self.log_handler.close()

        if self.log_handler is not None:
            # Remove the handler so we stop accumulating log messages.
            logging.getLogger().removeHandler(self.log_handler)

        try:
            if store and taskDef.logOutputDatasetName is not None and self.log_handler is not None:
                # DatasetRef has to be in the Quantum outputs, can lookup by
                # name
                try:
                    ref = quantum.outputs[taskDef.logOutputDatasetName]
                except LookupError as exc:
                    raise InvalidQuantumError(
                        f"Quantum outputs is missing log output dataset type {taskDef.logOutputDatasetName};"
                        f" this could happen due to inconsistent options between QuantumGraph generation"
                        f" and execution"
                    ) from exc

                if isinstance(self.log_handler, ButlerLogRecordHandler):
                    butler.put(self.log_handler.records, ref[0])

                    # Clear the records in case the handler is reused.
                    self.log_handler.records.clear()
                else:
                    assert filename is not None, "Somehow unable to extract filename from file handler"

                    # Need to ingest this file directly into butler.
                    dataset = FileDataset(path=filename, refs=ref[0])
                    try:
                        butler.ingest(dataset, transfer="move")
                        filename = None
                    except NotImplementedError:
                        # Some datastores can't receive files (e.g. in-memory
                        # datastore when testing), we store empty list for
                        # those just to have a dataset. Alternative is to read
                        # the file as a ButlerLogRecords object and put it.
                        _LOG.info(
                            "Log records could not be stored in this butler because the"
                            " datastore can not ingest files, empty record list is stored instead."
                        )
                        records = ButlerLogRecords.from_records([])
                        butler.put(records, ref[0])
        finally:
            # remove file if it is not ingested
            if filename is not None:
                try:
                    os.remove(filename)
                except OSError:
                    pass

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
                        assert (
                            instrument == oneInstrument
                        ), "Currently require that only one instrument is used per graph"
                    else:
                        oneInstrument = instrument
                        Instrument.fromName(instrument, butler.registry)
