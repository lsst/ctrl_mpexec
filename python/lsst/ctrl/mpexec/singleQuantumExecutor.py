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
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from itertools import chain
from typing import Any, Optional, Union

from lsst.daf.butler import Butler, DatasetRef, DatasetType, LimitedButler, NamedKeyDict, Quantum
from lsst.pipe.base import (
    AdjustQuantumHelper,
    ButlerQuantumContext,
    Instrument,
    InvalidQuantumError,
    NoWorkFound,
    PipelineTask,
    RepeatableQuantumError,
    TaskDef,
    TaskFactory,
)
from lsst.pipe.base.configOverrides import ConfigOverrides

# During metadata transition phase, determine metadata class by
# asking pipe_base
from lsst.pipe.base.task import _TASK_FULL_METADATA_TYPE, _TASK_METADATA_TYPE
from lsst.utils.timer import logInfo

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .cli.utils import _PipelineAction
from .log_capture import LogCapture
from .mock_task import MockButlerQuantumContext, MockPipelineTask
from .quantumGraphExecutor import QuantumExecutor
from .reports import QuantumReport

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)


class SingleQuantumExecutor(QuantumExecutor):
    """Executor class which runs one Quantum at a time.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler` or `None`
        Data butler, `None` means that Quantum-backed butler should be used
        instead.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Instance of a task factory.
    skipExistingIn : `list` [ `str` ], optional
        Accepts list of collections, if all Quantum outputs already exist in
        the specified list of collections then that Quantum will not be rerun.
    clobberOutputs : `bool`, optional
        If `True`, then existing outputs in output run collection will be
        overwritten.  If ``skipExistingIn`` is defined, only outputs from
        failed quanta will be overwritten. Only used when ``butler`` is not
        `None`.
    enableLsstDebug : `bool`, optional
        Enable debugging with ``lsstDebug`` facility for a task.
    exitOnKnownError : `bool`, optional
        If `True`, call `sys.exit` with the appropriate exit code for special
        known exceptions, after printing a traceback, instead of letting the
        exception propagate up to calling.  This is always the behavior for
        InvalidQuantumError.
    mock : `bool`, optional
        If `True` then mock task execution.
    mock_configs : `list` [ `_PipelineAction` ], optional
        Optional config overrides for mock tasks.
    limited_butler_factory : `Callable`, optional
        A method that creates a `~lsst.daf.butler.LimitedButler` instance
        for a given Quantum. This parameter must be defined if ``butler`` is
        `None`. If ``butler`` is not `None` then this parameter is ignored.
    """

    def __init__(
        self,
        butler: Butler | None,
        taskFactory: TaskFactory,
        skipExistingIn: list[str] | None = None,
        clobberOutputs: bool = False,
        enableLsstDebug: bool = False,
        exitOnKnownError: bool = False,
        mock: bool = False,
        mock_configs: list[_PipelineAction] | None = None,
        limited_butler_factory: Callable[[Quantum], LimitedButler] | None = None,
    ):
        self.butler = butler
        self.taskFactory = taskFactory
        self.skipExistingIn = skipExistingIn
        self.enableLsstDebug = enableLsstDebug
        self.clobberOutputs = clobberOutputs
        self.exitOnKnownError = exitOnKnownError
        self.mock = mock
        self.mock_configs = mock_configs if mock_configs is not None else []
        self.limited_butler_factory = limited_butler_factory
        self.report: Optional[QuantumReport] = None

        if self.butler is None:
            assert not self.mock, "Mock execution only possible with full butler"
            assert limited_butler_factory is not None, "limited_butler_factory is needed when butler is None"

    def execute(self, taskDef: TaskDef, quantum: Quantum) -> Quantum:
        # Docstring inherited from QuantumExecutor.execute
        assert quantum.dataId is not None, "Quantum DataId cannot be None"

        if self.butler is not None:
            self.butler.registry.refresh()

        # Catch any exception and make a report based on that.
        try:
            result = self._execute(taskDef, quantum)
            self.report = QuantumReport(dataId=quantum.dataId, taskLabel=taskDef.label)
            return result
        except Exception as exc:
            self.report = QuantumReport.from_exception(
                exception=exc,
                dataId=quantum.dataId,
                taskLabel=taskDef.label,
            )
            raise

    def _resolve_ref(self, ref: DatasetRef, collections: Any = None) -> DatasetRef | None:
        """Return resolved reference.

        Parameters
        ----------
        ref : `DatasetRef`
            Input reference, can be either resolved or unresolved.
        collections :
            Collections to search for the existing reference, only used when
            running with full butler.

        Notes
        -----
        When running with Quantum-backed butler it assumes that reference is
        already resolved and returns input references without any checks. When
        running with full butler, it always searches registry fof a reference
        in specified collections, even if reference is already resolved.
        """
        if self.butler is not None:
            # If running with full butler, need to re-resolve it in case
            # collections are different.
            ref = ref.unresolved()
            return self.butler.registry.findDataset(ref.datasetType, ref.dataId, collections=collections)
        else:
            # In case of QBB all refs must be resolved already, do not check.
            return ref

    def _execute(self, taskDef: TaskDef, quantum: Quantum) -> Quantum:
        """Internal implementation of execute()"""
        startTime = time.time()

        # Make a limited butler instance if needed (which should be QBB if full
        # butler is not defined).
        limited_butler: LimitedButler
        if self.butler is not None:
            limited_butler = self.butler
        else:
            # We check this in constructor, but mypy needs this check here.
            assert self.limited_butler_factory is not None
            limited_butler = self.limited_butler_factory(quantum)

        if self.butler is not None:
            log_capture = LogCapture.from_full(self.butler)
        else:
            log_capture = LogCapture.from_limited(limited_butler)
        with log_capture.capture_logging(taskDef, quantum) as captureLog:
            # Save detailed resource usage before task start to metadata.
            quantumMetadata = _TASK_METADATA_TYPE()
            logInfo(None, "prep", metadata=quantumMetadata)  # type: ignore[arg-type]

            # check whether to skip or delete old outputs, if it returns True
            # or raises an exception do not try to store logs, as they may be
            # already in butler.
            captureLog.store = False
            if self.checkExistingOutputs(quantum, taskDef, limited_butler):
                _LOG.info(
                    "Skipping already-successful quantum for label=%s dataId=%s.",
                    taskDef.label,
                    quantum.dataId,
                )
                return quantum
            captureLog.store = True

            try:
                quantum = self.updatedQuantumInputs(quantum, taskDef, limited_butler)
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
                logInfo(None, "end", metadata=quantumMetadata)  # type: ignore[arg-type]
                fullMetadata = _TASK_FULL_METADATA_TYPE()
                fullMetadata[taskDef.label] = _TASK_METADATA_TYPE()
                fullMetadata["quantum"] = quantumMetadata
                self.writeMetadata(quantum, fullMetadata, taskDef, limited_butler)
                return quantum

            # enable lsstDebug debugging
            if self.enableLsstDebug:
                try:
                    _LOG.debug("Will try to import debug.py")
                    import debug  # type: ignore # noqa:F401
                except ImportError:
                    _LOG.warn("No 'debug' module found.")

            # initialize global state
            self.initGlobals(quantum)

            # Ensure that we are executing a frozen config
            taskDef.config.freeze()
            logInfo(None, "init", metadata=quantumMetadata)  # type: ignore[arg-type]
            init_input_refs = []
            for ref in quantum.initInputs.values():
                resolved = self._resolve_ref(ref)
                if resolved is None:
                    raise ValueError(f"Failed to resolve init input reference {ref}")
                init_input_refs.append(resolved)
            task = self.taskFactory.makeTask(taskDef, limited_butler, init_input_refs)
            logInfo(None, "start", metadata=quantumMetadata)  # type: ignore[arg-type]
            try:
                if self.mock:
                    # Use mock task instance to execute method.
                    runTask = self._makeMockTask(taskDef)
                else:
                    runTask = task
                self.runQuantum(runTask, quantum, taskDef, limited_butler)
            except Exception as e:
                _LOG.error(
                    "Execution of task '%s' on quantum %s failed. Exception %s: %s",
                    taskDef.label,
                    quantum.dataId,
                    e.__class__.__name__,
                    str(e),
                )
                raise
            logInfo(None, "end", metadata=quantumMetadata)  # type: ignore[arg-type]
            fullMetadata = task.getFullMetadata()
            fullMetadata["quantum"] = quantumMetadata
            self.writeMetadata(quantum, fullMetadata, taskDef, limited_butler)
            stopTime = time.time()
            _LOG.info(
                "Execution of task '%s' on quantum %s took %.3f seconds",
                taskDef.label,
                quantum.dataId,
                stopTime - startTime,
            )
        return quantum

    def _makeMockTask(self, taskDef: TaskDef) -> PipelineTask:
        """Make an instance of mock task for given TaskDef."""
        # Make config instance and apply overrides
        overrides = ConfigOverrides()
        for action in self.mock_configs:
            if action.label == taskDef.label + "-mock":
                if action.action == "config":
                    key, _, value = action.value.partition("=")
                    overrides.addValueOverride(key, value)
                elif action.action == "configfile":
                    overrides.addFileOverride(os.path.expandvars(action.value))
                else:
                    raise ValueError(f"Unexpected action for mock task config overrides: {action}")
        config = MockPipelineTask.ConfigClass()
        overrides.applyTo(config)

        task = MockPipelineTask(config=config, name=taskDef.label)
        return task

    def checkExistingOutputs(self, quantum: Quantum, taskDef: TaskDef, limited_butler: LimitedButler) -> bool:
        """Decide whether this quantum needs to be executed.

        If only partial outputs exist then they are removed if
        ``clobberOutputs`` is True, otherwise an exception is raised.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Quantum to check for existing outputs
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
            [metadata_ref] = quantum.outputs[taskDef.metadataDatasetName]
            ref = self._resolve_ref(metadata_ref, self.skipExistingIn)
            if ref is not None:
                if limited_butler.datastore.exists(ref):
                    return True

        # Previously we always checked for existing outputs in `butler.run`,
        # now logic gets more complicated as we only want to skip quantum
        # whose outputs exist in `self.skipExistingIn` but pruning should only
        # be done for outputs existing in `butler.run`.

        def findOutputs(
            collections: Optional[Union[str, list[str]]]
        ) -> tuple[list[DatasetRef], list[DatasetRef]]:
            """Find quantum outputs in specified collections."""
            existingRefs = []
            missingRefs = []
            for datasetRefs in quantum.outputs.values():
                checkRefs: list[DatasetRef] = []
                registryRefToQuantumRef: dict[DatasetRef, DatasetRef] = {}
                for datasetRef in datasetRefs:
                    ref = self._resolve_ref(datasetRef, collections)
                    if ref is None:
                        missingRefs.append(datasetRef)
                    else:
                        checkRefs.append(ref)
                        registryRefToQuantumRef[ref] = datasetRef

                # More efficient to ask the datastore in bulk for ref
                # existence rather than one at a time.
                existence = limited_butler.datastore.mexists(checkRefs)
                for ref, exists in existence.items():
                    if exists:
                        existingRefs.append(ref)
                    else:
                        missingRefs.append(registryRefToQuantumRef[ref])
            return existingRefs, missingRefs

        existingRefs, missingRefs = findOutputs(self.skipExistingIn)
        if self.skipExistingIn:
            if existingRefs and not missingRefs:
                # everything is already there
                return True

        # If we are to re-run quantum then prune datasets that exists in
        # output run collection, only if `self.clobberOutputs` is set,
        # that only works when we have full butler.
        if existingRefs and self.butler is not None:
            existingRefs, missingRefs = findOutputs(self.butler.run)
            if existingRefs and missingRefs:
                _LOG.debug(
                    "Partial outputs exist for task %s dataId=%s collection=%s "
                    "existingRefs=%s missingRefs=%s",
                    taskDef,
                    quantum.dataId,
                    self.butler.run,
                    existingRefs,
                    missingRefs,
                )
                if self.clobberOutputs:
                    # only prune
                    _LOG.info("Removing partial outputs for task %s: %s", taskDef, existingRefs)
                    self.butler.pruneDatasets(existingRefs, disassociate=True, unstore=True, purge=True)
                    return False
                else:
                    raise RuntimeError(
                        "Registry inconsistency while checking for existing outputs:"
                        f" collection={self.butler.run} existingRefs={existingRefs}"
                        f" missingRefs={missingRefs}"
                    )

        # need to re-run
        return False

    def updatedQuantumInputs(
        self, quantum: Quantum, taskDef: TaskDef, limited_butler: LimitedButler
    ) -> Quantum:
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
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.

        Returns
        -------
        update : `~lsst.daf.butler.Quantum`
            Updated Quantum instance
        """
        anyChanges = False
        updatedInputs: defaultdict[DatasetType, list] = defaultdict(list)
        for key, refsForDatasetType in quantum.inputs.items():
            newRefsForDatasetType = updatedInputs[key]
            for ref in refsForDatasetType:
                # Inputs may already be resolved even if they do not exist, but
                # we have to re-resolve them because IDs are ignored on output.
                # Check datastore for existence first to cover calibration
                # dataset types, as they would need a timespan for findDataset.
                resolvedRef: DatasetRef | None
                checked_datastore = False
                if ref.id is not None and limited_butler.datastore.exists(ref):
                    resolvedRef = ref
                    checked_datastore = True
                elif self.butler is not None:
                    # In case of full butler try to (re-)resolve it.
                    resolvedRef = self._resolve_ref(ref)
                    if resolvedRef is None:
                        _LOG.info("No dataset found for %s", ref)
                        continue
                    else:
                        _LOG.debug("Updated dataset ID for %s", ref)
                else:
                    # QBB with missing intermediate
                    _LOG.info("No dataset found for %s", ref)
                    continue

                # In case of mock execution we check that mock dataset exists
                # instead. Mock execution is only possible with full butler.
                if self.mock and self.butler is not None:
                    try:
                        typeName, component = ref.datasetType.nameAndComponent()
                        if component is not None:
                            mockDatasetTypeName = MockButlerQuantumContext.mockDatasetTypeName(typeName)
                        else:
                            mockDatasetTypeName = MockButlerQuantumContext.mockDatasetTypeName(
                                ref.datasetType.name
                            )

                        mockDatasetType = self.butler.registry.getDatasetType(mockDatasetTypeName)
                    except KeyError:
                        # means that mock dataset type is not there and this
                        # should be a pre-existing dataset
                        _LOG.debug("No mock dataset type for %s", ref)
                        if self.butler.datastore.exists(resolvedRef):
                            newRefsForDatasetType.append(resolvedRef)
                    else:
                        mockRef = DatasetRef(mockDatasetType, ref.dataId)
                        resolvedMockRef = self.butler.registry.findDataset(
                            mockRef.datasetType, mockRef.dataId, collections=self.butler.collections
                        )
                        _LOG.debug("mockRef=%s resolvedMockRef=%s", mockRef, resolvedMockRef)
                        if resolvedMockRef is not None and self.butler.datastore.exists(resolvedMockRef):
                            _LOG.debug("resolvedMockRef dataset exists")
                            newRefsForDatasetType.append(resolvedRef)
                elif checked_datastore or limited_butler.datastore.exists(resolvedRef):
                    # We need to ask datastore if the dataset actually exists
                    # because the Registry of a local "execution butler"
                    # cannot know this (because we prepopulate it with all of
                    # the datasets that might be created).
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
        namedUpdatedInputs = NamedKeyDict[DatasetType, list[DatasetRef]](updatedInputs.items())
        helper = AdjustQuantumHelper(namedUpdatedInputs, quantum.outputs)
        if anyChanges:
            assert quantum.dataId is not None, "Quantum DataId cannot be None"
            helper.adjust_in_place(taskDef.connections, label=taskDef.label, data_id=quantum.dataId)
        return Quantum(
            taskName=quantum.taskName,
            taskClass=quantum.taskClass,
            dataId=quantum.dataId,
            initInputs=quantum.initInputs,
            inputs=helper.inputs,
            outputs=helper.outputs,
        )

    def runQuantum(
        self, task: PipelineTask, quantum: Quantum, taskDef: TaskDef, limited_butler: LimitedButler
    ) -> None:
        """Execute task on a single quantum.

        Parameters
        ----------
        task : `~lsst.pipe.base.PipelineTask`
            Task object.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.
        """
        # Create a butler that operates in the context of a quantum
        if self.butler is None:
            butlerQC = ButlerQuantumContext.from_limited(limited_butler, quantum)
        else:
            if self.mock:
                butlerQC = MockButlerQuantumContext(self.butler, quantum)
            else:
                butlerQC = ButlerQuantumContext.from_full(self.butler, quantum)

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

    def writeMetadata(
        self, quantum: Quantum, metadata: Any, taskDef: TaskDef, limited_butler: LimitedButler
    ) -> None:
        if taskDef.metadataDatasetName is not None:
            # DatasetRef has to be in the Quantum outputs, can lookup by name
            try:
                [ref] = quantum.outputs[taskDef.metadataDatasetName]
            except LookupError as exc:
                raise InvalidQuantumError(
                    f"Quantum outputs is missing metadata dataset type {taskDef.metadataDatasetName};"
                    " this could happen due to inconsistent options between QuantumGraph generation"
                    " and execution"
                ) from exc
            if self.butler is not None:
                # Dataset ref can already be resolved, for non-QBB executor we
                # have to ignore that because may be overriding run
                # collection.
                if ref.id is not None:
                    ref = ref.unresolved()
                self.butler.put(metadata, ref)
            else:
                limited_butler.putDirect(metadata, ref)

    def initGlobals(self, quantum: Quantum) -> None:
        """Initialize global state needed for task execution.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.

        Notes
        -----
        There is an issue with initializing filters singleton which is done
        by instrument, to avoid requiring tasks to do it in runQuantum()
        we do it here when any dataId has an instrument dimension. Also for
        now we only allow single instrument, verify that all instrument
        names in all dataIds are identical.

        This will need revision when filter singleton disappears.
        """
        # can only work for full butler
        if self.butler is None:
            return
        oneInstrument = None
        for datasetRefs in chain(quantum.inputs.values(), quantum.outputs.values()):
            for datasetRef in datasetRefs:
                dataId = datasetRef.dataId
                instrument = dataId.get("instrument")
                if instrument is not None:
                    if oneInstrument is not None:
                        assert (  # type: ignore
                            instrument == oneInstrument
                        ), "Currently require that only one instrument is used per graph"
                    else:
                        oneInstrument = instrument
                        Instrument.fromName(instrument, self.butler.registry)

    def getReport(self) -> Optional[QuantumReport]:
        # Docstring inherited from base class
        if self.report is None:
            raise RuntimeError("getReport() called before execute()")
        return self.report
