# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = ["PreExecInit"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import abc
import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import DatasetRef, DatasetType
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.pipe.base import PipelineDatasetTypes
from lsst.utils.packages import Packages

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, LimitedButler
    from lsst.pipe.base import QuantumGraph, TaskDef, TaskFactory

_LOG = logging.getLogger(__name__)


class MissingReferenceError(Exception):
    """Exception raised when resolved reference is missing from graph."""

    pass


def _compare_packages(old_packages: Packages, new_packages: Packages) -> None:
    """Compare two versions of Packages.

    Parameters
    ----------
    old_packages : `Packages`
        Previously recorded package versions.
    new_packages : `Packages`
        New set of package versions.

    Raises
    ------
    TypeError
        Raised if parameters are inconsistent.
    """
    diff = new_packages.difference(old_packages)
    if diff:
        versions_str = "; ".join(f"{pkg}: {diff[pkg][1]} vs {diff[pkg][0]}" for pkg in diff)
        raise TypeError(f"Package versions mismatch: ({versions_str})")
    else:
        _LOG.debug("new packages are consistent with old")


class PreExecInitBase(abc.ABC):
    """Common part of the implementation of PreExecInit classes that does not
    depend on Butler type.
    """

    def __init__(self, butler: LimitedButler, taskFactory: TaskFactory, extendRun: bool):
        self.butler = butler
        self.taskFactory = taskFactory
        self.extendRun = extendRun

    def initialize(
        self,
        graph: QuantumGraph,
        saveInitOutputs: bool = True,
        registerDatasetTypes: bool = False,
        saveVersions: bool = True,
    ) -> None:
        """Perform all initialization steps.

        Convenience method to execute all initialization steps. Instead of
        calling this method and providing all options it is also possible to
        call methods individually.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        saveInitOutputs : `bool`, optional
            If ``True`` (default) then save "init outputs", configurations,
            and package versions to butler.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.
        saveVersions : `bool`, optional
            If ``False`` then do not save package versions even if
            ``saveInitOutputs`` is set to ``True``.
        """
        # register dataset types or check consistency
        self.initializeDatasetTypes(graph, registerDatasetTypes)

        # Save task initialization data or check that saved data
        # is consistent with what tasks would save
        if saveInitOutputs:
            self.saveInitOutputs(graph)
            self.saveConfigs(graph)
            if saveVersions:
                self.savePackageVersions(graph)

    @abc.abstractmethod
    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        """Save or check DatasetTypes output by the tasks in a graph.

        Iterates over all DatasetTypes for all tasks in a graph and either
        tries to add them to registry or compares them to existing ones.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.

        Raises
        ------
        ValueError
            Raised if existing DatasetType is different from DatasetType
            in a graph.
        KeyError
            Raised if ``registerDatasetTypes`` is ``False`` and DatasetType
            does not exist in registry.
        """
        raise NotImplementedError()

    def saveInitOutputs(self, graph: QuantumGraph) -> None:
        """Write any datasets produced by initializing tasks in a graph.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        TypeError
            Raised if the type of existing object in butler is different from
            new data.
        """
        _LOG.debug("Will save InitOutputs for all tasks")
        for taskDef in self._task_iter(graph):
            init_input_refs = graph.initInputRefs(taskDef) or []
            task = self.taskFactory.makeTask(taskDef, self.butler, init_input_refs)
            for name in taskDef.connections.initOutputs:
                attribute = getattr(taskDef.connections, name)
                init_output_refs = graph.initOutputRefs(taskDef) or []
                init_output_ref, obj_from_store = self._find_dataset(init_output_refs, attribute.name)
                if init_output_ref is None:
                    raise ValueError(f"Cannot find dataset reference for init output {name} in a graph")
                init_output_var = getattr(task, name)

                if obj_from_store is not None:
                    _LOG.debug(
                        "Retrieving InitOutputs for task=%s key=%s dsTypeName=%s", task, name, attribute.name
                    )
                    obj_from_store = self.butler.get(init_output_ref)
                    # Types are supposed to be identical.
                    # TODO: Check that object contents is identical too.
                    if type(obj_from_store) is not type(init_output_var):
                        raise TypeError(
                            f"Stored initOutput object type {type(obj_from_store)} "
                            "is different from task-generated type "
                            f"{type(init_output_var)} for task {taskDef}"
                        )
                else:
                    _LOG.debug("Saving InitOutputs for task=%s key=%s", taskDef.label, name)
                    # This can still raise if there is a concurrent write.
                    self.butler.put(init_output_var, init_output_ref)

    def saveConfigs(self, graph: QuantumGraph) -> None:
        """Write configurations for pipeline tasks to butler or check that
        existing configurations are equal to the new ones.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        TypeError
            Raised if existing object in butler is different from new data.
        Exception
            Raised if ``extendRun`` is `False` and datasets already exists.
            Content of a butler collection should not be changed if exception
            is raised.
        """

        def logConfigMismatch(msg: str) -> None:
            """Log messages about configuration mismatch."""
            _LOG.fatal("Comparing configuration: %s", msg)

        _LOG.debug("Will save Configs for all tasks")
        # start transaction to rollback any changes on exceptions
        with self.transaction():
            for taskDef in self._task_iter(graph):
                # Config dataset ref is stored in task init outputs, but it
                # may be also be missing.
                task_output_refs = graph.initOutputRefs(taskDef)
                if task_output_refs is None:
                    continue

                config_ref, old_config = self._find_dataset(task_output_refs, taskDef.configDatasetName)
                if config_ref is None:
                    continue

                if old_config is not None:
                    if not taskDef.config.compare(old_config, shortcut=False, output=logConfigMismatch):
                        raise TypeError(
                            f"Config does not match existing task config {taskDef.configDatasetName!r} in "
                            "butler; tasks configurations must be consistent within the same run collection"
                        )
                else:
                    _LOG.debug(
                        "Saving Config for task=%s dataset type=%s", taskDef.label, taskDef.configDatasetName
                    )
                    self.butler.put(taskDef.config, config_ref)

    def savePackageVersions(self, graph: QuantumGraph) -> None:
        """Write versions of software packages to butler.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        TypeError
            Raised if existing object in butler is incompatible with new data.
        """
        packages = Packages.fromSystem()
        _LOG.debug("want to save packages: %s", packages)

        # start transaction to rollback any changes on exceptions
        with self.transaction():
            # Packages dataset ref is stored in graph's global init outputs,
            # but it may be also be missing.

            packages_ref, old_packages = self._find_dataset(
                graph.globalInitOutputRefs(), PipelineDatasetTypes.packagesDatasetName
            )
            if packages_ref is None:
                return

            if old_packages is not None:
                # Note that because we can only detect python modules that have
                # been imported, the stored list of products may be more or
                # less complete than what we have now.  What's important is
                # that the products that are in common have the same version.
                _compare_packages(old_packages, packages)
                # Update the old set of packages in case we have more packages
                # that haven't been persisted.
                extra = packages.extra(old_packages)
                if extra:
                    _LOG.debug("extra packages: %s", extra)
                    old_packages.update(packages)
                    # have to remove existing dataset first, butler has no
                    # replace option.
                    self.butler.pruneDatasets([packages_ref], unstore=True, purge=True)
                    self.butler.put(old_packages, packages_ref)
            else:
                self.butler.put(packages, packages_ref)

    def _find_dataset(
        self, refs: Iterable[DatasetRef], dataset_type: str
    ) -> tuple[DatasetRef | None, Any | None]:
        """Find a ref with a given dataset type name in a list of references
        and try to retrieve its data from butler.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `~lsst.daf.butler.DatasetRef` ]
            References to check for matching dataset type.
        dataset_type : `str`
            Name of a dataset type to look for.

        Returns
        -------
        ref : `~lsst.daf.butler.DatasetRef` or `None`
            Dataset reference or `None` if there is no matching dataset type.
        data : `Any`
            An existing object extracted from butler, `None` if ``ref`` is
            `None` or if there is no existing object for that reference.
        """
        ref: DatasetRef | None = None
        for ref in refs:
            if ref.datasetType.name == dataset_type:
                break
        else:
            return None, None

        try:
            data = self.butler.get(ref)
            if data is not None and not self.extendRun:
                # It must not exist unless we are extending run.
                raise ConflictingDefinitionError(f"Dataset {ref} already exists in butler")
        except (LookupError, FileNotFoundError):
            data = None
        return ref, data

    def _task_iter(self, graph: QuantumGraph) -> Iterator[TaskDef]:
        """Iterate over TaskDefs in a graph, return only tasks that have one or
        more associated quanta.
        """
        for taskDef in graph.iterTaskGraph():
            if graph.getNumberOfQuantaForTask(taskDef) > 0:
                yield taskDef

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager for transaction.

        Default implementation has no transaction support.
        """
        yield


class PreExecInit(PreExecInitBase):
    """Initialization of registry for QuantumGraph execution.

    This class encapsulates all necessary operations that have to be performed
    on butler and registry to prepare them for QuantumGraph execution.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Task factory.
    extendRun : `bool`, optional
        If `True` then do not try to overwrite any datasets that might exist
        in ``butler.run``; instead compare them when appropriate/possible.  If
        `False`, then any existing conflicting dataset will cause a butler
        exception to be raised.
    """

    def __init__(self, butler: Butler, taskFactory: TaskFactory, extendRun: bool = False):
        super().__init__(butler, taskFactory, extendRun)
        self.full_butler = butler
        if self.extendRun and self.full_butler.run is None:
            raise RuntimeError(
                "Cannot perform extendRun logic unless butler is initialized "
                "with a default output RUN collection."
            )

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # dosctring inherited
        with self.full_butler.transaction():
            yield

    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        # docstring inherited
        pipeline = graph.taskGraph
        pipelineDatasetTypes = PipelineDatasetTypes.fromPipeline(
            pipeline, registry=self.full_butler.registry, include_configs=True, include_packages=True
        )

        for datasetTypes, is_input in (
            (pipelineDatasetTypes.initIntermediates, True),
            (pipelineDatasetTypes.initOutputs, False),
            (pipelineDatasetTypes.intermediates, True),
            (pipelineDatasetTypes.outputs, False),
        ):
            self._register_output_dataset_types(registerDatasetTypes, datasetTypes, is_input)

    def _register_output_dataset_types(
        self, registerDatasetTypes: bool, datasetTypes: Iterable[DatasetType], is_input: bool
    ) -> None:
        def _check_compatibility(datasetType: DatasetType, expected: DatasetType, is_input: bool) -> bool:
            # These are output dataset types so check for compatibility on put.
            is_compatible = expected.is_compatible_with(datasetType)

            if is_input:
                # This dataset type is also used for input so must be
                # compatible on get as ell.
                is_compatible = is_compatible and datasetType.is_compatible_with(expected)

            if is_compatible:
                _LOG.debug(
                    "The dataset type configurations differ (%s from task != %s from registry) "
                    "but the storage classes are compatible. Can continue.",
                    datasetType,
                    expected,
                )
            return is_compatible

        missing_datasetTypes = set()
        for datasetType in datasetTypes:
            # Only composites are registered, no components, and by this point
            # the composite should already exist.
            if registerDatasetTypes and not datasetType.isComponent():
                _LOG.debug("Registering DatasetType %s with registry", datasetType)
                # this is a no-op if it already exists and is consistent,
                # and it raises if it is inconsistent.
                try:
                    self.full_butler.registry.registerDatasetType(datasetType)
                except ConflictingDefinitionError:
                    if not _check_compatibility(
                        datasetType, self.full_butler.get_dataset_type(datasetType.name), is_input
                    ):
                        raise
            else:
                _LOG.debug("Checking DatasetType %s against registry", datasetType)
                try:
                    expected = self.full_butler.get_dataset_type(datasetType.name)
                except KeyError:
                    # Likely means that --register-dataset-types is forgotten.
                    missing_datasetTypes.add(datasetType.name)
                    continue
                if expected != datasetType:
                    if not _check_compatibility(datasetType, expected, is_input):
                        raise ValueError(
                            f"DatasetType configuration does not match Registry: {datasetType} != {expected}"
                        )

        if missing_datasetTypes:
            plural = "s" if len(missing_datasetTypes) != 1 else ""
            raise KeyError(
                f"Missing dataset type definition{plural}: {', '.join(missing_datasetTypes)}. "
                "Dataset types have to be registered with either `butler register-dataset-type` or "
                "passing `--register-dataset-types` option to `pipetask run`."
            )


class PreExecInitLimited(PreExecInitBase):
    """Initialization of registry for QuantumGraph execution.

    This class works with LimitedButler and expects that all references in
    QuantumGraph are resolved.

    Parameters
    ----------
    butler : `~lsst.daf.butler.LimitedButler`
        Limited data butler instance.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Task factory.
    """

    def __init__(self, butler: LimitedButler, taskFactory: TaskFactory):
        super().__init__(butler, taskFactory, False)

    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        # docstring inherited
        # With LimitedButler we never create or check dataset types.
        pass
