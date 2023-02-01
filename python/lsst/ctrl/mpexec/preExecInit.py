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
from lsst.daf.butler import DataCoordinate, DatasetIdFactory, DatasetRef, DatasetType
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.pipe.base import PipelineDatasetTypes
from lsst.utils.packages import Packages

from .mock_task import MockButlerQuantumContext

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

    def __init__(self, butler: LimitedButler, taskFactory: TaskFactory):
        self.butler = butler
        self.taskFactory = taskFactory

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
        for taskDef in graph.iterTaskGraph():
            init_input_refs = self.find_init_input_refs(taskDef, graph)
            task = self.taskFactory.makeTask(taskDef, self.butler, init_input_refs)
            for name in taskDef.connections.initOutputs:
                attribute = getattr(taskDef.connections, name)
                obj_from_store, init_output_ref = self.find_init_output(taskDef, attribute.name, graph)
                if init_output_ref is None:
                    raise ValueError(f"Cannot find or make dataset reference for init output {name}")
                init_output_var = getattr(task, name)

                if obj_from_store is not None:
                    _LOG.debug(
                        "Retrieving InitOutputs for task=%s key=%s dsTypeName=%s", task, name, attribute.name
                    )
                    obj_from_store = self.butler.getDirect(init_output_ref)
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
                    self.butler.putDirect(init_output_var, init_output_ref)

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
            for taskDef in graph.iterTaskGraph():
                config_name = taskDef.configDatasetName

                old_config, dataset_ref = self.find_init_output(taskDef, taskDef.configDatasetName, graph)

                if old_config is not None:
                    if not taskDef.config.compare(old_config, shortcut=False, output=logConfigMismatch):
                        raise TypeError(
                            f"Config does not match existing task config {taskDef.configDatasetName!r} in "
                            "butler; tasks configurations must be consistent within the same run collection"
                        )
                else:
                    # butler will raise exception if dataset is already there
                    _LOG.debug("Saving Config for task=%s dataset type=%s", taskDef.label, config_name)
                    self.butler.putDirect(taskDef.config, dataset_ref)

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
            old_packages, dataset_ref = self.find_packages(graph)

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
                    self.butler.pruneDatasets([dataset_ref], unstore=True, purge=True)
                    self.butler.putDirect(old_packages, dataset_ref)
            else:
                self.butler.putDirect(packages, dataset_ref)

    @abc.abstractmethod
    def find_init_input_refs(self, taskDef: TaskDef, graph: QuantumGraph) -> Iterable[DatasetRef]:
        """Return the list of resolved dataset references for task init inputs.

        Parameters
        ----------
        taskDef : `~lsst.pipe.base.TaskDef`
            Pipeline task definition.
        graph : `~lsst.pipe.base.QuantumGraph`
            Quantum graph.

        Returns
        -------
        refs : `~collections.abc.Iterable` [`~lsst.daf.butler.DatasetRef`]
            Resolved dataset references.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def find_init_output(
        self, taskDef: TaskDef, dataset_type: str, graph: QuantumGraph
    ) -> tuple[Any | None, DatasetRef]:
        """Find task init output for given dataset type.

        Parameters
        ----------
        taskDef : `~lsst.pipe.base.TaskDef`
            Pipeline task definition.
        dataset_type : `str`
            Dataset type name.
        graph : `~lsst.pipe.base.QuantumGraph`
            Quantum graph.

        Returns
        -------
        data
            Existing init output object retrieved from butler, `None` if butler
            has no existing object.
        ref : `~lsst.daf.butler.DatasetRef`
            Resolved reference for init output to be stored in butler.

        Raises
        ------
        MissingReferenceError
            Raised if reference cannot be found or generated.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def find_packages(self, graph: QuantumGraph) -> tuple[Packages | None, DatasetRef]:
        """Find packages information.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Quantum graph.

        Returns
        -------
        packages : `lsst.utils.packages.Packages` or `None`
            Existing packages data retrieved from butler, or `None`.
        ref : `~lsst.daf.butler.DatasetRef`
            Resolved reference for packages to be stored in butler.

        Raises
        ------
        MissingReferenceError
            Raised if reference cannot be found or generated.
        """
        raise NotImplementedError()

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
    mock : `bool`, optional
        If `True` then also do initialization needed for pipeline mocking.
    """

    def __init__(self, butler: Butler, taskFactory: TaskFactory, extendRun: bool = False, mock: bool = False):
        super().__init__(butler, taskFactory)
        self.full_butler = butler
        self.extendRun = extendRun
        self.mock = mock
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

        if self.mock:
            # register special mock data types, skip logs and metadata
            skipDatasetTypes = {taskDef.metadataDatasetName for taskDef in pipeline}
            skipDatasetTypes |= {taskDef.logOutputDatasetName for taskDef in pipeline}
            for datasetTypes, is_input in (
                (pipelineDatasetTypes.intermediates, True),
                (pipelineDatasetTypes.outputs, False),
            ):
                mockDatasetTypes = []
                for datasetType in datasetTypes:
                    if not (datasetType.name in skipDatasetTypes or datasetType.isComponent()):
                        mockDatasetTypes.append(
                            DatasetType(
                                MockButlerQuantumContext.mockDatasetTypeName(datasetType.name),
                                datasetType.dimensions,
                                "StructuredDataDict",
                            )
                        )
                if mockDatasetTypes:
                    self._register_output_dataset_types(registerDatasetTypes, mockDatasetTypes, is_input)

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
                        datasetType, self.full_butler.registry.getDatasetType(datasetType.name), is_input
                    ):
                        raise
            else:
                _LOG.debug("Checking DatasetType %s against registry", datasetType)
                try:
                    expected = self.full_butler.registry.getDatasetType(datasetType.name)
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

    def find_init_input_refs(self, taskDef: TaskDef, graph: QuantumGraph) -> Iterable[DatasetRef]:
        # docstring inherited
        refs: list[DatasetRef] = []
        for name in taskDef.connections.initInputs:
            attribute = getattr(taskDef.connections, name)
            dataId = DataCoordinate.makeEmpty(self.full_butler.dimensions)
            dataset_type = DatasetType(attribute.name, graph.universe.empty, attribute.storageClass)
            ref = self.full_butler.registry.findDataset(dataset_type, dataId)
            if ref is None:
                raise ValueError(f"InitInput does not exist in butler for dataset type {dataset_type}")
            refs.append(ref)
        return refs

    def find_init_output(
        self, taskDef: TaskDef, dataset_type_name: str, graph: QuantumGraph
    ) -> tuple[Any | None, DatasetRef]:
        # docstring inherited
        dataset_type = self.full_butler.registry.getDatasetType(dataset_type_name)
        dataId = DataCoordinate.makeEmpty(self.full_butler.dimensions)
        return self._find_existing(dataset_type, dataId)

    def find_packages(self, graph: QuantumGraph) -> tuple[Packages | None, DatasetRef]:
        # docstring inherited
        dataset_type = self.full_butler.registry.getDatasetType(PipelineDatasetTypes.packagesDatasetName)
        dataId = DataCoordinate.makeEmpty(self.full_butler.dimensions)
        return self._find_existing(dataset_type, dataId)

    def _find_existing(
        self, dataset_type: DatasetType, dataId: DataCoordinate
    ) -> tuple[Any | None, DatasetRef]:
        """Make a reference of a given dataset type and try to retrieve it from
        butler. If not found then generate new resolved reference.
        """
        run = self.full_butler.run
        assert run is not None

        ref = self.full_butler.registry.findDataset(dataset_type, dataId, collections=[run])
        if self.extendRun and ref is not None:
            try:
                config = self.butler.getDirect(ref)
                return config, ref
            except (LookupError, FileNotFoundError):
                return None, ref
        else:
            # make new resolved dataset ref
            ref = DatasetRef(dataset_type, dataId)
            ref = DatasetIdFactory().resolveRef(ref, run)
            return None, ref


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
        super().__init__(butler, taskFactory)

    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        # docstring inherited
        # With LimitedButler we never create or check dataset types.
        pass

    def find_init_input_refs(self, taskDef: TaskDef, graph: QuantumGraph) -> Iterable[DatasetRef]:
        # docstring inherited
        return graph.initInputRefs(taskDef) or []

    def find_init_output(
        self, taskDef: TaskDef, dataset_type: str, graph: QuantumGraph
    ) -> tuple[Any | None, DatasetRef]:
        # docstring inherited
        return self._find_existing(graph.initOutputRefs(taskDef) or [], dataset_type)

    def find_packages(self, graph: QuantumGraph) -> tuple[Packages | None, DatasetRef]:
        # docstring inherited
        return self._find_existing(graph.globalInitOutputRefs(), PipelineDatasetTypes.packagesDatasetName)

    def _find_existing(self, refs: Iterable[DatasetRef], dataset_type: str) -> tuple[Any | None, DatasetRef]:
        """Find a reference of a given dataset type in the list of references
        and try to retrieve it from butler.
        """
        for ref in refs:
            if ref.datasetType.name == dataset_type:
                try:
                    data = self.butler.getDirect(ref)
                    return data, ref
                except (LookupError, FileNotFoundError):
                    return None, ref
        raise MissingReferenceError(f"Failed to find reference for dataset type {dataset_type}")
