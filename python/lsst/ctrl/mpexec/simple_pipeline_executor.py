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

__all__ = ("SimplePipelineExecutor",)

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, List, Optional, Type, Union

from lsst.daf.butler import Butler, CollectionType, Quantum
from lsst.pex.config import Config
from lsst.pipe.base import GraphBuilder, Instrument, Pipeline, PipelineTask, QuantumGraph, TaskDef

from .preExecInit import PreExecInit
from .singleQuantumExecutor import SingleQuantumExecutor
from .taskFactory import TaskFactory


class SimplePipelineExecutor:
    """A simple, high-level executor for pipelines.

    Parameters
    ----------
    quantum_graph : `QuantumGraph`
        Graph to be executed.
    butler : `Butler`
        Object that manages all I/O.  Must be initialized with `collections`
        and `run` properties that correspond to the input and output
        collections, which must be consistent with those used to create
        ``quantum_graph``.

    Notes
    -----
    Most callers should use one of the `classmethod` factory functions
    (`from_pipeline_filename`, `from_task_class`, `from_pipeline`) instead of
    invoking the constructor directly; these guarantee that the `Butler` and
    `QuantumGraph` are created consistently.

    This class is intended primarily to support unit testing and small-scale
    integration testing of `PipelineTask` classes.  It deliberately lacks many
    features present in the command-line-only ``pipetask`` tool in order to
    keep the implementation simple.  Python callers that need more
    sophistication should call lower-level tools like `GraphBuilder`,
    `PreExecInit`, and `SingleQuantumExecutor` directly.
    """

    def __init__(self, quantum_graph: QuantumGraph, butler: Butler):
        self.quantum_graph = quantum_graph
        self.butler = butler

    @classmethod
    def prep_butler(
        cls,
        root: str,
        inputs: Iterable[str],
        output: str,
        output_run: Optional[str] = None,
    ) -> Butler:
        """Helper method for creating `Butler` instances with collections
        appropriate for processing.

        Parameters
        ----------
        root : `str`
            Root of the butler data repository; must already exist, with all
            necessary input data.
        inputs : `Iterable` [ `str` ]
            Collections to search for all input datasets, in search order.
        output : `str`
            Name of a new output `~CollectionType.CHAINED` collection to create
            that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~CollectionType.RUN` that will directly hold
            all output datasets.  If not provided, a name will be created from
            ``output`` and a timestamp.

        Returns
        -------
        butler : `Butler`
            Butler client instance compatible with all `classmethod` factories.
            Always writeable.
        """
        if output_run is None:
            output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"
        # Make initial butler with no collections, since we haven't created
        # them yet.
        butler = Butler(root, writeable=True)
        butler.registry.registerCollection(output_run, CollectionType.RUN)
        butler.registry.registerCollection(output, CollectionType.CHAINED)
        collections = [output_run]
        collections.extend(inputs)
        butler.registry.setCollectionChain(output, collections)
        # Remake butler to let it infer default data IDs from collections, now
        # that those collections exist.
        return Butler(butler=butler, collections=[output], run=output_run)

    @classmethod
    def from_pipeline_filename(
        cls,
        pipeline_filename: str,
        *,
        where: str = "",
        bind: Optional[Mapping[str, Any]] = None,
        butler: Butler,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an on-disk
        pipeline YAML file.

        Parameters
        ----------
        pipeline_filename : `str`
            Name of the YAML file to load the pipeline definition from.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed `QuantumGraph` and
            `Butler`, ready for `run` to be called.
        """
        pipeline = Pipeline.fromFile(pipeline_filename)
        return cls.from_pipeline(pipeline, butler=butler, where=where, bind=bind)

    @classmethod
    def from_task_class(
        cls,
        task_class: Type[PipelineTask],
        config: Optional[Config] = None,
        label: Optional[str] = None,
        *,
        where: str = "",
        bind: Optional[Mapping[str, Any]] = None,
        butler: Butler,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from a pipeline
        containing a single task.

        Parameters
        ----------
        task_class : `type`
            A concrete `PipelineTask` subclass.
        config : `Config`, optional
            Configuration for the task.  If not provided, task-level defaults
            will be used (no per-instrument overrides).
        label : `str`, optional
            Label for the task in its pipeline; defaults to
            ``task_class._DefaultName``.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed `QuantumGraph` and
            `Butler`, ready for `run` to be called.
        """
        if config is None:
            config = task_class.ConfigClass()
        if label is None:
            label = task_class._DefaultName
        if not isinstance(config, task_class.ConfigClass):
            raise TypeError(
                f"Invalid config class type: expected {task_class.ConfigClass.__name__}, "
                f"got {type(config).__name__}."
            )
        task_def = TaskDef(taskName=task_class.__name__, config=config, label=label, taskClass=task_class)
        return cls.from_pipeline([task_def], butler=butler, where=where, bind=bind)

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Union[Pipeline, Iterable[TaskDef]],
        *,
        where: str = "",
        bind: Optional[Mapping[str, Any]] = None,
        butler: Butler,
        **kwargs: Any,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an in-memory
        pipeline.

        Parameters
        ----------
        pipeline : `Pipeline` or `Iterable` [ `TaskDef` ]
            A Python object describing the tasks to run, along with their
            labels and configuration.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed `QuantumGraph` and
            `Butler`, ready for `run` to be called.
        """
        if isinstance(pipeline, Pipeline):
            pipeline = list(pipeline.toExpandedPipeline())
        else:
            pipeline = list(pipeline)
        graph_builder = GraphBuilder(butler.registry)
        quantum_graph = graph_builder.makeGraph(
            pipeline, collections=butler.collections, run=butler.run, userQuery=where, bind=bind
        )
        return cls(quantum_graph=quantum_graph, butler=butler)

    def run(self, register_dataset_types: bool = False, save_versions: bool = True) -> List[Quantum]:
        """Run all the quanta in the `QuantumGraph` in topological order.

        Use this method to run all quanta in the graph.  Use
        `as_generator` to get a generator to run the quanta one at
        a time.

        Parameters
        ----------
        register_dataset_types : `bool`, optional
            If `True`, register all output dataset types before executing any
            quanta.
        save_versions : `bool`, optional
            If `True` (default), save a package versions dataset.

        Returns
        -------
        quanta : `List` [ `Quantum` ]
            Executed quanta.  At present, these will contain only unresolved
            `DatasetRef` instances for output datasets, reflecting the state of
            the quantum just before it was run (but after any adjustments for
            predicted but now missing inputs).  This may change in the future
            to include resolved output `DatasetRef` objects.

        Notes
        -----
        A topological ordering is not in general unique, but no other
        guarantees are made about the order in which quanta are processed.
        """
        return list(
            self.as_generator(register_dataset_types=register_dataset_types, save_versions=save_versions)
        )

    def as_generator(
        self, register_dataset_types: bool = False, save_versions: bool = True
    ) -> Iterator[Quantum]:
        """Yield quanta in the `QuantumGraph` in topological order.

        These quanta will be run as the returned generator is iterated
        over.  Use this method to run the quanta one at a time.
        Use `run` to run all quanta in the graph.

        Parameters
        ----------
        register_dataset_types : `bool`, optional
            If `True`, register all output dataset types before executing any
            quanta.
        save_versions : `bool`, optional
            If `True` (default), save a package versions dataset.

        Returns
        -------
        quanta : `Iterator` [ `Quantum` ]
            Executed quanta.  At present, these will contain only unresolved
            `DatasetRef` instances for output datasets, reflecting the state of
            the quantum just before it was run (but after any adjustments for
            predicted but now missing inputs).  This may change in the future
            to include resolved output `DatasetRef` objects.


        Notes
        -----
        Global initialization steps (see `PreExecInit`) are performed
        immediately when this method is called, but individual quanta are not
        actually executed until the returned iterator is iterated over.

        A topological ordering is not in general unique, but no other
        guarantees are made about the order in which quanta are processed.
        """
        task_factory = TaskFactory()
        pre_exec_init = PreExecInit(self.butler, task_factory)
        pre_exec_init.initialize(
            graph=self.quantum_graph, registerDatasetTypes=register_dataset_types, saveVersions=save_versions
        )
        single_quantum_executor = SingleQuantumExecutor(self.butler, task_factory)
        # Important that this returns a generator expression rather than being
        # a generator itself; that is what makes the PreExecInit stuff above
        # happen immediately instead of when the first quanta is executed,
        # which might be useful for callers who want to check the state of the
        # repo in between.
        return (single_quantum_executor.execute(qnode.taskDef, qnode.quantum) for qnode in self.quantum_graph)
