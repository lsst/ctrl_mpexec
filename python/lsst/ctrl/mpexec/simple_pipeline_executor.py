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

__all__ = ("SimplePipelineExecutor",)

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from lsst.daf.butler import Butler, CollectionType, Quantum
from lsst.pex.config import Config
from lsst.pipe.base import ExecutionResources, Instrument, Pipeline, PipelineGraph, PipelineTask, QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder

from .preExecInit import PreExecInit
from .singleQuantumExecutor import SingleQuantumExecutor
from .taskFactory import TaskFactory


class SimplePipelineExecutor:
    """A simple, high-level executor for pipelines.

    Parameters
    ----------
    quantum_graph : `~lsst.pipe.base.QuantumGraph`
        Graph to be executed.
    butler : `~lsst.daf.butler.Butler`
        Object that manages all I/O.  Must be initialized with `collections`
        and `run` properties that correspond to the input and output
        collections, which must be consistent with those used to create
        ``quantum_graph``.
    resources : `~lsst.pipe.base.ExecutionResources`
        The resources available to each quantum being executed.
    raise_on_partial_outputs : `bool`, optional
        If `True` raise exceptions chained by
        `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead of
        considering the partial result a success and continuing to run
        downstream tasks.

    Notes
    -----
    Most callers should use one of the `classmethod` factory functions
    (`from_pipeline_filename`, `from_task_class`, `from_pipeline`) instead of
    invoking the constructor directly; these guarantee that the
    `~lsst.daf.butler.Butler` and `~lsst.pipe.base.QuantumGraph` are created
    consistently.

    This class is intended primarily to support unit testing and small-scale
    integration testing of `~lsst.pipe.base.PipelineTask` classes.  It
    deliberately lacks many features present in the command-line-only
    ``pipetask`` tool in order to keep the implementation simple.  Python
    callers that need more sophistication should call lower-level tools like
    `~lsst.pipe.base.quantum_graph_builder.QuantumGraphBuilder`, `PreExecInit`,
    and `SingleQuantumExecutor` directly.
    """

    def __init__(
        self,
        quantum_graph: QuantumGraph,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ):
        self.quantum_graph = quantum_graph
        self.butler = butler
        self.resources = resources
        self.raise_on_partial_outputs = raise_on_partial_outputs

    @classmethod
    def prep_butler(
        cls,
        root: str,
        inputs: Iterable[str],
        output: str,
        output_run: str | None = None,
    ) -> Butler:
        """Return configured `~lsst.daf.butler.Butler`.

        Helper method for creating `~lsst.daf.butler.Butler` instances with
        collections appropriate for processing.

        Parameters
        ----------
        root : `str`
            Root of the butler data repository; must already exist, with all
            necessary input data.
        inputs : `~collections.abc.Iterable` [ `str` ]
            Collections to search for all input datasets, in search order.
        output : `str`
            Name of a new output `~lsst.daf.butler.CollectionType.CHAINED`
            collection to create that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~lsst.daf.butler.CollectionType.RUN` that will
            directly hold all output datasets.  If not provided, a name will
            be created from ``output`` and a timestamp.

        Returns
        -------
        butler : `~lsst.daf.butler.Butler`
            Butler client instance compatible with all `classmethod` factories.
            Always writeable.
        """
        if output_run is None:
            output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"
        # Make initial butler with no collections, since we haven't created
        # them yet.
        butler = Butler.from_config(root, writeable=True)
        butler.registry.registerCollection(output_run, CollectionType.RUN)
        butler.registry.registerCollection(output, CollectionType.CHAINED)
        collections = [output_run]
        collections.extend(inputs)
        butler.registry.setCollectionChain(output, collections)
        # Remake butler to let it infer default data IDs from collections, now
        # that those collections exist.
        return Butler.from_config(butler=butler, collections=[output], run=output_run)

    @classmethod
    def from_pipeline_filename(
        cls,
        pipeline_filename: str,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an on-disk
        pipeline YAML file.

        Parameters
        ----------
        pipeline_filename : `str`
            Name of the YAML file to load the pipeline definition from.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.
        resources : `~lsst.pipe.base.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead
            of considering the partial result a success and continuing to run
            downstream tasks.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed
            `~lsst.pipe.base.QuantumGraph` and `~lsst.daf.butler.Butler`, ready
            for `run` to be called.
        """
        pipeline = Pipeline.fromFile(pipeline_filename)
        return cls.from_pipeline(
            pipeline,
            butler=butler,
            where=where,
            bind=bind,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )

    @classmethod
    def from_task_class(
        cls,
        task_class: type[PipelineTask],
        config: Config | None = None,
        label: str | None = None,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from a pipeline
        containing a single task.

        Parameters
        ----------
        task_class : `type`
            A concrete `~lsst.pipe.base.PipelineTask` subclass.
        config : `~lsst.pex.config.Config`, optional
            Configuration for the task.  If not provided, task-level defaults
            will be used (no per-instrument overrides).
        label : `str`, optional
            Label for the task in its pipeline; defaults to
            ``task_class._DefaultName``.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.
        resources : `~lsst.pipe.base.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead
            of considering the partial result a success and continuing to run
            downstream tasks.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed
            `~lsst.pipe.base.QuantumGraph` and `~lsst.daf.butler.Butler`, ready
            for `run` to be called.
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
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task(label=label, task_class=task_class, config=config)
        return cls.from_pipeline_graph(
            pipeline_graph,
            butler=butler,
            where=where,
            bind=bind,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Pipeline,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an in-memory
        pipeline.

        Parameters
        ----------
        pipeline : `~lsst.pipe.base.Pipeline` or \
                `~collections.abc.Iterable` [ `~lsst.pipe.base.TaskDef` ]
            A Python object describing the tasks to run, along with their
            labels and configuration.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.
        resources : `~lsst.pipe.base.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead
            of considering the partial result a success and continuing to run
            downstream tasks.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed
            `~lsst.pipe.base.QuantumGraph` and `~lsst.daf.butler.Butler`, ready
            for `run` to be called.
        """
        pipeline_graph = pipeline.to_graph()
        return cls.from_pipeline_graph(
            pipeline_graph,
            where=where,
            bind=bind,
            butler=butler,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )

    @classmethod
    def from_pipeline_graph(
        cls,
        pipeline_graph: PipelineGraph,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an in-memory
        pipeline graph.

        Parameters
        ----------
        pipeline_graph : `~lsst.pipe.base.PipelineGraph`
            A Python object describing the tasks to run, along with their
            labels and configuration, in graph form.  Will be resolved against
            the given ``butler``, with any existing resolutions ignored.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.  Must have its `~Butler.run` and
            `~Butler.collections.defaults` not empty and not `None`.
        resources : `~lsst.pipe.base.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead
            of considering the partial result a success and continuing to run
            downstream tasks.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed
            `~lsst.pipe.base.QuantumGraph` and `~lsst.daf.butler.Butler`, ready
            for `run` to be called.
        """
        quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph, butler, where=where, bind=bind
        )
        quantum_graph = quantum_graph_builder.build(attach_datastore_records=False)
        return cls(
            quantum_graph=quantum_graph,
            butler=butler,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )

    def run(self, register_dataset_types: bool = False, save_versions: bool = True) -> list[Quantum]:
        """Run all the quanta in the `~lsst.pipe.base.QuantumGraph` in
        topological order.

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
        quanta : `list` [ `~lsst.daf.butler.Quantum` ]
            Executed quanta.

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
        """Yield quanta in the `~lsst.pipe.base.QuantumGraph` in topological
        order.

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
        quanta : `~collections.abc.Iterator` [ `~lsst.daf.butler.Quantum` ]
            Executed quanta.

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
        single_quantum_executor = SingleQuantumExecutor(
            self.butler,
            task_factory,
            resources=self.resources,
            raise_on_partial_outputs=self.raise_on_partial_outputs,
        )
        # Important that this returns a generator expression rather than being
        # a generator itself; that is what makes the PreExecInit stuff above
        # happen immediately instead of when the first quanta is executed,
        # which might be useful for callers who want to check the state of the
        # repo in between.
        return (
            single_quantum_executor.execute(qnode.task_node, qnode.quantum)[0] for qnode in self.quantum_graph
        )
