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

"""Module defining CmdLineFwk class and related methods."""

from __future__ import annotations

__all__ = ["CmdLineFwk"]

import atexit
import contextlib
import copy
import logging
import pickle
import shutil
from collections.abc import Mapping, Sequence
from types import SimpleNamespace

import astropy.units as u
from astropy.table import Table

import lsst.utils.timer
from lsst.daf.butler import (
    Butler,
    CollectionType,
    Config,
    DatasetType,
    DimensionConfig,
    DimensionUniverse,
    LimitedButler,
    Quantum,
    QuantumBackedButler,
    Registry,
)
from lsst.daf.butler.datastore.cache_manager import DatastoreCacheManager
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import MissingCollectionError, RegistryDefaults
from lsst.daf.butler.registry.wildcards import CollectionWildcard
from lsst.pipe.base import (
    ExecutionResources,
    Instrument,
    Pipeline,
    PipelineGraph,
    QuantumGraph,
    TaskFactory,
    buildExecutionButler,
)
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.dot_tools import graph2dot
from lsst.pipe.base.pipeline_graph import NodeType
from lsst.utils import doImportType
from lsst.utils.logging import getLogger
from lsst.utils.threads import disable_implicit_threading

from .executionGraphFixup import ExecutionGraphFixup
from .mpGraphExecutor import MPGraphExecutor
from .preExecInit import PreExecInit, PreExecInitLimited
from .reports import Report
from .singleQuantumExecutor import SingleQuantumExecutor

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = getLogger(__name__)


class _OutputChainedCollectionInfo:
    """A helper class for handling command-line arguments related to an output
    `~lsst.daf.butler.CollectionType.CHAINED` collection.

    Parameters
    ----------
    registry : `lsst.daf.butler.Registry`
        Butler registry that collections will be added to and/or queried from.
    name : `str`
        Name of the collection given on the command line.
    """

    def __init__(self, registry: Registry, name: str):
        self.name = name
        try:
            self.chain = tuple(registry.getCollectionChain(name))
            self.exists = True
        except MissingCollectionError:
            self.chain = ()
            self.exists = False

    def __str__(self) -> str:
        return self.name

    name: str
    """Name of the collection provided on the command line (`str`).
    """

    exists: bool
    """Whether this collection already exists in the registry (`bool`).
    """

    chain: tuple[str, ...]
    """The definition of the collection, if it already exists (`tuple`[`str`]).

    Empty if the collection does not already exist.
    """


class _OutputRunCollectionInfo:
    """A helper class for handling command-line arguments related to an output
    `~lsst.daf.butler.CollectionType.RUN` collection.

    Parameters
    ----------
    registry : `lsst.daf.butler.Registry`
        Butler registry that collections will be added to and/or queried from.
    name : `str`
        Name of the collection given on the command line.
    """

    def __init__(self, registry: Registry, name: str):
        self.name = name
        try:
            actualType = registry.getCollectionType(name)
            if actualType is not CollectionType.RUN:
                raise TypeError(f"Collection '{name}' exists but has type {actualType.name}, not RUN.")
            self.exists = True
        except MissingCollectionError:
            self.exists = False

    name: str
    """Name of the collection provided on the command line (`str`).
    """

    exists: bool
    """Whether this collection already exists in the registry (`bool`).
    """


class _ButlerFactory:
    """A helper class for processing command-line arguments related to input
    and output collections.

    Parameters
    ----------
    registry : `lsst.daf.butler.Registry`
        Butler registry that collections will be added to and/or queried from.

    args : `types.SimpleNamespace`
        Parsed command-line arguments.  The following attributes are used,
        either at construction or in later methods.

        ``output``
            The name of a `~lsst.daf.butler.CollectionType.CHAINED`
            input/output collection.

        ``output_run``
            The name of a `~lsst.daf.butler.CollectionType.RUN` input/output
            collection.

        ``extend_run``
            A boolean indicating whether ``output_run`` should already exist
            and be extended.

        ``replace_run``
            A boolean indicating that (if `True`) ``output_run`` should already
            exist but will be removed from the output chained collection and
            replaced with a new one.

        ``prune_replaced``
            A boolean indicating whether to prune the replaced run (requires
            ``replace_run``).

        ``rebase``
            A boolean indicating whether to force the ``output`` collection
            to be consistent with ``inputs`` and ``output`` run such that the
            ``output`` collection has output run collections first (i.e. those
            that start with the same prefix), then the new inputs, then any
            original inputs not included in the new inputs.

        ``inputs``
            Input collections of any type; see
            :ref:`daf_butler_ordered_collection_searches` for details.

        ``butler_config``
            Path to a data repository root or configuration file.

    writeable : `bool`
        If `True`, a `~lsst.daf.butler.Butler` is being initialized in a
        context where actual writes should happens, and hence no output run
        is necessary.

    Raises
    ------
    ValueError
        Raised if ``writeable is True`` but there are no output collections.
    """

    def __init__(self, registry: Registry, args: SimpleNamespace, writeable: bool):
        if args.output is not None:
            self.output = _OutputChainedCollectionInfo(registry, args.output)
        else:
            self.output = None
        if args.output_run is not None:
            if args.rebase and self.output and not args.output_run.startswith(self.output.name):
                raise ValueError("Cannot rebase if output run does not start with output collection name.")
            self.outputRun = _OutputRunCollectionInfo(registry, args.output_run)
        elif self.output is not None:
            if args.extend_run:
                if not self.output.chain:
                    raise ValueError("Cannot use --extend-run option with non-existing or empty output chain")
                runName = self.output.chain[0]
            else:
                runName = f"{self.output}/{Instrument.makeCollectionTimestamp()}"
            self.outputRun = _OutputRunCollectionInfo(registry, runName)
        elif not writeable:
            # If we're not writing yet, ok to have no output run.
            self.outputRun = None
        else:
            raise ValueError("Cannot write without at least one of (--output, --output-run).")
        # Recursively flatten any input CHAINED collections.  We do this up
        # front so we can tell if the user passes the same inputs on subsequent
        # calls, even though we also flatten when we define the output CHAINED
        # collection.
        self.inputs = tuple(registry.queryCollections(args.input, flattenChains=True)) if args.input else ()

        # If things are inconsistent and user has asked for a rebase then
        # construct the new output chain.
        if args.rebase and self._checkOutputInputConsistency():
            assert self.output is not None
            newOutputChain = [item for item in self.output.chain if item.startswith(self.output.name)]
            newOutputChain.extend([item for item in self.inputs if item not in newOutputChain])
            newOutputChain.extend([item for item in self.output.chain if item not in newOutputChain])
            self.output.chain = tuple(newOutputChain)

    def check(self, args: SimpleNamespace) -> None:
        """Check command-line options for consistency with each other and the
        data repository.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.
        """
        assert not (args.extend_run and args.replace_run), "In mutually-exclusive group in ArgumentParser."
        if consistencyError := self._checkOutputInputConsistency():
            raise ValueError(consistencyError)

        if args.extend_run:
            if self.outputRun is None:
                raise ValueError("Cannot --extend-run when no output collection is given.")
            elif not self.outputRun.exists:
                raise ValueError(
                    f"Cannot --extend-run; output collection '{self.outputRun.name}' does not exist."
                )
        if not args.extend_run and self.outputRun is not None and self.outputRun.exists:
            raise ValueError(
                f"Output run '{self.outputRun.name}' already exists, but --extend-run was not given."
            )
        if args.prune_replaced and not args.replace_run:
            raise ValueError("--prune-replaced requires --replace-run.")
        if args.replace_run and (self.output is None or not self.output.exists):
            raise ValueError("--output must point to an existing CHAINED collection for --replace-run.")

    def _checkOutputInputConsistency(self) -> str | None:
        if self.inputs and self.output is not None and self.output.exists:
            # Passing the same inputs that were used to initialize the output
            # collection is allowed; this means the inputs must appear as a
            # contiguous subsequence of outputs (normally they're also at the
            # end, but --rebase will in general put them in the middle).
            for n in reversed(range(1 + len(self.output.chain) - len(self.inputs))):
                if self.inputs == self.output.chain[n : n + len(self.inputs)]:
                    return None
            return (
                f"Output CHAINED collection {self.output.name!r} exists and does not include the "
                f"same sequence of (flattened) input collections {self.inputs} as a contiguous "
                "subsequence. "
                "Use --rebase to ignore this problem and reset the output collection, but note that "
                "this may obfuscate what inputs were actually used to produce these outputs."
            )
        return None

    @classmethod
    def _makeReadParts(cls, args: SimpleNamespace) -> tuple[Butler, Sequence[str], _ButlerFactory]:
        """Parse arguments to support implementations of `makeReadButler` and
        `makeButlerAndCollections`.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler constructed from the repo at
            ``args.butler_config``, but with no default collections.
        inputs : `~collections.abc.Sequence` [ `str` ]
            A collection search path constructed according to ``args``.
        self : `_ButlerFactory`
            A new `_ButlerFactory` instance representing the processed version
            of ``args``.
        """
        butler = Butler.from_config(args.butler_config, writeable=False)
        self = cls(butler.registry, args, writeable=False)
        self.check(args)
        if self.output and self.output.exists:
            if args.replace_run:
                replaced = self.output.chain[0]
                inputs = list(self.output.chain[1:])
                _LOG.debug(
                    "Simulating collection search in '%s' after removing '%s'.", self.output.name, replaced
                )
            else:
                inputs = [self.output.name]
        else:
            inputs = list(self.inputs)
        if args.extend_run:
            assert self.outputRun is not None, "Output collection has to be specified."
            inputs.insert(0, self.outputRun.name)
        collSearch = CollectionWildcard.from_expression(inputs).require_ordered()
        return butler, collSearch, self

    @classmethod
    def makeReadButler(cls, args: SimpleNamespace) -> Butler:
        """Construct a read-only butler according to the given command-line
        arguments.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler initialized with the collections specified by
            ``args``.
        """
        cls.defineDatastoreCache()  # Ensure that this butler can use a shared cache.
        butler, inputs, _ = cls._makeReadParts(args)
        _LOG.debug("Preparing butler to read from %s.", inputs)
        return Butler.from_config(butler=butler, collections=inputs)

    @classmethod
    def makeButlerAndCollections(cls, args: SimpleNamespace) -> tuple[Butler, Sequence[str], str | None]:
        """Return a read-only registry, a collection search path, and the name
        of the run to be used for future writes.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler that collections will be added to and/or queried
            from.
        inputs : `Sequence` [ `str` ]
            Collections to search for datasets.
        run : `str` or `None`
            Name of the output `~lsst.daf.butler.CollectionType.RUN` collection
            if it already exists, or `None` if it does not.
        """
        butler, inputs, self = cls._makeReadParts(args)
        run: str | None = None
        if args.extend_run:
            assert self.outputRun is not None, "Output collection has to be specified."
        if self.outputRun is not None:
            run = self.outputRun.name
        _LOG.debug("Preparing registry to read from %s and expect future writes to '%s'.", inputs, run)
        return butler, inputs, run

    @staticmethod
    def defineDatastoreCache() -> None:
        """Define where datastore cache directories should be found.

        Notes
        -----
        All the jobs should share a datastore cache if applicable. This
        method asks for a shared fallback cache to be defined and then
        configures an exit handler to clean it up.
        """
        defined, cache_dir = DatastoreCacheManager.set_fallback_cache_directory_if_unset()
        if defined:
            atexit.register(shutil.rmtree, cache_dir, ignore_errors=True)
            _LOG.debug("Defining shared datastore cache directory to %s", cache_dir)

    @classmethod
    def makeWriteButler(cls, args: SimpleNamespace, pipeline_graph: PipelineGraph | None = None) -> Butler:
        """Return a read-write butler initialized to write to and read from
        the collections specified by the given command-line arguments.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.
        pipeline_graph : `lsst.pipe.base.PipelineGraph`, optional
            Definitions for tasks in a pipeline. This argument is only needed
            if ``args.replace_run`` is `True` and ``args.prune_replaced`` is
            "unstore".

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-write butler initialized according to the given arguments.
        """
        cls.defineDatastoreCache()  # Ensure that this butler can use a shared cache.
        butler = Butler.from_config(args.butler_config, writeable=True)
        self = cls(butler.registry, args, writeable=True)
        self.check(args)
        assert self.outputRun is not None, "Output collection has to be specified."  # for mypy
        if self.output is not None:
            chainDefinition = list(self.output.chain if self.output.exists else self.inputs)
            if args.replace_run:
                replaced = chainDefinition.pop(0)
                if args.prune_replaced == "unstore":
                    # Remove datasets from datastore
                    with butler.transaction():
                        # we want to remove regular outputs from this pipeline,
                        # but keep initOutputs, configs, and versions.
                        if pipeline_graph is not None:
                            refs = [
                                ref
                                for ref in butler.registry.queryDatasets(..., collections=replaced)
                                if (
                                    (producer := pipeline_graph.producer_of(ref.datasetType.name)) is not None
                                    and producer.key.node_type is NodeType.TASK  # i.e. not TASK_INIT
                                )
                            ]
                        butler.pruneDatasets(refs, unstore=True, disassociate=False)
                elif args.prune_replaced == "purge":
                    # Erase entire collection and all datasets, need to remove
                    # collection from its chain collection first.
                    with butler.transaction():
                        butler.registry.setCollectionChain(self.output.name, chainDefinition, flatten=True)
                        butler.removeRuns([replaced], unstore=True)
                elif args.prune_replaced is not None:
                    raise NotImplementedError(f"Unsupported --prune-replaced option '{args.prune_replaced}'.")
            if not self.output.exists:
                butler.registry.registerCollection(self.output.name, CollectionType.CHAINED)
            if not args.extend_run:
                butler.registry.registerCollection(self.outputRun.name, CollectionType.RUN)
                chainDefinition.insert(0, self.outputRun.name)
                butler.registry.setCollectionChain(self.output.name, chainDefinition, flatten=True)
            _LOG.debug(
                "Preparing butler to write to '%s' and read from '%s'=%s",
                self.outputRun.name,
                self.output.name,
                chainDefinition,
            )
            butler.registry.defaults = RegistryDefaults(run=self.outputRun.name, collections=self.output.name)
        else:
            inputs = (self.outputRun.name,) + self.inputs
            _LOG.debug("Preparing butler to write to '%s' and read from %s.", self.outputRun.name, inputs)
            butler.registry.defaults = RegistryDefaults(run=self.outputRun.name, collections=inputs)
        return butler

    output: _OutputChainedCollectionInfo | None
    """Information about the output chained collection, if there is or will be
    one (`_OutputChainedCollectionInfo` or `None`).
    """

    outputRun: _OutputRunCollectionInfo | None
    """Information about the output run collection, if there is or will be
    one (`_OutputRunCollectionInfo` or `None`).
    """

    inputs: tuple[str, ...]
    """Input collections provided directly by the user (`tuple` [ `str` ]).
    """


class _QBBFactory:
    """Class which is a callable for making QBB instances.

    This class is also responsible for reconstructing correct dimension
    universe after unpickling. When pickling multiple things that require
    dimension universe, this class must be unpickled first. The logic in
    MPGraphExecutor ensures that SingleQuantumExecutor is unpickled first in
    the subprocess, which causes unpickling of this class.
    """

    def __init__(
        self, butler_config: Config, dimensions: DimensionUniverse, dataset_types: Mapping[str, DatasetType]
    ):
        self.butler_config = butler_config
        self.dimensions = dimensions
        self.dataset_types = dataset_types

    def __call__(self, quantum: Quantum) -> LimitedButler:
        """Return freshly initialized `~lsst.daf.butler.QuantumBackedButler`.

        Factory method to create QuantumBackedButler instances.
        """
        return QuantumBackedButler.initialize(
            config=self.butler_config,
            quantum=quantum,
            dimensions=self.dimensions,
            dataset_types=self.dataset_types,
        )

    @classmethod
    def _unpickle(
        cls, butler_config: Config, dimensions_config: DimensionConfig | None, dataset_types_pickle: bytes
    ) -> _QBBFactory:
        universe = DimensionUniverse(dimensions_config)
        dataset_types = pickle.loads(dataset_types_pickle)
        return _QBBFactory(butler_config, universe, dataset_types)

    def __reduce__(self) -> tuple:
        # If dimension universe is not default one, we need to dump/restore
        # its config.
        config = self.dimensions.dimensionConfig
        default = DimensionConfig()
        # Only send configuration to other side if it is non-default, default
        # will be instantiated from config=None.
        if (config["namespace"], config["version"]) != (default["namespace"], default["version"]):
            dimension_config = config
        else:
            dimension_config = None
        # Dataset types need to be unpickled only after universe is made.
        dataset_types_pickle = pickle.dumps(self.dataset_types)
        return (self._unpickle, (self.butler_config, dimension_config, dataset_types_pickle))


# ------------------------
#  Exported definitions --
# ------------------------


class CmdLineFwk:
    """PipelineTask framework which executes tasks from command line.

    In addition to executing tasks this activator provides additional methods
    for task management like dumping configuration or execution chain.
    """

    MP_TIMEOUT = 3600 * 24 * 30  # Default timeout (sec) for multiprocessing

    def __init__(self) -> None:
        pass

    def makePipeline(self, args: SimpleNamespace) -> Pipeline:
        """Build a pipeline from command line arguments.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command line.

        Returns
        -------
        pipeline : `~lsst.pipe.base.Pipeline`
            Newly-constructed pipeline.
        """
        if args.pipeline:
            pipeline = Pipeline.from_uri(args.pipeline)
        else:
            pipeline = Pipeline("anonymous")

        # loop over all pipeline actions and apply them in order
        for action in args.pipeline_actions:
            if action.action == "add_instrument":
                pipeline.addInstrument(action.value)

            elif action.action == "new_task":
                pipeline.addTask(action.value, action.label)

            elif action.action == "delete_task":
                pipeline.removeTask(action.label)

            elif action.action == "config":
                # action value string is "field=value", split it at '='
                field, _, value = action.value.partition("=")
                pipeline.addConfigOverride(action.label, field, value)

            elif action.action == "configfile":
                pipeline.addConfigFile(action.label, action.value)

            else:
                raise ValueError(f"Unexpected pipeline action: {action.action}")

        if args.save_pipeline:
            pipeline.write_to_uri(args.save_pipeline)

        return pipeline

    def makeGraph(self, pipeline: Pipeline, args: SimpleNamespace) -> QuantumGraph | None:
        """Build a graph from command line arguments.

        Parameters
        ----------
        pipeline : `~lsst.pipe.base.Pipeline`
            Pipeline, can be empty or ``None`` if graph is read from a file.
        args : `types.SimpleNamespace`
            Parsed command line.

        Returns
        -------
        graph : `~lsst.pipe.base.QuantumGraph` or `None`
            If resulting graph is empty then `None` is returned.
        """
        # make sure that --extend-run always enables --skip-existing
        if args.extend_run:
            args.skip_existing = True

        butler, collections, run = _ButlerFactory.makeButlerAndCollections(args)

        if args.skip_existing and run:
            args.skip_existing_in += (run,)

        if args.qgraph:
            # click passes empty tuple as default value for qgraph_node_id
            nodes = args.qgraph_node_id or None
            qgraph = QuantumGraph.loadUri(args.qgraph, butler.dimensions, nodes=nodes, graphID=args.qgraph_id)

            # pipeline can not be provided in this case
            if pipeline:
                raise ValueError("Pipeline must not be given when quantum graph is read from file.")
            if args.show_qgraph_header:
                print(QuantumGraph.readHeader(args.qgraph))
        else:
            pipeline_graph = pipeline.to_graph()
            if args.mock:
                from lsst.pipe.base.tests.mocks import mock_pipeline_graph

                pipeline_graph = mock_pipeline_graph(
                    pipeline_graph,
                    unmocked_dataset_types=args.unmocked_dataset_types,
                    force_failures=args.mock_failure,
                )
            # make execution plan (a.k.a. DAG) for pipeline
            graph_builder = AllDimensionsQuantumGraphBuilder(
                pipeline_graph,
                butler,
                where=args.data_query or "",
                skip_existing_in=args.skip_existing_in if args.skip_existing_in is not None else (),
                clobber=args.clobber_outputs,
                dataset_query_constraint=args.dataset_query_constraint,
                input_collections=collections,
                output_run=run,
            )
            # accumulate metadata
            metadata = {
                "input": args.input,
                "output": args.output,
                "butler_argument": args.butler_config,
                "output_run": run,
                "extend_run": args.extend_run,
                "skip_existing_in": args.skip_existing_in,
                "skip_existing": args.skip_existing,
                "data_query": args.data_query or "",
            }
            assert run is not None, "Butler output run collection must be defined"
            qgraph = graph_builder.build(metadata, attach_datastore_records=args.qgraph_datastore_records)
            if args.show_qgraph_header:
                qgraph.buildAndPrintHeader()

        if len(qgraph) == 0:
            # Nothing to do.
            return None
        self._summarize_qgraph(qgraph)

        if args.save_qgraph:
            _LOG.verbose("Writing QuantumGraph to %r.", args.save_qgraph)
            qgraph.saveUri(args.save_qgraph)

        if args.save_single_quanta:
            for quantumNode in qgraph:
                sqgraph = qgraph.subset(quantumNode)
                uri = args.save_single_quanta.format(quantumNode)
                sqgraph.saveUri(uri)

        if args.qgraph_dot:
            _LOG.verbose("Writing quantum graph DOT visualization to %r.", args.qgraph_dot)
            graph2dot(qgraph, args.qgraph_dot)

        if args.execution_butler_location:
            _LOG.verbose("Writing execution butler to %r.", args.execution_butler_location)
            butler = Butler.from_config(args.butler_config)
            assert isinstance(butler, DirectButler), "Execution butler needs DirectButler"
            newArgs = copy.deepcopy(args)

            def builderShim(butler: Butler) -> Butler:
                assert isinstance(butler, DirectButler), "Execution butler needs DirectButler"
                newArgs.butler_config = butler._config
                # Calling makeWriteButler is done for the side effects of
                # calling that method, maining parsing all the args into
                # collection names, creating collections, etc.
                newButler = _ButlerFactory.makeWriteButler(newArgs)
                return newButler

            # Include output collection in collections for input
            # files if it exists in the repo.
            all_inputs = args.input
            if args.output is not None:
                with contextlib.suppress(MissingCollectionError):
                    all_inputs += (next(iter(butler.registry.queryCollections(args.output))),)

            _LOG.debug("Calling buildExecutionButler with collections=%s", all_inputs)
            buildExecutionButler(
                butler,
                qgraph,
                args.execution_butler_location,
                run,
                butlerModifier=builderShim,
                collections=all_inputs,
                clobber=args.clobber_execution_butler,
                datastoreRoot=args.target_datastore_root,
                transfer=args.transfer,
            )

        return qgraph

    def _make_execution_resources(self, args: SimpleNamespace) -> ExecutionResources:
        """Construct the execution resource class from arguments.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command line.

        Returns
        -------
        resources : `~lsst.pipe.base.ExecutionResources`
            The resources available to each quantum.
        """
        return ExecutionResources(
            num_cores=args.cores_per_quantum, max_mem=args.memory_per_quantum, default_mem_units=u.MB
        )

    def runPipeline(
        self,
        graph: QuantumGraph,
        taskFactory: TaskFactory,
        args: SimpleNamespace,
        butler: Butler | None = None,
    ) -> None:
        """Execute complete QuantumGraph.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        args : `types.SimpleNamespace`
            Parsed command line.
        butler : `~lsst.daf.butler.Butler`, optional
            Data Butler instance, if not defined then new instance is made
            using command line options.
        """
        if not args.enable_implicit_threading:
            disable_implicit_threading()

        # Check that output run defined on command line is consistent with
        # quantum graph.
        if args.output_run and graph.metadata:
            graph_output_run = graph.metadata.get("output_run", args.output_run)
            if graph_output_run != args.output_run:
                raise ValueError(
                    f"Output run defined on command line ({args.output_run}) has to be "
                    f"identical to graph metadata ({graph_output_run}). "
                    "To update graph metadata run `pipetask update-graph-run` command."
                )

        # Make sure that --extend-run always enables --skip-existing,
        # clobbering should be disabled if --extend-run is not specified.
        if args.extend_run:
            args.skip_existing = True
        else:
            args.clobber_outputs = False

        # Make butler instance. QuantumGraph should have an output run defined,
        # but we ignore it here and let command line decide actual output run.
        if butler is None:
            butler = _ButlerFactory.makeWriteButler(args, graph.pipeline_graph)

        if args.skip_existing:
            args.skip_existing_in += (butler.run,)

        # Enable lsstDebug debugging. Note that this is done once in the
        # main process before PreExecInit and it is also repeated before
        # running each task in SingleQuantumExecutor (which may not be
        # needed if `multiprocessing` always uses fork start method).
        if args.enableLsstDebug:
            try:
                _LOG.debug("Will try to import debug.py")
                import debug  # type: ignore # noqa:F401
            except ImportError:
                _LOG.warn("No 'debug' module found.")

        # Save all InitOutputs, configs, etc.
        preExecInit = PreExecInit(butler, taskFactory, extendRun=args.extend_run)
        preExecInit.initialize(
            graph,
            saveInitOutputs=not args.skip_init_writes,
            registerDatasetTypes=args.register_dataset_types,
            saveVersions=not args.no_versions,
        )

        if not args.init_only:
            graphFixup = self._importGraphFixup(args)
            resources = self._make_execution_resources(args)
            quantumExecutor = SingleQuantumExecutor(
                butler,
                taskFactory,
                skipExistingIn=args.skip_existing_in,
                clobberOutputs=args.clobber_outputs,
                enableLsstDebug=args.enableLsstDebug,
                resources=resources,
                raise_on_partial_outputs=args.raise_on_partial_outputs,
            )

            timeout = self.MP_TIMEOUT if args.timeout is None else args.timeout
            executor = MPGraphExecutor(
                numProc=args.processes,
                timeout=timeout,
                startMethod=args.start_method,
                quantumExecutor=quantumExecutor,
                failFast=args.fail_fast,
                pdb=args.pdb,
                executionGraphFixup=graphFixup,
            )
            # Have to reset connection pool to avoid sharing connections with
            # forked processes.
            butler.registry.resetConnectionPool()
            try:
                with lsst.utils.timer.profile(args.profile, _LOG):
                    executor.execute(graph)
            finally:
                if args.summary:
                    report = executor.getReport()
                    if report:
                        with open(args.summary, "w") as out:
                            # Do not save fields that are not set.
                            out.write(report.model_dump_json(exclude_none=True, indent=2))

    def _generateTaskTable(self) -> Table:
        """Generate astropy table listing the number of quanta per task for a
        given quantum graph.

        Returns
        -------
        qg_task_table : `astropy.table.table.Table`
            An astropy table containing columns: Quanta and Tasks.
        """
        qg_quanta, qg_tasks = [], []
        for task_label, task_info in self.report.qgraphSummary.qgraphTaskSummaries.items():
            qg_tasks.append(task_label)
            qg_quanta.append(task_info.numQuanta)

        qg_task_table = Table(dict(Quanta=qg_quanta, Tasks=qg_tasks))
        return qg_task_table

    def _summarize_qgraph(self, qgraph: QuantumGraph) -> int:
        """Report a summary of the quanta in the graph.

        Parameters
        ----------
        qgraph : `lsst.pipe.base.QuantumGraph`
            The graph to be summarized.

        Returns
        -------
        n_quanta : `int`
            The number of quanta in the graph.
        """
        n_quanta = len(qgraph)
        if n_quanta == 0:
            _LOG.info("QuantumGraph contains no quanta.")
        else:
            self.report = Report(qgraphSummary=qgraph.getSummary())
            if _LOG.isEnabledFor(logging.INFO):
                qg_task_table = self._generateTaskTable()
                qg_task_table_formatted = "\n".join(qg_task_table.pformat())
                quanta_str = "quantum" if n_quanta == 1 else "quanta"
                n_tasks = len(qgraph.taskGraph)
                n_tasks_plural = "" if n_tasks == 1 else "s"
                _LOG.info(
                    "QuantumGraph contains %d %s for %d task%s, graph ID: %r\n%s",
                    n_quanta,
                    quanta_str,
                    n_tasks,
                    n_tasks_plural,
                    qgraph.graphID,
                    qg_task_table_formatted,
                )
        return n_quanta

    def _importGraphFixup(self, args: SimpleNamespace) -> ExecutionGraphFixup | None:
        """Import/instantiate graph fixup object.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command line.

        Returns
        -------
        fixup : `ExecutionGraphFixup` or `None`

        Raises
        ------
        ValueError
            Raised if import fails, method call raises exception, or returned
            instance has unexpected type.
        """
        if args.graph_fixup:
            try:
                factory = doImportType(args.graph_fixup)
            except Exception as exc:
                raise ValueError("Failed to import graph fixup class/method") from exc
            try:
                fixup = factory()
            except Exception as exc:
                raise ValueError("Failed to make instance of graph fixup") from exc
            if not isinstance(fixup, ExecutionGraphFixup):
                raise ValueError("Graph fixup is not an instance of ExecutionGraphFixup class")
            return fixup
        return None

    def preExecInitQBB(self, task_factory: TaskFactory, args: SimpleNamespace) -> None:
        # Load quantum graph. We do not really need individual Quanta here,
        # but we need datastore records for initInputs, and those are only
        # available from Quanta, so load the whole thing.
        qgraph = QuantumGraph.loadUri(args.qgraph, graphID=args.qgraph_id)
        # Make QBB.
        butler = qgraph.make_init_qbb(args.butler_config, config_search_paths=args.config_search_path)
        # Save all InitOutputs, configs, etc.
        preExecInit = PreExecInitLimited(butler, task_factory)
        preExecInit.initialize(qgraph)

    def runGraphQBB(self, task_factory: TaskFactory, args: SimpleNamespace) -> None:
        if not args.enable_implicit_threading:
            disable_implicit_threading()

        # Load quantum graph.
        nodes = args.qgraph_node_id or None
        qgraph = QuantumGraph.loadUri(args.qgraph, nodes=nodes, graphID=args.qgraph_id)

        if qgraph.metadata is None:
            raise ValueError("QuantumGraph is missing metadata, cannot continue.")

        self._summarize_qgraph(qgraph)

        dataset_types = {dstype.name: dstype for dstype in qgraph.registryDatasetTypes()}

        _butler_factory = _QBBFactory(
            butler_config=args.butler_config,
            dimensions=qgraph.universe,
            dataset_types=dataset_types,
        )

        # make special quantum executor
        resources = self._make_execution_resources(args)
        quantumExecutor = SingleQuantumExecutor(
            butler=None,
            taskFactory=task_factory,
            enableLsstDebug=args.enableLsstDebug,
            limited_butler_factory=_butler_factory,
            resources=resources,
            assumeNoExistingOutputs=True,
            raise_on_partial_outputs=args.raise_on_partial_outputs,
        )

        timeout = self.MP_TIMEOUT if args.timeout is None else args.timeout
        executor = MPGraphExecutor(
            numProc=args.processes,
            timeout=timeout,
            startMethod=args.start_method,
            quantumExecutor=quantumExecutor,
            failFast=args.fail_fast,
            pdb=args.pdb,
        )
        try:
            with lsst.utils.timer.profile(args.profile, _LOG):
                executor.execute(qgraph)
        finally:
            if args.summary:
                report = executor.getReport()
                if report:
                    with open(args.summary, "w") as out:
                        # Do not save fields that are not set.
                        out.write(report.model_dump_json(exclude_none=True, indent=2))
