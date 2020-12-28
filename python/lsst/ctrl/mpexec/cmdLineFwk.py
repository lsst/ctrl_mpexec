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

"""Module defining CmdLineFwk class and related methods.
"""

__all__ = ['CmdLineFwk']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import fnmatch
import logging
import re
import sys
from typing import Optional, Tuple
import warnings

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import (
    Butler,
    CollectionSearch,
    CollectionType,
    Registry,
)
from lsst.daf.butler.registry import MissingCollectionError
import lsst.pex.config as pexConfig
from lsst.pipe.base import GraphBuilder, Pipeline, QuantumGraph
from lsst.obs.base import Instrument
from .dotTools import graph2dot, pipeline2dot
from .executionGraphFixup import ExecutionGraphFixup
from .mpGraphExecutor import MPGraphExecutor
from .preExecInit import PreExecInit
from .singleQuantumExecutor import SingleQuantumExecutor
from . import util
from lsst.utils import doImport

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


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

    def __str__(self):
        return self.name

    name: str
    """Name of the collection provided on the command line (`str`).
    """

    exists: bool
    """Whether this collection already exists in the registry (`bool`).
    """

    chain: Tuple[str, ...]
    """The definition of the collection, if it already exists (`tuple` [`str`]).

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

    args : `argparse.Namespace`
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

        ``inputs``
            Input collections of any type; may be any type handled by
            `lsst.daf.butler.registry.CollectionSearch.fromExpression`.

        ``butler_config``
            Path to a data repository root or configuration file.

    writeable : `bool`
        If `True`, a `Butler` is being initialized in a context where actual
        writes should happens, and hence no output run is necessary.

    Raises
    ------
    ValueError
        Raised if ``writeable is True`` but there are no output collections.
    """
    def __init__(self, registry: Registry, args: argparse.Namespace, writeable: bool):
        if args.output is not None:
            self.output = _OutputChainedCollectionInfo(registry, args.output)
        else:
            self.output = None
        if args.output_run is not None:
            self.outputRun = _OutputRunCollectionInfo(registry, args.output_run)
        elif self.output is not None:
            if args.extend_run:
                runName = self.output.chain[0]
            else:
                runName = "{}/{}".format(self.output, Instrument.makeCollectionTimestamp())
            self.outputRun = _OutputRunCollectionInfo(registry, runName)
        elif not writeable:
            # If we're not writing yet, ok to have no output run.
            self.outputRun = None
        else:
            raise ValueError("Cannot write without at least one of (--output, --output-run).")
        self.inputs = tuple(CollectionSearch.fromExpression(args.input)) if args.input else ()

    def check(self, args: argparse.Namespace):
        """Check command-line options for consistency with each other and the
        data repository.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.
        """
        assert not (args.extend_run and args.replace_run), "In mutually-exclusive group in ArgumentParser."
        if self.inputs and self.output is not None and self.output.exists:
            raise ValueError("Cannot use --output with existing collection with --inputs.")
        if args.extend_run and self.outputRun is None:
            raise ValueError("Cannot --extend-run when no output collection is given.")
        if args.extend_run and not self.outputRun.exists:
            raise ValueError(f"Cannot --extend-run; output collection "
                             f"'{self.outputRun.name}' does not exist.")
        if not args.extend_run and self.outputRun is not None and self.outputRun.exists:
            raise ValueError(f"Output run '{self.outputRun.name}' already exists, but "
                             f"--extend-run was not given.")
        if args.prune_replaced and not args.replace_run:
            raise ValueError("--prune-replaced requires --replace-run.")
        if args.replace_run and (self.output is None or not self.output.exists):
            raise ValueError("--output must point to an existing CHAINED collection for --replace-run.")

    @classmethod
    def _makeReadParts(cls, args: argparse.Namespace):
        """Common implementation for `makeReadButler` and
        `makeRegistryAndCollections`.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler constructed from the repo at
            ``args.butler_config``, but with no default collections.
        inputs : `lsst.daf.butler.registry.CollectionSearch`
            A collection search path constructed according to ``args``.
        self : `_ButlerFactory`
            A new `_ButlerFactory` instance representing the processed version
            of ``args``.
        """
        butler = Butler(args.butler_config, writeable=False)
        self = cls(butler.registry, args, writeable=False)
        self.check(args)
        if self.output and self.output.exists:
            if args.replace_run:
                replaced = self.output.chain[0]
                inputs = self.output.chain[1:]
                _LOG.debug("Simulating collection search in '%s' after removing '%s'.",
                           self.output.name, replaced)
            else:
                inputs = [self.output.name]
        else:
            inputs = list(self.inputs)
        if args.extend_run:
            inputs.insert(0, self.outputRun.name)
        inputs = CollectionSearch.fromExpression(inputs)
        return butler, inputs, self

    @classmethod
    def makeReadButler(cls, args: argparse.Namespace) -> Butler:
        """Construct a read-only butler according to the given command-line
        arguments.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler initialized with the collections specified by
            ``args``.
        """
        butler, inputs, _ = cls._makeReadParts(args)
        _LOG.debug("Preparing butler to read from %s.", inputs)
        return Butler(butler=butler, collections=inputs)

    @classmethod
    def makeRegistryAndCollections(cls, args: argparse.Namespace) -> \
            Tuple[Registry, CollectionSearch, Optional[str]]:
        """Return a read-only registry, a collection search path, and the name
        of the run to be used for future writes.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        registry : `lsst.daf.butler.Registry`
            Butler registry that collections will be added to and/or queried
            from.
        inputs : `lsst.daf.butler.registry.CollectionSearch`
            Collections to search for datasets.
        run : `str` or `None`
            Name of the output `~lsst.daf.butler.CollectionType.RUN` collection
            if it already exists, or `None` if it does not.
        """
        butler, inputs, self = cls._makeReadParts(args)
        run = self.outputRun.name if args.extend_run else None
        _LOG.debug("Preparing registry to read from %s and expect future writes to '%s'.", inputs, run)
        return butler.registry, inputs, run

    @classmethod
    def makeWriteButler(cls, args: argparse.Namespace) -> Butler:
        """Return a read-write butler initialized to write to and read from
        the collections specified by the given command-line arguments.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  See class documentation for the
            construction parameter of the same name.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-write butler initialized according to the given arguments.
        """
        butler = Butler(args.butler_config, writeable=True)
        self = cls(butler.registry, args, writeable=True)
        self.check(args)
        if self.output is not None:
            chainDefinition = list(self.output.chain if self.output.exists else self.inputs)
            if args.replace_run:
                replaced = chainDefinition.pop(0)
                if args.prune_replaced == "unstore":
                    # Remove datasets from datastore
                    with butler.transaction():
                        refs = butler.registry.queryDatasets(..., collections=replaced)
                        butler.pruneDatasets(refs, unstore=True, run=replaced, disassociate=False)
                elif args.prune_replaced == "purge":
                    # Erase entire collection and all datasets, need to remove
                    # collection from its chain collection first.
                    with butler.transaction():
                        butler.registry.setCollectionChain(self.output.name, chainDefinition)
                        butler.pruneCollection(replaced, purge=True, unstore=True)
                elif args.prune_replaced is not None:
                    raise NotImplementedError(
                        f"Unsupported --prune-replaced option '{args.prune_replaced}'."
                    )
            chainDefinition.insert(0, self.outputRun.name)
            chainDefinition = CollectionSearch.fromExpression(chainDefinition)
            _LOG.debug("Preparing butler to write to '%s' and read from '%s'=%s",
                       self.outputRun.name, self.output.name, chainDefinition)
            return Butler(butler=butler, run=self.outputRun.name, collections=self.output.name,
                          chains={self.output.name: chainDefinition})
        else:
            inputs = CollectionSearch.fromExpression((self.outputRun.name,) + self.inputs)
            _LOG.debug("Preparing butler to write to '%s' and read from %s.", self.outputRun.name, inputs)
            return Butler(butler=butler, run=self.outputRun.name, collections=inputs)

    output: Optional[_OutputChainedCollectionInfo]
    """Information about the output chained collection, if there is or will be
    one (`_OutputChainedCollectionInfo` or `None`).
    """

    outputRun: Optional[_OutputRunCollectionInfo]
    """Information about the output run collection, if there is or will be
    one (`_OutputRunCollectionInfo` or `None`).
    """

    inputs: Tuple[str, ...]
    """Input collections provided directly by the user (`tuple` [ `str` ]).
    """


class _FilteredStream:
    """A file-like object that filters some config fields.

    Note
    ----
    This class depends on implementation details of ``Config.saveToStream``
    methods, in particular that that method uses single call to write()
    method to save information about single config field, and that call
    combines comments string(s) for a field and field path and value.
    This class will not work reliably on the "import" strings, so imports
    should be disabled by passing ``skipImports=True`` to ``saveToStream()``.
    """
    def __init__(self, pattern):
        # obey case if pattern isn't lowercase or requests NOIGNORECASE
        mat = re.search(r"(.*):NOIGNORECASE$", pattern)

        if mat:
            pattern = mat.group(1)
            self._pattern = re.compile(fnmatch.translate(pattern))
        else:
            if pattern != pattern.lower():
                print(f"Matching \"{pattern}\" without regard to case "
                      "(append :NOIGNORECASE to prevent this)", file=sys.stdout)
            self._pattern = re.compile(fnmatch.translate(pattern), re.IGNORECASE)

    def write(self, showStr):
        # Strip off doc string line(s) and cut off at "=" for string matching
        matchStr = showStr.rstrip().split("\n")[-1].split("=")[0]
        if self._pattern.search(matchStr):
            sys.stdout.write(showStr)

# ------------------------
#  Exported definitions --
# ------------------------


class CmdLineFwk:
    """PipelineTask framework which executes tasks from command line.

    In addition to executing tasks this activator provides additional methods
    for task management like dumping configuration or execution chain.
    """

    MP_TIMEOUT = 9999  # Default timeout (sec) for multiprocessing

    def __init__(self):
        pass

    def makePipeline(self, args):
        """Build a pipeline from command line arguments.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command line

        Returns
        -------
        pipeline : `~lsst.pipe.base.Pipeline`
        """
        if args.pipeline:
            pipeline = Pipeline.fromFile(args.pipeline)
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
            pipeline.toFile(args.save_pipeline)

        if args.pipeline_dot:
            pipeline2dot(pipeline, args.pipeline_dot)

        return pipeline

    def makeGraph(self, pipeline, args):
        """Build a graph from command line arguments.

        Parameters
        ----------
        pipeline : `~lsst.pipe.base.Pipeline`
            Pipeline, can be empty or ``None`` if graph is read from a file.
        args : `argparse.Namespace`
            Parsed command line

        Returns
        -------
        graph : `~lsst.pipe.base.QuantumGraph` or `None`
            If resulting graph is empty then `None` is returned.
        """

        registry, collections, run = _ButlerFactory.makeRegistryAndCollections(args)

        if args.qgraph:
            # click passes empty tuple as default value for qgraph_node_id
            nodes = args.qgraph_node_id or None
            qgraph = QuantumGraph.loadUri(args.qgraph, registry.dimensions,
                                          nodes=nodes, graphID=args.qgraph_id)

            # pipeline can not be provided in this case
            if pipeline:
                raise ValueError("Pipeline must not be given when quantum graph is read from file.")

        else:

            # make execution plan (a.k.a. DAG) for pipeline
            graphBuilder = GraphBuilder(registry,
                                        skipExisting=args.skip_existing)
            qgraph = graphBuilder.makeGraph(pipeline, collections, run, args.data_query)

        # count quanta in graph and give a warning if it's empty and return None
        nQuanta = len(qgraph)
        if nQuanta == 0:
            warnings.warn("QuantumGraph is empty", stacklevel=2)
            return None
        else:
            _LOG.info("QuantumGraph contains %d quanta for %d tasks, graph ID: %r",
                      nQuanta, len(qgraph.taskGraph), qgraph.graphID)

        if args.save_qgraph:
            qgraph.saveUri(args.save_qgraph)

        if args.save_single_quanta:
            for quantumNode in qgraph:
                sqgraph = qgraph.subset(quantumNode)
                uri = args.save_single_quanta.format(quantumNode.nodeId.number)
                sqgraph.saveUri(uri)

        if args.qgraph_dot:
            graph2dot(qgraph, args.qgraph_dot)

        return qgraph

    def runPipeline(self, graph, taskFactory, args, butler=None):
        """Execute complete QuantumGraph.

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory
        args : `argparse.Namespace`
            Parsed command line
        butler : `~lsst.daf.butler.Butler`, optional
            Data Butler instance, if not defined then new instance is made
            using command line options.
        """
        # make butler instance
        if butler is None:
            butler = _ButlerFactory.makeWriteButler(args)

        # Enable lsstDebug debugging. Note that this is done once in the
        # main process before PreExecInit and it is also repeated before
        # running each task in SingleQuantumExecutor (which may not be
        # needed if `multipocessing` always uses fork start method).
        if args.enableLsstDebug:
            try:
                _LOG.debug("Will try to import debug.py")
                import debug  # noqa:F401
            except ImportError:
                _LOG.warn("No 'debug' module found.")

        preExecInit = PreExecInit(butler, taskFactory, args.skip_existing)
        preExecInit.initialize(graph,
                               saveInitOutputs=not args.skip_init_writes,
                               registerDatasetTypes=args.register_dataset_types,
                               saveVersions=not args.no_versions)

        if not args.init_only:
            graphFixup = self._importGraphFixup(args)
            quantumExecutor = SingleQuantumExecutor(taskFactory,
                                                    skipExisting=args.skip_existing,
                                                    clobberPartialOutputs=args.clobber_partial_outputs,
                                                    enableLsstDebug=args.enableLsstDebug)
            timeout = self.MP_TIMEOUT if args.timeout is None else args.timeout
            executor = MPGraphExecutor(numProc=args.processes, timeout=timeout,
                                       startMethod=args.start_method,
                                       quantumExecutor=quantumExecutor,
                                       failFast=args.fail_fast,
                                       executionGraphFixup=graphFixup)
            with util.profile(args.profile, _LOG):
                executor.execute(graph, butler)

    def showInfo(self, args, pipeline, graph=None):
        """Display useful info about pipeline and environment.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command line
        pipeline : `Pipeline`
            Pipeline definition
        graph : `QuantumGraph`, optional
            Execution graph
        """
        showOpts = args.show
        for what in showOpts:
            showCommand, _, showArgs = what.partition("=")

            if showCommand in ["pipeline", "config", "history", "tasks"]:
                if not pipeline:
                    _LOG.warning("Pipeline is required for --show=%s", showCommand)
                    continue

            if showCommand in ["graph", "workflow", "uri"]:
                if not graph:
                    _LOG.warning("QuantumGraph is required for --show=%s", showCommand)
                    continue

            if showCommand == "pipeline":
                print(pipeline)
            elif showCommand == "config":
                self._showConfig(pipeline, showArgs, False)
            elif showCommand == "dump-config":
                self._showConfig(pipeline, showArgs, True)
            elif showCommand == "history":
                self._showConfigHistory(pipeline, showArgs)
            elif showCommand == "tasks":
                self._showTaskHierarchy(pipeline)
            elif showCommand == "graph":
                if graph:
                    self._showGraph(graph)
            elif showCommand == "uri":
                if graph:
                    self._showUri(graph, args)
            elif showCommand == "workflow":
                if graph:
                    self._showWorkflow(graph, args)
            else:
                print("Unknown value for show: %s (choose from '%s')" %
                      (what, "', '".join("pipeline config[=XXX] history=XXX tasks graph".split())),
                      file=sys.stderr)
                sys.exit(1)

    def _showConfig(self, pipeline, showArgs, dumpFullConfig):
        """Show task configuration

        Parameters
        ----------
        pipeline : `Pipeline`
            Pipeline definition
        showArgs : `str`
            Defines what to show
        dumpFullConfig : `bool`
            If true then dump complete task configuration with all imports.
        """
        stream = sys.stdout
        if dumpFullConfig:
            # Task label can be given with this option
            taskName = showArgs
        else:
            # The argument can have form [TaskLabel::][pattern:NOIGNORECASE]
            matConfig = re.search(r"^(?:(\w+)::)?(?:config.)?(.+)?", showArgs)
            taskName = matConfig.group(1)
            pattern = matConfig.group(2)
            if pattern:
                stream = _FilteredStream(pattern)

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            print("Pipeline has no tasks named {}".format(taskName), file=sys.stderr)
            sys.exit(1)

        for taskDef in tasks:
            print("### Configuration for task `{}'".format(taskDef.label))
            taskDef.config.saveToStream(stream, root="config", skipImports=not dumpFullConfig)

    def _showConfigHistory(self, pipeline, showArgs):
        """Show history for task configuration

        Parameters
        ----------
        pipeline : `Pipeline`
            Pipeline definition
        showArgs : `str`
            Defines what to show
        """

        taskName = None
        pattern = None
        matHistory = re.search(r"^(?:(\w+)::)?(?:config[.])?(.+)", showArgs)
        if matHistory:
            taskName = matHistory.group(1)
            pattern = matHistory.group(2)
        if not pattern:
            print("Please provide a value with --show history (e.g. history=Task::param)", file=sys.stderr)
            sys.exit(1)

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            print(f"Pipeline has no tasks named {taskName}", file=sys.stderr)
            sys.exit(1)

        found = False
        for taskDef in tasks:

            config = taskDef.config

            # Look for any matches in the config hierarchy for this name
            for nmatch, thisName in enumerate(fnmatch.filter(config.names(), pattern)):
                if nmatch > 0:
                    print("")

                cpath, _, cname = thisName.rpartition(".")
                try:
                    if not cpath:
                        # looking for top-level field
                        hconfig = taskDef.config
                    else:
                        hconfig = eval("config." + cpath, {}, {"config": config})
                except AttributeError:
                    print(f"Error: Unable to extract attribute {cpath} from task {taskDef.label}",
                          file=sys.stderr)
                    hconfig = None

                # Sometimes we end up with a non-Config so skip those
                if isinstance(hconfig, (pexConfig.Config, pexConfig.ConfigurableInstance)) and \
                        hasattr(hconfig, cname):
                    print(f"### Configuration field for task `{taskDef.label}'")
                    print(pexConfig.history.format(hconfig, cname))
                    found = True

        if not found:
            print(f"None of the tasks has field matching {pattern}", file=sys.stderr)
            sys.exit(1)

    def _showTaskHierarchy(self, pipeline):
        """Print task hierarchy to stdout

        Parameters
        ----------
        pipeline: `Pipeline`
        """
        for taskDef in pipeline.toExpandedPipeline():
            print("### Subtasks for task `{}'".format(taskDef.taskName))

            for configName, taskName in util.subTaskIter(taskDef.config):
                print("{}: {}".format(configName, taskName))

    def _showGraph(self, graph):
        """Print quanta information to stdout

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        """
        for taskNode in graph.taskGraph:
            print(taskNode)

            for iq, quantum in enumerate(graph.getQuantaForTask(taskNode)):
                print("  Quantum {}:".format(iq))
                print("    inputs:")
                for key, refs in quantum.inputs.items():
                    dataIds = ["DataId({})".format(ref.dataId) for ref in refs]
                    print("      {}: [{}]".format(key, ", ".join(dataIds)))
                print("    outputs:")
                for key, refs in quantum.outputs.items():
                    dataIds = ["DataId({})".format(ref.dataId) for ref in refs]
                    print("      {}: [{}]".format(key, ", ".join(dataIds)))

    def _showWorkflow(self, graph, args):
        """Print quanta information and dependency to stdout

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        args : `argparse.Namespace`
            Parsed command line
        """
        for node in graph:
            print(f"Quantum {node.nodeId.number}: {node.taskDef.taskName}")
            for parent in graph.determineInputsToQuantumNode(node):
                print(f"Parent Quantum {parent.nodeId.number} - Child Quantum {node.nodeId.number}")

    def _showUri(self, graph, args):
        """Print input and predicted output URIs to stdout

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph
        args : `argparse.Namespace`
            Parsed command line
        """
        def dumpURIs(thisRef):
            primary, components = butler.getURIs(thisRef, predict=True, run="TBD")
            if primary:
                print(f"    {primary}")
            else:
                print("    (disassembled artifact)")
                for compName, compUri in components.items():
                    print(f"        {compName}: {compUri}")

        butler = _ButlerFactory.makeReadButler(args)
        for node in graph:
            print(f"Quantum {node.nodeId.number}: {node.taskDef.taskName}")
            print("  inputs:")
            for key, refs in node.quantum.inputs.items():
                for ref in refs:
                    dumpURIs(ref)
            print("  outputs:")
            for key, refs in node.quantum.outputs.items():
                for ref in refs:
                    dumpURIs(ref)

    def _importGraphFixup(self, args):
        """Import/instantiate graph fixup object.

        Parameters
        ----------
        args : `argparse.Namespace`
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
                factory = doImport(args.graph_fixup)
            except Exception as exc:
                raise ValueError("Failed to import graph fixup class/method") from exc
            try:
                fixup = factory()
            except Exception as exc:
                raise ValueError("Failed to make instance of graph fixup") from exc
            if not isinstance(fixup, ExecutionGraphFixup):
                raise ValueError("Graph fixup is not an instance of ExecutionGraphFixup class")
            return fixup
