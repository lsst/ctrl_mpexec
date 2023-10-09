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

__all__ = ["ShowInfo"]

import fnmatch
import re
import sys
from collections import defaultdict
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import lsst.pex.config as pexConfig
import lsst.pex.config.history as pexConfigHistory
from lsst.daf.butler import Butler, DatasetRef, DatasetType, NamedKeyMapping
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.pipe.base import Pipeline, QuantumGraph
from lsst.pipe.base.pipeline_graph import visualization

from . import util
from .cmdLineFwk import _ButlerFactory


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

    def __init__(self, pattern: str, stream: Any = None) -> None:
        if stream is None:
            stream = sys.stdout
        self.stream = stream
        # obey case if pattern isn't lowercase or requests NOIGNORECASE
        mat = re.search(r"(.*):NOIGNORECASE$", pattern)

        if mat:
            pattern = mat.group(1)
            self._pattern = re.compile(fnmatch.translate(pattern))
        else:
            if pattern != pattern.lower():
                print(
                    f'Matching "{pattern}" without regard to case (append :NOIGNORECASE to prevent this)',
                    file=self.stream,
                )
            self._pattern = re.compile(fnmatch.translate(pattern), re.IGNORECASE)

    def write(self, showStr: str) -> None:
        # Strip off doc string line(s) and cut off at "=" for string matching
        matchStr = showStr.rstrip().split("\n")[-1].split("=")[0]
        if self._pattern.search(matchStr):
            self.stream.write(showStr)


class ShowInfo:
    """Show information about a pipeline or quantum graph.

    Parameters
    ----------
    show : `list` [`str`]
        A list of show commands, some of which may have additional parameters
        specified using an ``=``.
    stream : I/O stream or None.
        The output stream to use. `None` will be treated as `sys.stdout`.

    Raises
    ------
    ValueError
        Raised if some show commands are not recognized.
    """

    pipeline_commands = {
        "pipeline",
        "config",
        "history",
        "tasks",
        "dump-config",
        "pipeline-graph",
        "task-graph",
    }
    graph_commands = {"graph", "workflow", "uri"}

    def __init__(self, show: list[str], stream: Any = None) -> None:
        if stream is None:
            # Defer assigning sys.stdout to allow click to redefine it if
            # it wants. Assigning the default at class definition leads
            # to confusion on reassignment.
            stream = sys.stdout
        commands: dict[str, list[str]] = defaultdict(list)
        for value in show:
            command, _, args = value.partition("=")
            commands[command].append(args)
        self.commands = commands
        self.stream = stream
        self.handled: set[str] = set()

        known = self.pipeline_commands | self.graph_commands
        unknown = set(commands) - known
        if unknown:
            raise ValueError(f"Unknown value(s) for show: {unknown} (choose from '{', '.join(known)}')")

    @property
    def unhandled(self) -> frozenset[str]:
        """Return the commands that have not yet been processed."""
        return frozenset(set(self.commands) - self.handled)

    def show_pipeline_info(self, pipeline: Pipeline, butler: Butler | None) -> None:
        """Display useful information about the pipeline.

        Parameters
        ----------
        pipeline : `lsst.pipe.base.Pipeline`
            The pipeline to use when reporting information.
        """
        if butler is not None:
            registry = butler.registry
        else:
            registry = None
        for command in self.pipeline_commands:
            if command not in self.commands:
                continue
            args = self.commands[command]

            match command:
                case "pipeline":
                    print(pipeline, file=self.stream)
                case "config":
                    for arg in args:
                        self._showConfig(pipeline, arg, False)
                case "dump-config":
                    for arg in args:
                        self._showConfig(pipeline, arg, True)
                case "history":
                    for arg in args:
                        self._showConfigHistory(pipeline, arg)
                case "tasks":
                    self._showTaskHierarchy(pipeline)
                case "pipeline-graph":
                    visualization.show(pipeline.to_graph(registry), self.stream, dataset_types=True)
                case "task-graph":
                    visualization.show(pipeline.to_graph(registry), self.stream, dataset_types=False)
                case _:
                    raise RuntimeError(f"Unexpectedly tried to process command {command!r}.")
            self.handled.add(command)

    def show_graph_info(self, graph: QuantumGraph, args: SimpleNamespace | None = None) -> None:
        """Show information associated with this graph.

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            Graph to use when reporting information.
        args : `types.SimpleNamespace`, optional
            Parsed command-line parameters. Used to obtain additional external
            information such as the location of a usable Butler.
        """
        for command in self.graph_commands:
            if command not in self.commands:
                continue
            match command:
                case "graph":
                    self._showGraph(graph)
                case "uri":
                    if args is None:
                        raise ValueError("The uri option requires additional command line arguments.")
                    self._showUri(graph, args)
                case "workflow":
                    self._showWorkflow(graph)
                case _:
                    raise RuntimeError(f"Unexpectedly tried to process command {command!r}.")
            self.handled.add(command)

    def _showConfig(self, pipeline: Pipeline, showArgs: str, dumpFullConfig: bool) -> None:
        """Show task configuration

        Parameters
        ----------
        pipeline : `lsst.pipe.base.Pipeline`
            Pipeline definition
        showArgs : `str`
            Defines what to show
        dumpFullConfig : `bool`
            If true then dump complete task configuration with all imports.
        """
        stream: Any = self.stream
        if dumpFullConfig:
            # Task label can be given with this option
            taskName = showArgs
        else:
            # The argument can have form [TaskLabel::][pattern:NOIGNORECASE]
            matConfig = re.search(r"^(?:(\w+)::)?(?:config.)?(.+)?", showArgs)
            assert matConfig is not None, "regex always matches"
            taskName = matConfig.group(1)
            pattern = matConfig.group(2)
            if pattern:
                stream = _FilteredStream(pattern, stream=stream)

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            raise ValueError(f"Pipeline has no tasks named {taskName}")

        for taskDef in tasks:
            print(f"### Configuration for task `{taskDef.label}'", file=self.stream)
            taskDef.config.saveToStream(stream, root="config", skipImports=not dumpFullConfig)

    def _showConfigHistory(self, pipeline: Pipeline, showArgs: str) -> None:
        """Show history for task configuration.

        Parameters
        ----------
        pipeline : `lsst.pipe.base.Pipeline`
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
            raise ValueError("Please provide a value with --show history (e.g. history=Task::param)")

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            raise ValueError(f"Pipeline has no tasks named {taskName}")

        found = False
        for taskDef in tasks:
            config = taskDef.config

            # Look for any matches in the config hierarchy for this name
            for nmatch, thisName in enumerate(fnmatch.filter(config.names(), pattern)):
                if nmatch > 0:
                    print("", file=self.stream)

                cpath, _, cname = thisName.rpartition(".")
                try:
                    if not cpath:
                        # looking for top-level field
                        hconfig = taskDef.config
                    else:
                        hconfig = eval("config." + cpath, {}, {"config": config})
                except AttributeError:
                    print(
                        f"Error: Unable to extract attribute {cpath} from task {taskDef.label}",
                        file=sys.stderr,
                    )
                    hconfig = None

                # Sometimes we end up with a non-Config so skip those
                if isinstance(hconfig, pexConfig.Config | pexConfig.ConfigurableInstance) and hasattr(
                    hconfig, cname
                ):
                    print(f"### Configuration field for task `{taskDef.label}'", file=self.stream)
                    print(pexConfigHistory.format(hconfig, cname), file=self.stream)
                    found = True

        if not found:
            raise ValueError(f"None of the tasks has field matching {pattern}")

    def _showTaskHierarchy(self, pipeline: Pipeline) -> None:
        """Print task hierarchy to stdout

        Parameters
        ----------
        pipeline: `lsst.pipe.base.Pipeline`
            Pipeline definition.
        """
        for taskDef in pipeline.toExpandedPipeline():
            print(f"### Subtasks for task `{taskDef.taskName}'", file=self.stream)

            for configName, taskName in util.subTaskIter(taskDef.config):
                print(f"{configName}: {taskName}", file=self.stream)

    def _showGraph(self, graph: QuantumGraph) -> None:
        """Print quanta information to stdout

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            Execution graph.
        """

        def _print_refs(
            mapping: NamedKeyMapping[DatasetType, tuple[DatasetRef, ...]],
            datastore_records: Mapping[str, DatastoreRecordData],
        ) -> None:
            """Print complete information on quantum input or output refs."""
            for key, refs in mapping.items():
                if refs:
                    print(f"      {key}:", file=self.stream)
                    for ref in refs:
                        print(f"        - {ref}", file=self.stream)
                        for datastore_name, record_data in datastore_records.items():
                            if record_map := record_data.records.get(ref.id):
                                print(f"          records for {datastore_name}:", file=self.stream)
                                for table_name, records in record_map.items():
                                    print(f"            - {table_name}:", file=self.stream)
                                    for record in records:
                                        print(f"              - {record}:", file=self.stream)
                else:
                    print(f"      {key}: []", file=self.stream)

        for taskNode in graph.iterTaskGraph():
            print(taskNode, file=self.stream)

            for iq, quantum_node in enumerate(graph.getNodesForTask(taskNode)):
                quantum = quantum_node.quantum
                print(
                    f"  Quantum {iq} dataId={quantum.dataId} nodeId={quantum_node.nodeId}:", file=self.stream
                )
                print("    inputs:", file=self.stream)
                _print_refs(quantum.inputs, quantum.datastore_records)
                print("    outputs:", file=self.stream)
                _print_refs(quantum.outputs, quantum.datastore_records)

    def _showWorkflow(self, graph: QuantumGraph) -> None:
        """Print quanta information and dependency to stdout

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            Execution graph.
        """
        for node in graph:
            print(f"Quantum {node.nodeId}: {node.taskDef.taskName}", file=self.stream)
            for parent in graph.determineInputsToQuantumNode(node):
                print(f"Parent Quantum {parent.nodeId} - Child Quantum {node.nodeId}", file=self.stream)

    def _showUri(self, graph: QuantumGraph, args: SimpleNamespace) -> None:
        """Print input and predicted output URIs to stdout

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            Execution graph
        args : `types.SimpleNamespace`
            Parsed command line
        """

        def dumpURIs(thisRef: DatasetRef) -> None:
            primary, components = butler.getURIs(thisRef, predict=True, run="TBD")
            if primary:
                print(f"    {primary}", file=self.stream)
            else:
                print("    (disassembled artifact)", file=self.stream)
                for compName, compUri in components.items():
                    print(f"        {compName}: {compUri}", file=self.stream)

        butler = _ButlerFactory.makeReadButler(args)
        for node in graph:
            print(f"Quantum {node.nodeId}: {node.taskDef.taskName}", file=self.stream)
            print("  inputs:", file=self.stream)
            for refs in node.quantum.inputs.values():
                for ref in refs:
                    dumpURIs(ref)
            print("  outputs:", file=self.stream)
            for refs in node.quantum.outputs.values():
                for ref in refs:
                    dumpURIs(ref)
