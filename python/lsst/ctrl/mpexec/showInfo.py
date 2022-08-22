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

__all__ = ["ShowInfo"]

import fnmatch
import re
import sys
from types import SimpleNamespace
from typing import Any, Optional

import lsst.pex.config as pexConfig
import lsst.pex.config.history as pexConfigHistory
from lsst.daf.butler import DatasetRef
from lsst.pipe.base import Pipeline, QuantumGraph

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

    def __init__(self, pattern: str) -> None:
        # obey case if pattern isn't lowercase or requests NOIGNORECASE
        mat = re.search(r"(.*):NOIGNORECASE$", pattern)

        if mat:
            pattern = mat.group(1)
            self._pattern = re.compile(fnmatch.translate(pattern))
        else:
            if pattern != pattern.lower():
                print(
                    f'Matching "{pattern}" without regard to case ' "(append :NOIGNORECASE to prevent this)",
                    file=sys.stdout,
                )
            self._pattern = re.compile(fnmatch.translate(pattern), re.IGNORECASE)

    def write(self, showStr: str) -> None:
        # Strip off doc string line(s) and cut off at "=" for string matching
        matchStr = showStr.rstrip().split("\n")[-1].split("=")[0]
        if self._pattern.search(matchStr):
            sys.stdout.write(showStr)


class ShowInfo:
    """Show information about a pipeline or quantum graph.

    Parameters
    ----------
    show : `list` [`str`]
        A list of show commands, some of which may have additional parameters
        specified using an ``=``.

    Raises
    ------
    ValueError
        Raised if some show commands are not recognized.
    """

    pipeline_commands = {"pipeline", "config", "history", "tasks", "dump-config"}
    graph_commands = {"graph", "workflow", "uri"}

    def __init__(self, show: list[str]) -> None:
        commands: dict[str, list[str]] = {}
        for value in show:
            command, _, args = value.partition("=")
            if command in commands:
                commands[command].append(args)
            else:
                commands[command] = [args]
        self.commands = commands
        self.handled: set[str] = set()

        known = self.pipeline_commands | self.graph_commands
        unknown = set(commands) - known
        if unknown:
            raise ValueError(f"Unknown value(s) for show: {unknown} (choose from '{', '.join(known)}')")

    @property
    def unhandled(self) -> frozenset[str]:
        """Return the commands that have not yet been processed."""
        return frozenset(set(self.commands) - self.handled)

    def show_pipeline_info(self, pipeline: Pipeline) -> None:
        """Display useful information about the pipeline.

        Parameters
        ----------
        pipeline : `lsst.pipe.base.Pipeline`
            The pipeline to use when reporting information.
        """
        for command in self.pipeline_commands:
            if command not in self.commands:
                continue
            args = self.commands[command]

            if command == "pipeline":
                print(pipeline)
            elif command == "config":
                for arg in args:
                    self._showConfig(pipeline, arg, False)
            elif command == "dump-config":
                for arg in args:
                    self._showConfig(pipeline, arg, True)
            elif command == "history":
                for arg in args:
                    self._showConfigHistory(pipeline, arg)
            elif command == "tasks":
                self._showTaskHierarchy(pipeline)
            else:
                raise RuntimeError(f"Unexpectedly tried to process command {command!r}.")
            self.handled.add(command)

    def show_graph_info(self, graph: QuantumGraph, args: Optional[SimpleNamespace] = None) -> None:
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
            if command == "graph":
                self._showGraph(graph)
            elif command == "uri":
                if args is None:
                    raise ValueError("The uri option requires additional command line arguments.")
                self._showUri(graph, args)
            elif command == "workflow":
                self._showWorkflow(graph)
            else:
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
        stream: Any = sys.stdout
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
                stream = _FilteredStream(pattern)

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            print("Pipeline has no tasks named {}".format(taskName), file=sys.stderr)
            sys.exit(1)

        for taskDef in tasks:
            print("### Configuration for task `{}'".format(taskDef.label))
            taskDef.config.saveToStream(stream, root="config", skipImports=not dumpFullConfig)

    def _showConfigHistory(self, pipeline: Pipeline, showArgs: str) -> None:
        """Show history for task configuration

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
                    print(
                        f"Error: Unable to extract attribute {cpath} from task {taskDef.label}",
                        file=sys.stderr,
                    )
                    hconfig = None

                # Sometimes we end up with a non-Config so skip those
                if isinstance(hconfig, (pexConfig.Config, pexConfig.ConfigurableInstance)) and hasattr(
                    hconfig, cname
                ):
                    print(f"### Configuration field for task `{taskDef.label}'")
                    print(pexConfigHistory.format(hconfig, cname))
                    found = True

        if not found:
            print(f"None of the tasks has field matching {pattern}", file=sys.stderr)
            sys.exit(1)

    def _showTaskHierarchy(self, pipeline: Pipeline) -> None:
        """Print task hierarchy to stdout

        Parameters
        ----------
        pipeline: `lsst.pipe.base.Pipeline`
            Pipeline definition.
        """
        for taskDef in pipeline.toExpandedPipeline():
            print("### Subtasks for task `{}'".format(taskDef.taskName))

            for configName, taskName in util.subTaskIter(taskDef.config):
                print("{}: {}".format(configName, taskName))

    def _showGraph(self, graph: QuantumGraph) -> None:
        """Print quanta information to stdout

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
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

    def _showWorkflow(self, graph: QuantumGraph) -> None:
        """Print quanta information and dependency to stdout

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            Execution graph.
        """
        for node in graph:
            print(f"Quantum {node.nodeId}: {node.taskDef.taskName}")
            for parent in graph.determineInputsToQuantumNode(node):
                print(f"Parent Quantum {parent.nodeId} - Child Quantum {node.nodeId}")

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
                print(f"    {primary}")
            else:
                print("    (disassembled artifact)")
                for compName, compUri in components.items():
                    print(f"        {compName}: {compUri}")

        butler = _ButlerFactory.makeReadButler(args)
        for node in graph:
            print(f"Quantum {node.nodeId}: {node.taskDef.taskName}")
            print("  inputs:")
            for key, refs in node.quantum.inputs.items():
                for ref in refs:
                    dumpURIs(ref)
            print("  outputs:")
            for key, refs in node.quantum.outputs.items():
                for ref in refs:
                    dumpURIs(ref)
