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
import fnmatch
import logging
import pickle
import re
import sys
import warnings

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import Butler, DatasetOriginInfoDef
import lsst.log
import lsst.pex.config as pexConfig
from lsst.pipe.base import GraphBuilder, PipelineBuilder, Pipeline, QuantumGraph
from .cmdLineParser import makeParser
from .dotTools import graph2dot, pipeline2dot
from .mpGraphExecutor import MPGraphExecutor
from .preExecInit import PreExecInit
from .taskFactory import TaskFactory
from .taskLoader import (TaskLoader, KIND_PIPELINETASK)
from . import util

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""

_LOG = logging.getLogger(__name__.partition(".")[2])

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

    def parseAndRun(self, argv=None):
        """
        This method is a main entry point for this class, it parses command
        line and executes all commands.

        Parameters
        ----------
        argv : `list` of `str`, optional
            list of command line arguments, if not specified then
            `sys.argv[1:]` is used
        """

        if argv is None:
            argv = sys.argv[1:]

        # start with parsing command line, only do partial parsing now as
        # the tasks can add more arguments later
        parser = makeParser()
        args = parser.parse_args(argv)

        # First thing to do is to setup logging.
        self.configLog(args.longlog, args.loglevel)

        taskLoader = TaskLoader(args.packages)
        taskFactory = TaskFactory(taskLoader)

        if args.subcommand == "list":
            # just dump some info about where things may be found
            return self.doList(taskLoader, args.show, args.show_headers)

        # make pipeline out of command line arguments (can return empty pipeline)
        try:
            pipeline = self.makePipeline(taskFactory, args)
        except Exception as exc:
            print("Failed to build pipeline: {}".format(exc), file=sys.stderr)
            raise

        if args.subcommand == "build":
            # stop here but process --show option first
            self.showInfo(args.show, pipeline)
            return 0

        # make quantum graph
        try:
            qgraph = self.makeGraph(pipeline, taskFactory, args)
        except Exception as exc:
            print("Failed to build graph: {}".format(exc), file=sys.stderr)
            raise

        # optionally dump some info
        self.showInfo(args.show, pipeline, qgraph)

        if qgraph is None:
            # No need to raise an exception here, code that makes graph
            # should have printed warning message already.
            return 2

        if args.subcommand == "qgraph":
            # stop here
            return 0

        # execute
        if args.subcommand == "run":
            return self.runPipeline(qgraph, taskFactory, args)

    @staticmethod
    def configLog(longlog, logLevels):
        """Configure logging system.

        Parameters
        ----------
        longlog : `bool`
            If True then make log messages appear in "long format"
        logLevels : `list` of `tuple`
            per-component logging levels, each item in the list is a tuple
            (component, level), `component` is a logger name or `None` for root
            logger, `level` is a logging level name ('DEBUG', 'INFO', etc.)
        """
        if longlog:
            message_fmt = "%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c (%X{LABEL})(%F:%L)- %m%n"
        else:
            message_fmt = "%c %p: %m%n"

        # global logging config
        lsst.log.configure_prop(_LOG_PROP.format(message_fmt))

        # Forward all Python logging to lsst.log
        lgr = logging.getLogger()
        lgr.setLevel(logging.INFO)  # same as in log4cxx config above
        lgr.addHandler(lsst.log.LogHandler())

        # also capture warnings and send them to logging
        logging.captureWarnings(True)

        # configure individual loggers
        for component, level in logLevels:
            level = getattr(lsst.log.Log, level.upper(), None)
            if level is not None:
                # set logging level for lsst.log
                logger = lsst.log.Log.getLogger(component or "")
                logger.setLevel(level)
                # set logging level for Python logging
                pyLevel = lsst.log.LevelTranslator.lsstLog2logging(level)
                logging.getLogger(component).setLevel(pyLevel)

    def doList(self, taskLoader, show, show_headers):
        """Implementation of the "list" command.

        Parameters
        ----------
        taskLoader : `TaskLoader`
        show : `list` of `str`
            List of items to show.
        show_headers : `bool`
            True to display additional headers
        """

        if not show:
            show = ["pipeline-tasks"]

        if "packages" in show:
            if show_headers:
                print()
                print("Modules search path")
                print("-------------------")
            for pkg in sorted(taskLoader.packages):
                print(pkg)

        if "modules" in show:
            try:
                modules = taskLoader.modules()
            except ImportError as exc:
                print("Failed to import package, check --package option or $PYTHONPATH:", exc,
                      file=sys.stderr)
                return 2
            modules = [(name, "package" if flag else "module") for name, flag in sorted(modules)]
            headers = None
            if show_headers:
                print()
                headers = ("Module or package name", "Type    ")
            util.printTable(modules, headers)

        if "tasks" in show or "pipeline-tasks" in show:
            try:
                tasks = taskLoader.tasks()
            except ImportError as exc:
                print("Failed to import package, check --packages option or PYTHONPATH:", exc,
                      file=sys.stderr)
                return 2

            if "tasks" not in show:
                # only show pipeline-tasks
                tasks = [(name, kind) for name, kind in tasks if kind == KIND_PIPELINETASK]
            tasks.sort()

            headers = None
            if show_headers:
                print()
                headers = ("Task class name", "Kind     ")
            util.printTable(tasks, headers)

    def makePipeline(self, taskFactory, args):
        """Build a pipeline from command line arguments.

        Parameters
        ----------
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        args : `argparse.Namespace`
            Parsed command line

        Returns
        -------
        pipeline : `~lsst.pipe.base.Pipeline`
        """
        # read existing pipeline from pickle file
        pipeline = None
        if args.pipeline:
            with open(args.pipeline, 'rb') as pickleFile:
                pipeline = pickle.load(pickleFile)
                if not isinstance(pipeline, Pipeline):
                    raise TypeError("Pipeline pickle file has incorrect object type: {}".format(
                        type(pipeline)))

        pipeBuilder = PipelineBuilder(taskFactory, pipeline)

        # loop over all pipeline actions and apply them in order
        for action in args.pipeline_actions:

            if action.action == "new_task":

                pipeBuilder.addTask(action.value, action.label)

            elif action.action == "delete_task":

                pipeBuilder.deleteTask(action.label)

            elif action.action == "move_task":

                pipeBuilder.moveTask(action.label, action.value)

            elif action.action == "relabel":

                pipeBuilder.labelTask(action.label, action.value)

            elif action.action == "config":

                pipeBuilder.configOverride(action.label, action.value)

            elif action.action == "configfile":

                pipeBuilder.configOverrideFile(action.label, action.value)

            else:

                raise ValueError(f"Unexpected pipeline action: {action.action}")

        pipeline = pipeBuilder.pipeline(args.order_pipeline)

        if args.save_pipeline:
            with open(args.save_pipeline, "wb") as pickleFile:
                pickle.dump(pipeline, pickleFile)

        if args.pipeline_dot:
            pipeline2dot(pipeline, args.pipeline_dot, taskFactory)

        return pipeline

    def makeGraph(self, pipeline, taskFactory, args):
        """Build a graph from command line arguments.

        Parameters
        ----------
        pipeline : `~lsst.pipe.base.Pipeline`
            Pipeline, can be empty or ``None`` if graph is read from pickle
            file.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        args : `argparse.Namespace`
            Parsed command line

        Returns
        -------
        graph : `~lsst.pipe.base.QuantumGraph` or `None`
            If resulting graph is empty then `None` is returned.
        """
        if args.qgraph:

            with open(args.qgraph, 'rb') as pickleFile:
                qgraph = pickle.load(pickleFile)
                if not isinstance(qgraph, QuantumGraph):
                    raise TypeError("QuantumGraph pickle file has incorrect object type: {}".format(
                        type(qgraph)))

            # pipeline can not be provided in this case
            if pipeline:
                raise ValueError("Pipeline must not be given when quantum graph is read from file.")

        else:

            if not pipeline:
                raise ValueError("Pipeline must be given for quantum graph construction.")

            # build collection names
            inputs = args.input.copy()
            defaultInputs = inputs.pop("", None)
            outputs = args.output.copy()
            defaultOutputs = outputs.pop("", None)

            # Make butler instance. From this Butler we only need Registry
            # instance. Input/output collections are handled by pre-flight
            # and we don't want to be constrained here by Butler's restrictions
            # on collection names.
            collection = defaultInputs[0] if defaultInputs else None
            butler = Butler(config=args.butler_config, collection=collection)

            # if default input collections are not given on command line then
            # use one from Butler (has to be configured in butler config)
            if not defaultInputs:
                defaultInputs = [butler.collection]
            coll = DatasetOriginInfoDef(defaultInputs=defaultInputs,
                                        defaultOutput=defaultOutputs,
                                        inputOverrides=inputs,
                                        outputOverrides=outputs)

            # make execution plan (a.k.a. DAG) for pipeline
            graphBuilder = GraphBuilder(taskFactory, butler.registry, args.skip_existing)
            qgraph = graphBuilder.makeGraph(pipeline, coll, args.data_query)

        # count quanta in graph and give a warning if it's empty and return None
        nQuanta = qgraph.countQuanta()
        if nQuanta == 0:
            warnings.warn("QuantumGraph is empty", stacklevel=2)
            return None
        else:
            _LOG.info("QuantumGraph contains %d quanta for %d tasks",
                      nQuanta, len(qgraph))

        if args.save_qgraph:
            with open(args.save_qgraph, "wb") as pickleFile:
                pickle.dump(qgraph, pickleFile)

        if args.qgraph_dot:
            graph2dot(qgraph, args.qgraph_dot)

        return qgraph

    def runPipeline(self, graph, taskFactory, args):
        """Execute complete QuantumGraph.

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        args : `argparse.Namespace`
            Parsed command line
        """
        # If default output collection is given then use it to override
        # butler-configured one.
        run = args.output.get("", None)

        # make butler instance
        butler = Butler(config=args.butler_config, run=run)

        # at this point we require that output collection was defined
        if not butler.run:
            raise ValueError("no output collection defined in data butler")

        preExecInit = PreExecInit(butler)
        preExecInit.initialize(graph, taskFactory,
                               registerDatasetTypes=args.register_dataset_types,
                               saveInitOutputs=not args.skip_init_writes,
                               updateOutputCollection=True)

        if not args.init_only:
            executor = MPGraphExecutor(numProc=args.processes, timeout=self.MP_TIMEOUT)
            with util.profile(args.profile, _LOG):
                executor.execute(graph, butler, taskFactory)

    def showInfo(self, showOpts, pipeline, graph=None):
        """Display useful info about pipeline and environment.

        Parameters
        ----------
        showOpts : `list` of `str`
            Defines what to show
        pipeline : `Pipeline`
            Pipeline definition
        graph : `QuantumGraph`, optional
            Execution graph
        """

        for what in showOpts:
            showCommand, _, showArgs = what.partition("=")

            if showCommand in ["pipeline", "config", "history", "tasks"]:
                if not pipeline:
                    _LOG.warning("Pipeline is required for --show=%s", showCommand)
                    continue

            if showCommand == "pipeline":
                for taskDef in pipeline:
                    print(taskDef)
            elif showCommand == "config":
                self._showConfig(pipeline, showArgs)
            elif showCommand == "history":
                self._showConfigHistory(pipeline, showArgs)
            elif showCommand == "tasks":
                self._showTaskHierarchy(pipeline)
            elif showCommand == "graph":
                if graph:
                    self._showGraph(graph)
            else:
                print("Unknown value for show: %s (choose from '%s')" %
                      (what, "', '".join("pipeline config[=XXX] history=XXX tasks graph".split())),
                      file=sys.stderr)
                sys.exit(1)

    def _showConfig(self, pipeline, showArgs):
        """Show task configuration

        Parameters
        ----------
        pipeline : `Pipeline`
            Pipeline definition
        showArgs : `str`
            Defines what to show
        """
        matConfig = re.search(r"^(?:(\w+)::)?(?:config.)?(.+)?", showArgs)
        taskName = matConfig.group(1)
        pattern = matConfig.group(2)
        if pattern:
            class FilteredStream:
                """A file object that only prints lines that match the glob "pattern"

                N.b. Newlines are silently discarded and reinserted;  crude but effective.
                """

                def __init__(self, pattern):
                    # obey case if pattern isn't lowecase or requests NOIGNORECASE
                    mat = re.search(r"(.*):NOIGNORECASE$", pattern)

                    if mat:
                        pattern = mat.group(1)
                        self._pattern = re.compile(fnmatch.translate(pattern))
                    else:
                        if pattern != pattern.lower():
                            print(u"Matching \"%s\" without regard to case "
                                  "(append :NOIGNORECASE to prevent this)" % (pattern,), file=sys.stdout)
                        self._pattern = re.compile(fnmatch.translate(pattern), re.IGNORECASE)

                def write(self, showStr):
                    showStr = showStr.rstrip()
                    # Strip off doc string line(s) and cut off at "=" for string matching
                    matchStr = showStr.split("\n")[-1].split("=")[0]
                    if self._pattern.search(matchStr):
                        print(u"\n" + showStr)

            fd = FilteredStream(pattern)
        else:
            fd = sys.stdout

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            print("Pipeline has not tasks named {}".format(taskName), file=sys.stderr)
            sys.exit(1)

        for taskDef in tasks:
            print("### Configuration for task `{}'".format(taskDef.taskName))
            taskDef.config.saveToStream(fd, "config")

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
        matHistory = re.search(r"^(?:(\w+)::)(?:config[.])?(.+)", showArgs)
        if matHistory:
            taskName = matHistory.group(1)
            pattern = matHistory.group(2)
        print(showArgs, taskName, pattern)
        if not pattern:
            print("Please provide a value with --show history (e.g. history=Task::param)", file=sys.stderr)
            sys.exit(1)

        tasks = util.filterTasks(pipeline, taskName)
        if not tasks:
            print("Pipeline has not tasks named {}".format(taskName), file=sys.stderr)
            sys.exit(1)

        pattern = pattern.split(".")
        cpath, cname = pattern[:-1], pattern[-1]
        found = False
        for taskDef in tasks:
            hconfig = taskDef.config
            for i, cpt in enumerate(cpath):
                hconfig = getattr(hconfig, cpt, None)
                if hconfig is None:
                    break

            if hconfig is not None and hasattr(hconfig, cname):
                print("### Configuration field for task `{}'".format(taskDef.taskName))
                print(pexConfig.history.format(hconfig, cname))
                found = True

        if not found:
            print("None of the tasks has field named {}".format(showArgs), file=sys.stderr)
            sys.exit(1)

    def _showTaskHierarchy(self, pipeline):
        """Print task hierarchy to stdout

        Parameters
        ----------
        pipeline: `Pipeline`
        """
        for taskDef in pipeline:
            print("### Subtasks for task `{}'".format(taskDef.taskName))

            for configName, taskName in util.subTaskIter(taskDef.config):
                print("{}: {}".format(configName, taskName))

    def _showGraph(self, graph):
        """Print task hierarchy to stdout

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        """
        for taskNodes in graph:
            print(taskNodes.taskDef)

            for iq, quantum in enumerate(taskNodes.quanta):
                print("  Quantum {}:".format(iq))
                print("    inputs:")
                for key, refs in quantum.predictedInputs.items():
                    dataIds = ["DataId({})".format(ref.dataId) for ref in refs]
                    print("      {}: [{}]".format(key, ", ".join(dataIds)))
                print("    outputs:")
                for key, refs in quantum.outputs.items():
                    dataIds = ["DataId({})".format(ref.dataId) for ref in refs]
                    print("      {}: [{}]".format(key, ", ".join(dataIds)))
