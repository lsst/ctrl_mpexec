#
# LSST Data Management System
# Copyright 2017-2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
"""
Module defining CmdLineFwk class and related methods.
"""

__all__ = ['CmdLineFwk']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import fnmatch
import logging
import multiprocessing
import pickle
import re
import sys

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.base import disableImplicitThreading
from lsst.daf.butler import Butler, DatasetOriginInfoDef
import lsst.log
import lsst.pex.config as pexConfig
from .graphBuilder import GraphBuilder
from .cmdLineParser import makeParser
from .pipelineBuilder import PipelineBuilder
from .dotTools import graph2dot, pipeline2dot
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


class _MPMap(object):
    """Class implementing "map" function using multiprocessing pool.

    Parameters
    ----------
    numProc : `int`
        Number of process to use for executing tasks.
    timeout : `float`
        Time in seconds to wait for tasks to finish.
    """

    def __init__(self, numProc, timeout):
        self.numProc = numProc
        self.timeout = timeout

    def __call__(self, function, iterable):
        """Apply function to every item of iterable.

        Wrapper around pool.map_async, to handle timeout. This is required
        so as to trigger an immediate interrupt on the KeyboardInterrupt
        (Ctrl-C); see
        http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool

        Further wraps the function in _poolFunctionWrapper to catch exceptions
        that don't inherit from Exception.
        """
        disableImplicitThreading()  # To prevent thread contention
        pool = multiprocessing.Pool(processes=self.numProc, maxtasksperchild=1)
        result = pool.map_async(function, iterable)
        return result.get(self.timeout)

# ------------------------
#  Exported definitions --
# ------------------------


class CmdLineFwk(object):
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

        self.taskLoader = TaskLoader(args.packages)
        self.taskFactory = TaskFactory(self.taskLoader)

        if args.subcommand == "list":
            # just dump some info about where things may be found
            return self.doList(args.show, args.show_headers)

        # make pipeline out of command line arguments
        try:
            pipeBuilder = PipelineBuilder(self.taskFactory)
            pipeline = pipeBuilder.makePipeline(args)
        except Exception as exc:
            print("Failed to build pipeline: {}".format(exc), file=sys.stderr)
            raise

        if args.save_pipeline:
            with open(args.save_pipeline, "wb") as pickleFile:
                pickle.dump(pipeline, pickleFile)

        if args.pipeline_dot:
            pipeline2dot(pipeline, args.pipeline_dot, self.taskFactory)

        if args.subcommand == "build":
            # stop here but process --show option first
            self.showInfo(args.show, pipeline, None)
            return 0

        if args.qgraph:
            with open(args.qgraph, 'rb') as pickleFile:
                qgraph = pickle.load(pickleFile)
            # TODO: pipeline is ignored in this case, make sure that user
            # does not specify any pipeline-related options
        else:

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
            graphBuilder = GraphBuilder(self.taskFactory, butler.registry, args.skip_existing)
            qgraph = graphBuilder.makeGraph(pipeline, coll, args.data_query)

        if args.save_qgraph:
            with open(args.save_qgraph, "wb") as pickleFile:
                pickle.dump(qgraph, pickleFile)

        if args.qgraph_dot:
            graph2dot(qgraph, args.qgraph_dot)

        # optionally dump some info
        self.showInfo(args.show, pipeline, qgraph)

        if args.subcommand == "qgraph":
            # stop here
            return 0

        # execute
        if args.subcommand == "run":

            # If output collections are given then use them to override
            # butler-configured ones.
            run = args.output.get("", None)

            # make butler instance
            butler = Butler(config=args.butler_config, run=run)

            # at this point we require that output collection was defined
            if not butler.run:
                raise ValueError("no output collection defined in data butler")

            return self.runPipeline(qgraph, butler, args)

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
            message_fmt = "%-5p %d{yyyy-MM-ddThh:mm:ss.sss} %c (%X{LABEL})(%F:%L)- %m%n"
        else:
            message_fmt = "%c %p: %m%n"

        # global logging config
        lsst.log.configure_prop(_LOG_PROP.format(message_fmt))

        # configure individual loggers
        for component, level in logLevels:
            level = getattr(lsst.log.Log, level.upper(), None)
            if level is not None:
                logger = lsst.log.Log.getLogger(component or "")
                logger.setLevel(level)

        # Forward all Python logging to lsst.log
        lgr = logging.getLogger()
        lgr.setLevel(logging.DEBUG)
        lgr.addHandler(lsst.log.LogHandler())

    def doList(self, show, show_headers):
        """Implementation of the "list" command.

        Parameters
        ----------
        show : `list` of `str`
            List of items to show.
        show_headers : `bool`
            True to display additional headers
        """

        if not show:
            show = ["super-tasks"]

        if "packages" in show:
            if show_headers:
                print()
                print("Modules search path")
                print("-------------------")
            for pkg in sorted(self.taskLoader.packages):
                print(pkg)

        if "modules" in show:
            try:
                modules = self.taskLoader.modules()
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

        if "tasks" in show or "super-tasks" in show:
            try:
                tasks = self.taskLoader.tasks()
            except ImportError as exc:
                print("Failed to import package, check --packages option or PYTHONPATH:", exc,
                      file=sys.stderr)
                return 2

            if "tasks" not in show:
                # only show super-tasks
                tasks = [(name, kind) for name, kind in tasks if kind == KIND_PIPELINETASK]
            tasks.sort()

            headers = None
            if show_headers:
                print()
                headers = ("Task class name", "Kind     ")
            util.printTable(tasks, headers)

    def runPipeline(self, graph, butler, args):
        """
        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        butler : `Butler`
            data butler instance
        args : `argparse.Namespace`
            Parsed command line
        """

        # how many processes do we want
        numProc = args.processes

        # associate all existing datasets with output collection.
        self._updateOutputCollection(graph, butler)

        # Save task initialization data.
        # TODO: see if Pipeline and software versions are already written
        # to butler and associated with Run, check for consistency if they
        # are, and if so skip writing TaskInitOutputs (because those should
        # also only be done once).  If not, write them.
        for taskNodes in graph:
            taskDef, quanta = taskNodes.taskDef, taskNodes.quanta
            task = self.taskFactory.makeTask(taskDef.taskClass, taskDef.config, None, butler)
            self.writeTaskInitOutputs(task, butler)

            if numProc > 1 and not taskDef.taskClass.canMultiprocess:
                _LOG.warn("Task %s does not support multiprocessing; using one process",
                          taskDef.taskName)
                numProc = 1

        # chose map function being simple sequential map or multi-process map
        if numProc > 1:
            timeout = getattr(args, 'timeout', None)
            if timeout is None or timeout <= 0:
                timeout = self.MP_TIMEOUT
            mapFunc = _MPMap(numProc, timeout)
        else:

            def _mapFunc(func, iterable):
                """Call function for all items sequentially"""
                return [func(item) for item in iterable]

            mapFunc = _mapFunc

        # tasks are executed sequentially but quanta can run in parallel
        for taskNodes in graph:
            taskDef, quanta = taskNodes.taskDef, taskNodes.quanta
            # targets for map function
            target_list = [(taskDef.taskClass, taskDef.config, quantum, butler)
                           for quantum in quanta]
            # call task on each argument in a list
            profile_name = getattr(args, "profile", None)
            with util.profile(profile_name, _LOG):
                mapFunc(self._executePipelineTask, target_list)

    def _updateOutputCollection(self, graph, butler):
        """Associate all existing datasets with output collection.

        For every Quantum in a graph make sure that its existing inputs are
        added to the Butler's output collection.

        For each quantum there are input and output DataRefs. With the
        current implementation of preflight output refs should not exist but
        input refs may belong to a different collection. We want all refs to
        appear in output collection, so we have to "copy" those refs.

        Parameters
        ----------
        graph : `QuantumGraph`
            Execution graph.
        butler : `Butler`
            data butler instance
        """
        def _refComponents(refs):
            """Return all dataset components recursively"""
            for ref in refs:
                yield ref
                yield from _refComponents(ref.components.values())

        # Main issue here is that the same DataRef can appear as input for
        # many quanta, to keep them unique we first collect them into one
        # dict indexed by dataset id.
        id2ref = {}
        for taskDef, quantum in graph.quanta():
            for refs in quantum.predictedInputs.values():
                for ref in _refComponents(refs):
                    id2ref[ref.id] = ref
        if id2ref:
            # copy all collected refs to output collection
            collection = butler.run.collection
            registry = butler.registry
            registry.associate(collection, id2ref.values())

    def _executePipelineTask(self, target):
        """Execute super-task on a single data item.

        Parameters
        ----------
        target: `tuple` of `(taskClass, config, quantum, butler)`
        """
        taskClass, config, quantum, butler = target

        # setup logging, include dataId into MDC
#         if dataRef is not None:
#             if hasattr(dataRef, "dataId"):
#                 lsst.log.MDC("LABEL", str(dataRef.dataId))
#             elif isinstance(dataRef, (list, tuple)):
#                 lsst.log.MDC("LABEL", str([ref.dataId for ref in dataRef if hasattr(ref, "dataId")]))

        # make task instance
        task = self.taskFactory.makeTask(taskClass, config, None, butler)

        # Call task runQuantum() method. Any exception thrown here propagates
        # to multiprocessing module and to parent process.
        result = task.runQuantum(quantum, butler)

        # save provenenace for current quantum
        quantum._task = taskClass.__name__
        quantum._run = butler.run
        butler.registry.addQuantum(quantum)

        return result

    def writeTaskInitOutputs(self, task, butler):
        """Write any datasets produced by initializing the given PipelineTask.

        Parameters
        ----------
        task : `PipelineTask`
            instance of PipelineTask
        butler : `lsst.daf.butler.Butler`
            data butler instance
        """
        initOutputs = task.getInitOutputDatasets()
        initOutputDatasetTypes = task.getInitOutputDatasetTypes(task.config)
        for key, obj in initOutputs.items():
            butler.put(obj, initOutputDatasetTypes[key], {})

    def showInfo(self, showOpts, pipeline, graph):
        """Display useful info about pipeline and environment.

        Parameters
        ----------
        showOpts : `list` of `str`
            Defines what to show
        pipeline : `Pipeline`
            Pipeline definition
        graph : `QuantumGraph`
            Execution graph.
        """

        for what in showOpts:
            showCommand, _, showArgs = what.partition("=")

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
            class FilteredStream(object):
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

        matHistory = re.search(r"^(?:(\w+)::)(?:config.)?(.+)?", showArgs)
        taskName = matHistory.group(1)
        pattern = matHistory.group(2)
        if not pattern:
            print("Please provide a value with --show history (e.g. history=XXX)", file=sys.stderr)
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
