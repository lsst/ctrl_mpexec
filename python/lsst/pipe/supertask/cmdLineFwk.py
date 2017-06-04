#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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

from __future__ import print_function
from builtins import object

__all__ = ['CmdLineFwk']

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import contextlib
import multiprocessing
import os
import pickle
import sys
import traceback

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.base import disableImplicitThreading
import lsst.daf.persistence as dafPersist
import lsst.log as lsstLog
import lsst.obs.base.repodb.tests as repodbTest
from lsst.pipe.base.task import TaskError
import lsst.utils
from .configOverrides import ConfigOverrides
from .graphBuilder import GraphBuilder
from .parser import makeParser, DEFAULT_INPUT_NAME, DEFAULT_CALIB_NAME, DEFAULT_OUTPUT_NAME
from .pipeline import Pipeline, TaskDef
from .taskFactory import TaskFactory
from .taskLoader import (TaskLoader, KIND_SUPERTASK)
from . import util

#----------------------------------
# Local non-exported definitions --
#----------------------------------

# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""


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


class BFactory(object):
    """Implement ButlerFactory using command line arguments.

    Parameters
    ----------
    namespace : argparse.Namespace
        Parsed command line arguments
    """
    def __init__(self, namespace):
        self._butler = None

        if namespace.output:
            outputs = {'root': namespace.output, 'mode': 'rw'}
            inputs = {'root': namespace.input}
            if namespace.calib:
                inputs['mapperArgs'] = {'calibRoot': namespace.calib}
            self._butler = dafPersist.Butler(inputs=inputs, outputs=outputs)
        else:
            outputs = {'root': namespace.input, 'mode': 'rw'}
            if namespace.calib:
                outputs['mapperArgs'] = {'calibRoot': namespace.calib}
            self._butler = dafPersist.Butler(outputs=outputs)

    def getButler(self):
        return self._butler


#------------------------
# Exported definitions --
#------------------------


class CmdLineFwk(object):
    """
    CmdLineActivator implements an activator for SuperTasks which executes
    tasks from command line.

    In addition to executing taks this activator provides additional methods
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

        # update all locations
        self._parseDirectories(args)

        # make butler instance
        bfactory = BFactory(args)
        butler = bfactory.getButler()

        # make pipeline out of command line arguments
        pipeline = self.makePipeline(args)
        if pipeline is None:
            return 2

        # make RepoDatabsae instance
        repoDb = self.makeRepodb(args)

        # make execution plan (a.k.a. DAG) for pipeline
        graphBuilder = GraphBuilder(self.taskFactory, butler, repoDb, args.data_query)
        graph = graphBuilder.makeGraph(pipeline)

        # execute
        return self.runPipeline(graph, butler, args)

    @staticmethod
    def configLog(longlog, logLevels):
        """Configure logging system.

        Parameters
        ----------
        longlog : bool
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
        lsstLog.configure_prop(_LOG_PROP.format(message_fmt))

        # configure individual loggers
        for component, level in logLevels:
            level = getattr(lsstLog.Log, level.upper(), None)
            if level is not None:
                logger = lsstLog.Log.getLogger(component or "")
                logger.setLevel(level)

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
                tasks = [(name, kind) for name, kind in tasks if kind == KIND_SUPERTASK]
            tasks.sort()

            headers = None
            if show_headers:
                print()
                headers = ("Task class name", "Kind     ")
            util.printTable(tasks, headers)

    def makePipeline(self, args):
        """Construct pipeline from command line arguments

        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line

        Returns
        -------
        `Pipeline` instance.
        """

        # need camera/package name to find overrides
        mapperClass = dafPersist.Butler.getMapperClass(args.input)
        camera = mapperClass.getCameraName()
        obsPkg = mapperClass.getPackageName()

        # for now parser supports just a single task on command line
        tasks = [(args.taskname, args.config_overrides)]

        pipeline = Pipeline()
        for taskName, configOverrides in tasks:

            # load task class
            try:
                taskClass, taskName = self.taskFactory.loadTaskClass(taskName)
            except ImportError:
                print("Failed to load task `{}'".format(taskName))
                return None

            # package all config overrides into a single object
            overrides = ConfigOverrides()

            # camera/package overrides
            configName = taskClass._DefaultName
            obsPkgDir = lsst.utils.getPackageDir(obsPkg)
            fileName = configName + ".py"
            for filePath in (
                os.path.join(obsPkgDir, "config", fileName),
                os.path.join(obsPkgDir, "config", camera, fileName),
            ):
                if os.path.exists(filePath):
                    lsstLog.info("Loading config overrride file %r", filePath)
                    overrides.addFileOverride(filePath)
                else:
                    lsstLog.debug("Config override file does not exist: %r", filePath)

            # command line overrides
            for override in configOverrides:
                if override.type == "override":
                    key, sep, val = override.value.partition('=')
                    overrides.addValueOverride(key, val)
                elif override.type == "file":
                    overrides.addFileOverride(override.value)

            # make config instance with defaults and overrides
            config = taskClass.ConfigClass()
            overrides.applyTo(config)

            pipeline.append(TaskDef(taskName, config, taskClass))

        return pipeline

    def makeRepodb(self, args):
        """Make repodb instance.

        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line

        Returns
        -------
        repodb.RepoDatabase instance.
        """
        if args.repo_db is not None:
            with open(args.repo_db, "rb") as fileObj:
                repodb = pickle.load(fileObj)
        else:
            # use some test database
            repodb = repodbTest.makeRepoDatabase()
        return repodb

    def runPipeline(self, plan, butler, args):
        """
        Parameters
        ----------
        plan : list of tuples
            Each tuple is (taskDef, quanta).
        butler : Butler
            data butler instance
        args : argparse.Namespace
            Parsed command line
        """

        # how many processes do we want
        numProc = args.processes

        # pre-flight check
        for taskDef, quanta in plan:
            task = self.taskFactory.makeTask(taskDef.taskClass, taskDef.config, None, butler)
            if not self.precall(task, butler, args):
                # non-zero means failure
                return 1

            if numProc > 1 and not taskDef.taskClass.canMultiprocess:
                lsstLog.warn("Task %s does not support multiprocessing; using one process", taskName)
                numProc = 1

        # chose map function being simple sequential map or multi-process map
        if numProc > 1:
            timeout = getattr(args, 'timeout', None)
            if timeout is None or timeout <= 0:
                timeout = self.MP_TIMEOUT
            mapFunc = _MPMap(numProc, timeout)
        else:
            # map in Py3 returns iterable and we want a complete result
            mapFunc = lambda func, iterable: list(map(func, iterable))

        # tasks are executed sequentially but quanta can run in parallel
        for taskDef, quanta in plan:
            # targets for map function
            target_list = [(taskDef.taskClass, taskDef.config, quantum, butler)
                           for quantum in quanta]
            # call task on each argument in a list
            profile_name = getattr(args, "profile", None)
            with util.profile(profile_name, lsstLog):
                mapFunc(self._executeSuperTask, target_list)

    def _executeSuperTask(self, target):
        """Execute super-task on a single data item.

        Parameters:
        target: `tuple` of `(taskClass, config, quantum, butler)`
        """
        taskClass, config, quantum, butler = target

        # setup logging, include dataId into MDC
#         if dataRef is not None:
#             if hasattr(dataRef, "dataId"):
#                 lsstLog.MDC("LABEL", str(dataRef.dataId))
#             elif isinstance(dataRef, (list, tuple)):
#                 lsstLog.MDC("LABEL", str([ref.dataId for ref in dataRef if hasattr(ref, "dataId")]))

        # make task instance
        task = self.taskFactory.makeTask(taskClass, config, None, butler)

        # Call task runQuantum() method and wrap it to catch exceptions that
        # don't inherit from Exception. Such exceptions aren't caught by
        # multiprocessing, which causes the slave process to crash and
        # you end up hitting the timeout.
        try:
            return task.runQuantum(quantum, butler)
        except Exception:
            raise  # No worries
        except:
            # Need to wrap the exception with something multiprocessing will recognise
            cls, exc, _ = sys.exc_info()
            lsstLog.warn("Unhandled exception %s (%s):\n%s" % (cls.__name__, exc, traceback.format_exc()))
            raise Exception("Unhandled exception: %s (%s)" % (cls.__name__, exc))


    @staticmethod
    def _precallImpl(task, butler, args):
        """The main work of 'precall'

        We write package versions, schemas and configs, or compare these to existing
        files on disk if present.

        Parameters
        ----------
        task
            instance of SuperTask
        butler : Butler
            data butler instance
        args : `argparse.Namespace`
            parsed command line
        """
#         if not args.noVersions:
#             task.writePackageVersions(args.butler, clobber=args.clobberVersions)
        do_backup = not args.noBackupConfig
        task.write_config(butler, clobber=args.clobberConfig, do_backup=do_backup)
        task.write_schemas(butler, clobber=args.clobberConfig, do_backup=do_backup)

    def precall(self, task, butler, args):
        """Hook for code that should run exactly once, before multiprocessing is invoked.

        The default implementation writes package versions, schemas and configs, or compares
        them to existing files on disk if present.

        Parameters
        ----------
        task
            instance of SuperTask
        args : `argparse.Namespace`
            parsed command line

        Returns
        -------
        `bool`, True if SuperTask should subsequently be called.
        """
        if args.doraise:
            self._precallImpl(task, butler, args)
        else:
            try:
                self._precallImpl(task, butler, args)
            except Exception as exc:
                task.log.fatal("Failed in task initialization: %s", exc)
                if not isinstance(exc, TaskError):
                    traceback.print_exc(file=sys.stderr)
                return False
        return True

    def _parseDirectories(self, namespace):
        """Parse input, output and calib directories

        This allows for hacking the directories, e.g., to include a "rerun".
        Modifications are made to the 'namespace' object in-place.
        """

        namespace.input = util.fixPath(DEFAULT_INPUT_NAME, namespace.inputRepo)
        namespace.calib = util.fixPath(DEFAULT_CALIB_NAME, namespace.calibRepo)

        # If an output directory is specified, process it and assign it to the namespace
        if namespace.outputRepo:
            namespace.output = util.fixPath(DEFAULT_OUTPUT_NAME, namespace.outputRepo)
        else:
            namespace.output = None

        # This section processes the rerun argument, if rerun is specified as a colon separated
        # value, it will be parsed as an input and output. The input value will be overridden if
        # previously specified (but a check is made to make sure both inputs use the same mapper)
        if namespace.rerun:
            if namespace.output:
                lsstLog.error("Error: cannot specify both --output and --rerun")
            namespace.rerun = namespace.rerun.split(":")
            rerunDir = [os.path.join(namespace.input, "rerun", dd) for dd in namespace.rerun]
            modifiedInput = False
            if len(rerunDir) == 2:
                namespace.input, namespace.output = rerunDir
                modifiedInput = True
            elif len(rerunDir) == 1:
                namespace.output = rerunDir[0]
                if os.path.exists(os.path.join(namespace.output, "_parent")):
                    namespace.input = os.path.realpath(os.path.join(namespace.output, "_parent"))
                    modifiedInput = True
            else:
                lsstLog.error("Error: invalid argument for --rerun: %s" % namespace.rerun)
            mapperClass = dafPersist.Butler.getMapperClass(namespace.input)
            if modifiedInput and dafPersist.Butler.getMapperClass(namespace.input) != mapperClass:
                lsstLog.error("Error: input directory specified by --rerun "
                              "must have the same mapper as INPUT")
        else:
            namespace.rerun = None

        del namespace.inputRepo
        del namespace.calibRepo
        del namespace.outputRepo
