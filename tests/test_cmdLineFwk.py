# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Simple unit test for cmdLineFwk module.
"""

import argparse
import contextlib
import logging
import os
import pickle
import shutil
import tempfile
import unittest

from lsst.ctrl.mpexec.cmdLineFwk import CmdLineFwk
from lsst.ctrl.mpexec.cmdLineParser import (_ACTION_ADD_TASK, _ACTION_CONFIG,
                                            _ACTION_CONFIG_FILE, _ACTION_ADD_INSTRUMENT)
from lsst.daf.butler import Config, Quantum, Registry
from lsst.daf.butler.registry import RegistryConfig
from lsst.obs.base import Instrument
import lsst.pex.config as pexConfig
from lsst.pipe.base import (Pipeline, PipelineTask, PipelineTaskConfig,
                            QuantumGraph, QuantumGraphTaskNodes,
                            TaskDef, TaskFactory, PipelineTaskConnections)
import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from testUtil import (AddTaskFactoryMock, makeSimpleQGraph)


logging.basicConfig(level=logging.INFO)

# Have to monkey-patch Instrument.fromName() to not retrieve non-existing
# instrument from registry, these tests can run fine without actual instrument
# and implementing full mock for Instrument is too complicated.
Instrument.fromName = lambda name, reg: None


@contextlib.contextmanager
def makeTmpFile(contents=None):
    """Context manager for generating temporary file name.

    Temporary file is deleted on exiting context.

    Parameters
    ----------
    contents : `bytes`
        Data to write into a file.
    """
    fd, tmpname = tempfile.mkstemp()
    if contents:
        os.write(fd, contents)
    os.close(fd)
    yield tmpname
    with contextlib.suppress(OSError):
        os.remove(tmpname)


@contextlib.contextmanager
def makeSQLiteRegistry():
    """Context manager to create new empty registry database.

    Yields
    ------
    config : `RegistryConfig`
        Registry configuration for initialized registry database.
    """
    with makeTmpFile() as filename:
        uri = f"sqlite:///{filename}"
        config = RegistryConfig()
        config["db"] = uri
        Registry.fromConfig(config, create=True)
        yield config


class SimpleConnections(PipelineTaskConnections, dimensions=(),
                        defaultTemplates={"template": "simple"}):
    schema = cT.InitInput(doc="Schema",
                          name="{template}schema",
                          storageClass="SourceCatalog")


class SimpleConfig(PipelineTaskConfig, pipelineConnections=SimpleConnections):
    field = pexConfig.Field(dtype=str, doc="arbitrary string")

    def setDefaults(self):
        PipelineTaskConfig.setDefaults(self)


class TaskOne(PipelineTask):
    ConfigClass = SimpleConfig
    _DefaultName = "taskOne"


class TaskTwo(PipelineTask):
    ConfigClass = SimpleConfig
    _DefaultName = "taskTwo"


class TaskFactoryMock(TaskFactory):
    def loadTaskClass(self, taskName):
        if taskName == "TaskOne":
            return TaskOne, "TaskOne"
        elif taskName == "TaskTwo":
            return TaskTwo, "TaskTwo"

    def makeTask(self, taskClass, config, overrides, butler):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        return taskClass(config=config, butler=butler)


def _makeArgs(pipeline=None, qgraph=None, pipeline_actions=(), order_pipeline=False,
              save_pipeline="", save_qgraph="", save_single_quanta="",
              pipeline_dot="", qgraph_dot="", registryConfig=None):
    """Return parsed command line arguments.

    Parameters
    ----------
    pipeline : `str`, optional
        Name of the YAML file with pipeline.
    qgraph : `str`, optional
        Name of the pickle file with QGraph.
    pipeline_actions : itrable of `cmdLinePArser._PipelineAction`, optional
    order_pipeline : `bool`
    save_pipeline : `str`
        Name of the YAML file to store pipeline.
    save_qgraph : `str`
        Name of the pickle file to store QGraph.
    save_single_quanta : `str`
        Name of the pickle file pattern to store individual QGraph.
    pipeline_dot : `str`
        Name of the DOT file to write pipeline graph.
    qgraph_dot : `str`
        Name of the DOT file to write QGraph representation.
    """
    args = argparse.Namespace()
    args.butler_config = Config()
    if registryConfig:
        args.butler_config["registry"] = registryConfig
    # The default datastore has a relocatable root, so we need to specify
    # some root here for it to use
    args.butler_config.configFile = "."
    args.pipeline = pipeline
    args.qgraph = qgraph
    args.pipeline_actions = pipeline_actions
    args.order_pipeline = order_pipeline
    args.save_pipeline = save_pipeline
    args.save_qgraph = save_qgraph
    args.save_single_quanta = save_single_quanta
    args.pipeline_dot = pipeline_dot
    args.qgraph_dot = qgraph_dot
    args.input = ""
    args.output = None
    args.output_run = None
    args.extend_run = False
    args.replace_run = False
    args.prune_replaced = False
    args.register_dataset_types = False
    args.skip_init_writes = False
    args.no_versions = False
    args.skip_existing = False
    args.clobber_partial_outputs = False
    args.init_only = False
    args.processes = 1
    args.profile = None
    args.enableLsstDebug = False
    args.graph_fixup = None
    args.timeout = None
    args.fail_fast = False
    return args


def _makeQGraph():
    """Make a trivial QuantumGraph with one quantum.

    The only thing that we need to do with this quantum graph is to pickle
    it, the quanta in this graph are not usable for anything else.

    Returns
    -------
    qgraph : `~lsst.pipe.base.QuantumGraph`
    """
    taskDef = TaskDef(taskName="taskOne", config=SimpleConfig())
    quanta = [Quantum()]
    taskNodes = QuantumGraphTaskNodes(taskDef=taskDef, quanta=quanta, initInputs={}, initOutputs={})
    qgraph = QuantumGraph([taskNodes])
    return qgraph


class CmdLineFwkTestCase(unittest.TestCase):
    """A test case for CmdLineFwk
    """

    def testMakePipeline(self):
        """Tests for CmdLineFwk.makePipeline method
        """
        fwk = CmdLineFwk()

        # make empty pipeline
        args = _makeArgs()
        pipeline = fwk.makePipeline(args)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 0)

        # few tests with serialization
        with makeTmpFile() as tmpname:
            # make empty pipeline and store it in a file
            args = _makeArgs(save_pipeline=tmpname)
            pipeline = fwk.makePipeline(args)
            self.assertIsInstance(pipeline, Pipeline)

            # read pipeline from a file
            args = _makeArgs(pipeline=tmpname)
            pipeline = fwk.makePipeline(args)
            self.assertIsInstance(pipeline, Pipeline)
            self.assertEqual(len(pipeline), 0)

        # single task pipeline
        actions = [
            _ACTION_ADD_TASK("TaskOne:task1")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(args)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 1)

        # many task pipeline
        actions = [
            _ACTION_ADD_TASK("TaskOne:task1a"),
            _ACTION_ADD_TASK("TaskTwo:task2"),
            _ACTION_ADD_TASK("TaskOne:task1b")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(args)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 3)

        # single task pipeline with config overrides, cannot use TaskOne, need
        # something that can be imported with `doImport()`
        actions = [
            _ACTION_ADD_TASK("testUtil.AddTask:task"),
            _ACTION_CONFIG("task:addend=100")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(args)
        taskDefs = list(pipeline.toExpandedPipeline())
        self.assertEqual(len(taskDefs), 1)
        self.assertEqual(taskDefs[0].config.addend, 100)

        overrides = b"config.addend = 1000\n"
        with makeTmpFile(overrides) as tmpname:
            actions = [
                _ACTION_ADD_TASK("testUtil.AddTask:task"),
                _ACTION_CONFIG_FILE("task:" + tmpname)
            ]
            args = _makeArgs(pipeline_actions=actions)
            pipeline = fwk.makePipeline(args)
            taskDefs = list(pipeline.toExpandedPipeline())
            self.assertEqual(len(taskDefs), 1)
            self.assertEqual(taskDefs[0].config.addend, 1000)

        # Check --instrument option, for now it only checks that it does not crash
        actions = [
            _ACTION_ADD_TASK("testUtil.AddTask:task"),
            _ACTION_ADD_INSTRUMENT("Instrument")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(args)

    def testMakeGraphFromPickle(self):
        """Tests for CmdLineFwk.makeGraph method.

        Only most trivial case is tested that does not do actual graph
        building.
        """
        fwk = CmdLineFwk()

        with makeTmpFile() as tmpname, makeSQLiteRegistry() as registryConfig:

            # make non-empty graph and store it in a file
            qgraph = _makeQGraph()
            with open(tmpname, "wb") as pickleFile:
                qgraph.save(pickleFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig)
            qgraph = fwk.makeGraph(None, args)
            self.assertIsInstance(qgraph, QuantumGraph)
            self.assertEqual(len(qgraph), 1)

            # pickle with wrong object type
            with open(tmpname, "wb") as pickleFile:
                pickle.dump({}, pickleFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig)
            with self.assertRaises(TypeError):
                fwk.makeGraph(None, args)

            # reading empty graph from pickle should work but makeGraph()
            # will return None and make a warning
            qgraph = QuantumGraph()
            with open(tmpname, "wb") as pickleFile:
                qgraph.save(pickleFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig)
            with self.assertWarnsRegex(UserWarning, "QuantumGraph is empty"):
                # this also tests that warning is generated for empty graph
                qgraph = fwk.makeGraph(None, args)
            self.assertIs(qgraph, None)

    def testShowPipeline(self):
        """Test for --show options for pipeline.
        """
        fwk = CmdLineFwk()

        actions = [
            _ACTION_ADD_TASK("testUtil.AddTask:task"),
            _ACTION_CONFIG("task:addend=100")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(args)

        args.show = ["pipeline"]
        fwk.showInfo(args, pipeline)
        args.show = ["config"]
        fwk.showInfo(args, pipeline)
        args.show = ["history=task::addend"]
        fwk.showInfo(args, pipeline)
        args.show = ["tasks"]
        fwk.showInfo(args, pipeline)


class CmdLineFwkTestCaseWithButler(unittest.TestCase):
    """A test case for CmdLineFwk
    """

    def setUp(self):
        super().setUpClass()
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)
        super().tearDownClass()

    def testSimpleQGraph(self):
        """Test successfull execution of trivial quantum graph.
        """

        nQuanta = 5
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        # should have one task and number of quanta
        self.assertEqual(len(list(qgraph.quanta())), nQuanta)

        args = _makeArgs()
        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        # run whole thing
        fwk.runPipeline(qgraph, taskFactory, args, butler=butler)
        self.assertEqual(taskFactory.countExec, nQuanta)

    def testSimpleQGraphSkipExisting(self):
        """Test continuing execution of trivial quantum graph with --skip-existing.
        """

        nQuanta = 5
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        # should have one task and number of quanta
        self.assertEqual(len(list(qgraph.quanta())), nQuanta)

        args = _makeArgs()
        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock(stopAt=3)

        # run first three quanta
        with self.assertRaises(RuntimeError):
            fwk.runPipeline(qgraph, taskFactory, args, butler=butler)
        self.assertEqual(taskFactory.countExec, 3)

        # run remaining ones
        taskFactory.stopAt = -1
        args.skip_existing = True
        args.no_versions = True
        fwk.runPipeline(qgraph, taskFactory, args, butler=butler)
        self.assertEqual(taskFactory.countExec, nQuanta)

    def testSimpleQGraphPartialOutputsFail(self):
        """Test continuing execution of trivial quantum graph with partial
        outputs.
        """

        nQuanta = 5
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        # should have one task and number of quanta
        self.assertEqual(len(list(qgraph.quanta())), nQuanta)

        args = _makeArgs()
        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock(stopAt=3)

        # run first three quanta
        with self.assertRaises(RuntimeError):
            fwk.runPipeline(qgraph, taskFactory, args, butler=butler)
        self.assertEqual(taskFactory.countExec, 3)

        # drop one of the two outputs from one task
        ref = butler._findDatasetRef("add2_dataset2", instrument="INSTR", detector=0)
        self.assertIsNotNone(ref)
        butler.pruneDatasets([ref], disassociate=True, unstore=True, purge=True)

        taskFactory.stopAt = -1
        args.skip_existing = True
        args.no_versions = True
        excRe = "Registry inconsistency while checking for existing outputs.*"
        with self.assertRaisesRegex(RuntimeError, excRe):
            fwk.runPipeline(qgraph, taskFactory, args, butler=butler)

    def testSimpleQGraphClobberPartialOutputs(self):
        """Test continuing execution of trivial quantum graph with
        --clobber-partial-outputs.
        """

        nQuanta = 5
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        # should have one task and number of quanta
        self.assertEqual(len(list(qgraph.quanta())), nQuanta)

        args = _makeArgs()
        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock(stopAt=3)

        # run first three quanta
        with self.assertRaises(RuntimeError):
            fwk.runPipeline(qgraph, taskFactory, args, butler=butler)
        self.assertEqual(taskFactory.countExec, 3)

        # drop one of the two outputs from one task
        ref = butler._findDatasetRef("add2_dataset2", instrument="INSTR", detector=0)
        self.assertIsNotNone(ref)
        butler.pruneDatasets([ref], disassociate=True, unstore=True, purge=True)

        taskFactory.stopAt = -1
        args.skip_existing = True
        args.clobber_partial_outputs = True
        args.no_versions = True
        fwk.runPipeline(qgraph, taskFactory, args, butler=butler)
        # number of executed quanta is incremented
        self.assertEqual(taskFactory.countExec, nQuanta + 1)

    def testShowGraph(self):
        """Test for --show options for quantum graph.
        """
        fwk = CmdLineFwk()

        nQuanta = 2
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        args = _makeArgs()
        args.show = ["graph"]
        fwk.showInfo(args, pipeline=None, graph=qgraph)
        # TODO: cannot test "workflow" option presently, it instanciates
        # butler from command line options and there is no way to pass butler
        # mock to that code.


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
