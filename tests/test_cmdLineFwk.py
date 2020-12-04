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

import click
from types import SimpleNamespace
import contextlib
import copy
from dataclasses import dataclass
import logging
import os
import pickle
import shutil
import tempfile
from typing import NamedTuple
import unittest

from lsst.ctrl.mpexec.cmdLineFwk import CmdLineFwk
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetaskCli
from lsst.ctrl.mpexec.cli.utils import (_ACTION_ADD_TASK, _ACTION_CONFIG,
                                        _ACTION_CONFIG_FILE, _ACTION_ADD_INSTRUMENT)
from lsst.daf.butler import Config, Quantum, Registry
from lsst.daf.butler.cli.utils import Mocker
from lsst.daf.butler.registry import RegistryConfig
from lsst.obs.base import Instrument
import lsst.pex.config as pexConfig
from lsst.pipe.base import (Pipeline, PipelineTask, PipelineTaskConfig,
                            QuantumGraph, TaskDef, TaskFactory,
                            PipelineTaskConnections)
import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.pipe.base.tests.simpleQGraph import (AddTaskFactoryMock, makeSimpleQGraph)
from lsst.utils.tests import temporaryDirectory


logging.basicConfig(level=logging.INFO)

# Have to monkey-patch Instrument.fromName() to not retrieve non-existing
# instrument from registry, these tests can run fine without actual instrument
# and implementing full mock for Instrument is too complicated.
Instrument.fromName = lambda name, reg: None


@contextlib.contextmanager
def makeTmpFile(contents=None, suffix=None):
    """Context manager for generating temporary file name.

    Temporary file is deleted on exiting context.

    Parameters
    ----------
    contents : `bytes`
        Data to write into a file.
    """
    fd, tmpname = tempfile.mkstemp(suffix=suffix)
    if contents:
        os.write(fd, contents)
    os.close(fd)
    yield tmpname
    with contextlib.suppress(OSError):
        os.remove(tmpname)


@contextlib.contextmanager
def makeSQLiteRegistry(create=True):
    """Context manager to create new empty registry database.

    Yields
    ------
    config : `RegistryConfig`
        Registry configuration for initialized registry database.
    """
    with temporaryDirectory() as tmpdir:
        uri = f"sqlite:///{tmpdir}/gen3.sqlite"
        config = RegistryConfig()
        config["db"] = uri
        if create:
            Registry.createFromConfig(config)
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


def _makeArgs(registryConfig=None, **kwargs):
    """Return parsed command line arguments.

    By default butler_config is set to `Config` populated with some defaults,
    it can be overriden completely by keyword argument.

    Parameters
    ----------
    cmd : `str`, optional
        Produce arguments for this pipetask command.
    registryConfig : `RegistryConfig`, optional
        Override for registry configuration.
    **kwargs
        Overrides for other arguments.
    """
    # Execute the "run" command with the --call-mocker flag set so we can get
    # all the default arguments that were passed to the command function out of
    # the Mocker call.
    # At some point, ctrl_mpexec should stop passing around a SimpleNamespace
    # of arguments, which would make this workaround unnecessary.
    runner = click.testing.CliRunner()
    result = result = runner.invoke(pipetaskCli, ["run", "--call-mocker"])
    if result.exit_code != 0:
        raise RuntimeError("Failure getting default args from 'pipetask run'.")
    _, args = Mocker.mock.call_args
    args["enableLsstDebug"] = args.pop("debug")
    if "pipeline_actions" not in args:
        args["pipeline_actions"] = []
    args = SimpleNamespace(**args)

    # override butler_config with our defaults
    args.butler_config = Config()
    if registryConfig:
        args.butler_config["registry"] = registryConfig
    # The default datastore has a relocatable root, so we need to specify
    # some root here for it to use
    args.butler_config.configFile = "."
    # override arguments from keyword parameters
    for key, value in kwargs.items():
        setattr(args, key, value)
    return args


class FakeTaskDef(NamedTuple):
    name: str


@dataclass(frozen=True)
class FakeDSRef:
    datasetType: str
    dataId: tuple


def _makeQGraph():
    """Make a trivial QuantumGraph with one quantum.

    The only thing that we need to do with this quantum graph is to pickle
    it, the quanta in this graph are not usable for anything else.

    Returns
    -------
    qgraph : `~lsst.pipe.base.QuantumGraph`
    """

    # The task name in TaskDef needs to be a real importable name, use one that is sure to exist
    taskDef = TaskDef(taskName="lsst.pipe.base.Struct", config=SimpleConfig())
    quanta = [Quantum(taskName="lsst.pipe.base.Struct",
                      inputs={FakeTaskDef("A"): FakeDSRef("A", (1, 2))})]  # type: ignore
    qgraph = QuantumGraph({taskDef: set(quanta)})
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
            _ACTION_ADD_TASK("lsst.pipe.base.tests.simpleQGraph.AddTask:task"),
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
                _ACTION_ADD_TASK("lsst.pipe.base.tests.simpleQGraph.AddTask:task"),
                _ACTION_CONFIG_FILE("task:" + tmpname)
            ]
            args = _makeArgs(pipeline_actions=actions)
            pipeline = fwk.makePipeline(args)
            taskDefs = list(pipeline.toExpandedPipeline())
            self.assertEqual(len(taskDefs), 1)
            self.assertEqual(taskDefs[0].config.addend, 1000)

        # Check --instrument option, for now it only checks that it does not crash
        actions = [
            _ACTION_ADD_TASK("lsst.pipe.base.tests.simpleQGraph.AddTask:task"),
            _ACTION_ADD_INSTRUMENT("Instrument")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(args)

    def testMakeGraphFromSave(self):
        """Tests for CmdLineFwk.makeGraph method.

        Only most trivial case is tested that does not do actual graph
        building.
        """
        fwk = CmdLineFwk()

        with makeTmpFile(suffix=".qgraph") as tmpname, makeSQLiteRegistry() as registryConfig:

            # make non-empty graph and store it in a file
            qgraph = _makeQGraph()
            with open(tmpname, "wb") as saveFile:
                qgraph.save(saveFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig)
            qgraph = fwk.makeGraph(None, args)
            self.assertIsInstance(qgraph, QuantumGraph)
            self.assertEqual(len(qgraph), 1)

            # save with wrong object type
            with open(tmpname, "wb") as saveFile:
                pickle.dump({}, saveFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig)
            with self.assertRaises(ValueError):
                fwk.makeGraph(None, args)

            # reading empty graph from pickle should work but makeGraph()
            # will return None and make a warning
            qgraph = QuantumGraph(dict())
            with open(tmpname, "wb") as saveFile:
                qgraph.save(saveFile)
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
            _ACTION_ADD_TASK("lsst.pipe.base.tests.simpleQGraph.AddTask:task"),
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

        self.assertEqual(len(qgraph.taskGraph), 5)
        self.assertEqual(len(qgraph), nQuanta)

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

        self.assertEqual(len(qgraph.taskGraph), 5)
        self.assertEqual(len(qgraph), nQuanta)

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
        self.assertEqual(len(qgraph), nQuanta)

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
        self.assertEqual(len(qgraph), nQuanta)

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

    def testSimpleQGraphReplaceRun(self):
        """Test repeated execution of trivial quantum graph with
        --replace-run.
        """

        # need non-memory registry in this case
        nQuanta = 5
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, inMemory=False)

        # should have one task and number of quanta
        self.assertEqual(len(qgraph), nQuanta)

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        # run whole thing
        args = _makeArgs(
            butler_config=self.root,
            input="test",
            output="output",
            output_run="output/run1")
        # deep copy is needed because quanta are updated in place
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)
        self.assertEqual(taskFactory.countExec, nQuanta)

        # need to refresh collections explicitly (or make new butler/registry)
        butler.registry._collections.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1"})

        # number of datasets written by pipeline:
        #  - nQuanta of init_outputs
        #  - nQuanta of configs
        #  - packages (single dataset)
        #  - nQuanta * two output datasets
        #  - nQuanta of metadata
        n_outputs = nQuanta * 5 + 1
        refs = butler.registry.queryDatasets(..., collections="output/run1")
        self.assertEqual(len(list(refs)), n_outputs)

        # re-run with --replace-run (--inputs is not compatible)
        args.input = None
        args.replace_run = True
        args.output_run = "output/run2"
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)

        butler.registry._collections.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2"})

        # new output collection
        refs = butler.registry.queryDatasets(..., collections="output/run2")
        self.assertEqual(len(list(refs)), n_outputs)

        # old output collection is still there
        refs = butler.registry.queryDatasets(..., collections="output/run1")
        self.assertEqual(len(list(refs)), n_outputs)

        # re-run with --replace-run and --prune-replaced=unstore
        args.input = None
        args.replace_run = True
        args.prune_replaced = "unstore"
        args.output_run = "output/run3"
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)

        butler.registry._collections.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2", "output/run3"})

        # new output collection
        refs = butler.registry.queryDatasets(..., collections="output/run3")
        self.assertEqual(len(list(refs)), n_outputs)

        # old output collection is still there, and it has all datasets but
        # they are not in datastore
        refs = butler.registry.queryDatasets(..., collections="output/run2")
        refs = list(refs)
        self.assertEqual(len(refs), n_outputs)
        with self.assertRaises(FileNotFoundError):
            butler.get(refs[0], collections="output/run2")

        # re-run with --replace-run and --prune-replaced=purge
        args.input = None
        args.replace_run = True
        args.prune_replaced = "purge"
        args.output_run = "output/run4"
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)

        butler.registry._collections.refresh()
        collections = set(butler.registry.queryCollections(...))
        # output/run3 should disappear now
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2", "output/run4"})

        # new output collection
        refs = butler.registry.queryDatasets(..., collections="output/run4")
        self.assertEqual(len(list(refs)), n_outputs)

    def testShowGraph(self):
        """Test for --show options for quantum graph.
        """
        fwk = CmdLineFwk()

        nQuanta = 2
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        args = _makeArgs(show=["graph"])
        fwk.showInfo(args, pipeline=None, graph=qgraph)

    def testShowGraphWorkflow(self):
        fwk = CmdLineFwk()

        nQuanta = 2
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root)

        args = _makeArgs(show=["workflow"])
        fwk.showInfo(args, pipeline=None, graph=qgraph)

        # TODO: cannot test "uri" option presently, it instanciates
        # butler from command line options and there is no way to pass butler
        # mock to that code.


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
