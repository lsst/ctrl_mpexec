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
import re
import shutil
import tempfile
from typing import NamedTuple
import unittest

from lsst.ctrl.mpexec.cmdLineFwk import CmdLineFwk
from lsst.ctrl.mpexec.cli.opt import run_options
from lsst.ctrl.mpexec.cli.utils import (
    _ACTION_ADD_TASK,
    _ACTION_CONFIG,
    _ACTION_CONFIG_FILE,
    _ACTION_ADD_INSTRUMENT,
    PipetaskCommand,
)
from lsst.daf.butler import Config, Quantum, Registry
from lsst.daf.butler.registry import RegistryConfig
from lsst.obs.base import Instrument
import lsst.pex.config as pexConfig
from lsst.pipe.base import (Pipeline, PipelineTaskConfig, QuantumGraph, TaskDef, PipelineTaskConnections)
import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.pipe.base.tests.simpleQGraph import (
    AddTaskFactoryMock,
    makeSimpleButler,
    makeSimplePipeline,
    makeSimpleQGraph,
    populateButler)
from lsst.utils.tests import temporaryDirectory


logging.basicConfig(level=getattr(logging, os.environ.get("UNIT_TEST_LOGGING_LEVEL", "INFO"), logging.INFO))

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


def _makeArgs(registryConfig=None, **kwargs):
    """Return parsed command line arguments.

    By default butler_config is set to `Config` populated with some defaults,
    it can be overridden completely by keyword argument.

    Parameters
    ----------
    cmd : `str`, optional
        Produce arguments for this pipetask command.
    registryConfig : `RegistryConfig`, optional
        Override for registry configuration.
    **kwargs
        Overrides for other arguments.
    """
    # Use a mock to get the default value of arguments to 'run'.

    mock = unittest.mock.Mock()

    @click.command(cls=PipetaskCommand)
    @run_options()
    def fake_run(ctx, **kwargs):
        """Fake "pipetask run" command for gathering input arguments.

        The arguments & options should always match the arguments & options in
        the "real" command function `lsst.ctrl.mpexec.cli.cmd.run`.
        """
        mock(**kwargs)

    runner = click.testing.CliRunner()
    result = runner.invoke(fake_run)
    if result.exit_code != 0:
        raise RuntimeError(f"Failure getting default args from 'fake_run': {result}")
    mock.assert_called_once()
    args = mock.call_args[1]
    args["enableLsstDebug"] = args.pop("debug")
    args["execution_butler_location"] = args.pop("save_execution_butler")
    if "pipeline_actions" not in args:
        args["pipeline_actions"] = []
    args = SimpleNamespace(**args)

    # override butler_config with our defaults
    if "butler_config" not in kwargs:
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


class FakeDSType(NamedTuple):
    name: str


@dataclass(frozen=True)
class FakeDSRef:
    datasetType: str
    dataId: tuple


# Task class name used by tests, needs to be importable
_TASK_CLASS = "lsst.pipe.base.tests.simpleQGraph.AddTask"


def _makeQGraph():
    """Make a trivial QuantumGraph with one quantum.

    The only thing that we need to do with this quantum graph is to pickle
    it, the quanta in this graph are not usable for anything else.

    Returns
    -------
    qgraph : `~lsst.pipe.base.QuantumGraph`
    """
    taskDef = TaskDef(taskName=_TASK_CLASS, config=SimpleConfig())
    quanta = [Quantum(taskName=_TASK_CLASS,
                      inputs={FakeDSType("A"): [FakeDSRef("A", (1, 2))]})]  # type: ignore
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

        # single task pipeline, task name can be anything here
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

        # single task pipeline with config overrides, need real task class
        actions = [
            _ACTION_ADD_TASK(f"{_TASK_CLASS}:task"),
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
                _ACTION_ADD_TASK(f"{_TASK_CLASS}:task"),
                _ACTION_CONFIG_FILE("task:" + tmpname)
            ]
            args = _makeArgs(pipeline_actions=actions)
            pipeline = fwk.makePipeline(args)
            taskDefs = list(pipeline.toExpandedPipeline())
            self.assertEqual(len(taskDefs), 1)
            self.assertEqual(taskDefs[0].config.addend, 1000)

        # Check --instrument option, for now it only checks that it does not
        # crash.
        actions = [
            _ACTION_ADD_TASK(f"{_TASK_CLASS}:task"),
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
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig, execution_butler_location=None)
            qgraph = fwk.makeGraph(None, args)
            self.assertIsInstance(qgraph, QuantumGraph)
            self.assertEqual(len(qgraph), 1)

            # will fail if graph id does not match
            args = _makeArgs(
                qgraph=tmpname,
                qgraph_id="R2-D2 is that you?",
                registryConfig=registryConfig,
                execution_butler_location=None
            )
            with self.assertRaisesRegex(ValueError, "graphID does not match"):
                fwk.makeGraph(None, args)

            # save with wrong object type
            with open(tmpname, "wb") as saveFile:
                pickle.dump({}, saveFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig, execution_butler_location=None)
            with self.assertRaises(ValueError):
                fwk.makeGraph(None, args)

            # reading empty graph from pickle should work but makeGraph()
            # will return None and make a warning
            qgraph = QuantumGraph(dict())
            with open(tmpname, "wb") as saveFile:
                qgraph.save(saveFile)
            args = _makeArgs(qgraph=tmpname, registryConfig=registryConfig, execution_butler_location=None)
            with self.assertWarnsRegex(UserWarning, "QuantumGraph is empty"):
                # this also tests that warning is generated for empty graph
                qgraph = fwk.makeGraph(None, args)
            self.assertIs(qgraph, None)

    def testShowPipeline(self):
        """Test for --show options for pipeline.
        """
        fwk = CmdLineFwk()

        actions = [
            _ACTION_ADD_TASK(f"{_TASK_CLASS}:task"),
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
        self.nQuanta = 5
        self.pipeline = makeSimplePipeline(nQuanta=self.nQuanta)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)
        super().tearDownClass()

    def testSimpleQGraph(self):
        """Test successfull execution of trivial quantum graph.
        """
        args = _makeArgs(butler_config=self.root, input="test", output="output")
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(self.pipeline, butler)

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        qgraph = fwk.makeGraph(self.pipeline, args)
        self.assertEqual(len(qgraph.taskGraph), self.nQuanta)
        self.assertEqual(len(qgraph), self.nQuanta)

        # run whole thing
        fwk.runPipeline(qgraph, taskFactory, args)
        self.assertEqual(taskFactory.countExec, self.nQuanta)

    def testSimpleQGraphNoSkipExisting_inputs(self):
        """Test for case when output data for one task already appears in
        _input_ collection, but no ``--extend-run`` or ``-skip-existing``
        option is present.
        """
        args = _makeArgs(
            butler_config=self.root,
            input="test",
            output="output",
        )
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(
            self.pipeline, butler,
            datasetTypes={args.input: [
                "add_dataset0",
                "add_dataset1", "add2_dataset1",
                "add_init_output1",
                "task0_config",
                "task0_metadata",
                "task0_log",
            ]}
        )

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        qgraph = fwk.makeGraph(self.pipeline, args)
        self.assertEqual(len(qgraph.taskGraph), self.nQuanta)
        # With current implementation graph has all nQuanta quanta, but when
        # executing one quantum is skipped.
        self.assertEqual(len(qgraph), self.nQuanta)

        # run whole thing
        fwk.runPipeline(qgraph, taskFactory, args)
        self.assertEqual(taskFactory.countExec, self.nQuanta)

    def testSimpleQGraphSkipExisting_inputs(self):
        """Test for ``--skip-existing`` with output data for one task already
        appears in _input_ collection. No ``--extend-run`` option is needed
        for this case.
        """
        args = _makeArgs(
            butler_config=self.root,
            input="test",
            output="output",
            skip_existing_in=("test", ),
        )
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(
            self.pipeline, butler,
            datasetTypes={args.input: [
                "add_dataset0",
                "add_dataset1", "add2_dataset1",
                "add_init_output1",
                "task0_config",
                "task0_metadata",
                "task0_log",
            ]}
        )

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        qgraph = fwk.makeGraph(self.pipeline, args)
        self.assertEqual(len(qgraph.taskGraph), self.nQuanta)
        self.assertEqual(len(qgraph), self.nQuanta - 1)

        # run whole thing
        fwk.runPipeline(qgraph, taskFactory, args)
        self.assertEqual(taskFactory.countExec, self.nQuanta - 1)

    def testSimpleQGraphSkipExisting_outputs(self):
        """Test for ``--skip-existing`` with output data for one task already
        appears in _output_ collection. The ``--extend-run`` option is needed
        for this case.
        """
        args = _makeArgs(
            butler_config=self.root,
            input="test",
            output_run="output/run",
            skip_existing_in=("output/run", ),
        )
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(
            self.pipeline, butler, datasetTypes={
                args.input: ["add_dataset0"],
                args.output_run: [
                    "add_dataset1", "add2_dataset1",
                    "add_init_output1",
                    "task0_metadata",
                    "task0_log",
                ]
            }
        )

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        # fails without --extend-run
        with self.assertRaisesRegex(ValueError, "--extend-run was not given"):
            qgraph = fwk.makeGraph(self.pipeline, args)

        # retry with --extend-run
        args.extend_run = True
        qgraph = fwk.makeGraph(self.pipeline, args)

        self.assertEqual(len(qgraph.taskGraph), self.nQuanta)
        # Graph does not include quantum for first task
        self.assertEqual(len(qgraph), self.nQuanta - 1)

        # run whole thing
        fwk.runPipeline(qgraph, taskFactory, args)
        self.assertEqual(taskFactory.countExec, self.nQuanta - 1)

    def testSimpleQGraphOutputsFail(self):
        """Test continuing execution of trivial quantum graph with partial
        outputs.
        """
        args = _makeArgs(butler_config=self.root, input="test", output="output")
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(self.pipeline, butler)

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock(stopAt=3)

        qgraph = fwk.makeGraph(self.pipeline, args)
        self.assertEqual(len(qgraph), self.nQuanta)

        # run first three quanta
        with self.assertRaises(RuntimeError):
            fwk.runPipeline(qgraph, taskFactory, args)
        self.assertEqual(taskFactory.countExec, 3)

        butler.registry.refresh()

        # drop one of the two outputs from one task
        ref1 = butler.registry.findDataset("add2_dataset2", collections=args.output,
                                           instrument="INSTR", detector=0)
        self.assertIsNotNone(ref1)
        # also drop the metadata output
        ref2 = butler.registry.findDataset("task1_metadata", collections=args.output,
                                           instrument="INSTR", detector=0)
        self.assertIsNotNone(ref2)
        butler.pruneDatasets([ref1, ref2], disassociate=True, unstore=True, purge=True)

        taskFactory.stopAt = -1
        args.skip_existing_in = (args.output, )
        args.extend_run = True
        args.no_versions = True
        excRe = "Registry inconsistency while checking for existing outputs.*"
        with self.assertRaisesRegex(RuntimeError, excRe):
            fwk.runPipeline(qgraph, taskFactory, args)

    def testSimpleQGraphClobberOutputs(self):
        """Test continuing execution of trivial quantum graph with
        --clobber-outputs.
        """
        args = _makeArgs(butler_config=self.root, input="test", output="output")
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(self.pipeline, butler)

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock(stopAt=3)

        qgraph = fwk.makeGraph(self.pipeline, args)

        # should have one task and number of quanta
        self.assertEqual(len(qgraph), self.nQuanta)

        # run first three quanta
        with self.assertRaises(RuntimeError):
            fwk.runPipeline(qgraph, taskFactory, args)
        self.assertEqual(taskFactory.countExec, 3)

        butler.registry.refresh()

        # drop one of the two outputs from one task
        ref1 = butler.registry.findDataset("add2_dataset2", collections=args.output,
                                           dataId=dict(instrument="INSTR", detector=0))
        self.assertIsNotNone(ref1)
        # also drop the metadata output
        ref2 = butler.registry.findDataset("task1_metadata", collections=args.output,
                                           dataId=dict(instrument="INSTR", detector=0))
        self.assertIsNotNone(ref2)
        butler.pruneDatasets([ref1, ref2], disassociate=True, unstore=True, purge=True)

        taskFactory.stopAt = -1
        args.skip_existing = True
        args.extend_run = True
        args.clobber_outputs = True
        args.no_versions = True
        fwk.runPipeline(qgraph, taskFactory, args)
        # number of executed quanta is incremented
        self.assertEqual(taskFactory.countExec, self.nQuanta + 1)

    def testSimpleQGraphReplaceRun(self):
        """Test repeated execution of trivial quantum graph with
        --replace-run.
        """
        args = _makeArgs(
            butler_config=self.root,
            input="test",
            output="output",
            output_run="output/run1")
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(self.pipeline, butler)

        fwk = CmdLineFwk()
        taskFactory = AddTaskFactoryMock()

        qgraph = fwk.makeGraph(self.pipeline, args)

        # should have one task and number of quanta
        self.assertEqual(len(qgraph), self.nQuanta)

        # deep copy is needed because quanta are updated in place
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)
        self.assertEqual(taskFactory.countExec, self.nQuanta)

        # need to refresh collections explicitly (or make new butler/registry)
        butler.registry.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1"})

        # number of datasets written by pipeline:
        #  - nQuanta of init_outputs
        #  - nQuanta of configs
        #  - packages (single dataset)
        #  - nQuanta * two output datasets
        #  - nQuanta of metadata
        #  - nQuanta of log output
        n_outputs = self.nQuanta * 6 + 1
        refs = butler.registry.queryDatasets(..., collections="output/run1")
        self.assertEqual(len(list(refs)), n_outputs)

        # re-run with --replace-run (--inputs is ignored, as long as it hasn't
        # changed)
        args.replace_run = True
        args.output_run = "output/run2"
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)

        butler.registry.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2"})

        # new output collection
        refs = butler.registry.queryDatasets(..., collections="output/run2")
        self.assertEqual(len(list(refs)), n_outputs)

        # old output collection is still there
        refs = butler.registry.queryDatasets(..., collections="output/run1")
        self.assertEqual(len(list(refs)), n_outputs)

        # re-run with --replace-run and --prune-replaced=unstore
        args.replace_run = True
        args.prune_replaced = "unstore"
        args.output_run = "output/run3"
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)

        butler.registry.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2", "output/run3"})

        # new output collection
        refs = butler.registry.queryDatasets(..., collections="output/run3")
        self.assertEqual(len(list(refs)), n_outputs)

        # old output collection is still there, and it has all datasets but
        # non-InitOutputs are not in datastore
        refs = butler.registry.queryDatasets(..., collections="output/run2")
        refs = list(refs)
        self.assertEqual(len(refs), n_outputs)
        initOutNameRe = re.compile("packages|task.*_config|add_init_output.*")
        for ref in refs:
            if initOutNameRe.fullmatch(ref.datasetType.name):
                butler.get(ref, collections="output/run2")
            else:
                with self.assertRaises(FileNotFoundError):
                    butler.get(ref, collections="output/run2")

        # re-run with --replace-run and --prune-replaced=purge
        # This time also remove --input; passing the same inputs that we
        # started with and not passing inputs at all should be equivalent.
        args.input = None
        args.replace_run = True
        args.prune_replaced = "purge"
        args.output_run = "output/run4"
        fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)

        butler.registry.refresh()
        collections = set(butler.registry.queryCollections(...))
        # output/run3 should disappear now
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2", "output/run4"})

        # new output collection
        refs = butler.registry.queryDatasets(..., collections="output/run4")
        self.assertEqual(len(list(refs)), n_outputs)

        # Trying to run again with inputs that aren't exactly what we started
        # with is an error, and the kind that should not modify the data repo.
        with self.assertRaises(ValueError):
            args.input = ["test", "output/run2"]
            args.prune_replaced = None
            args.replace_run = True
            args.output_run = "output/run5"
            fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)
        butler.registry.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2", "output/run4"})
        with self.assertRaises(ValueError):
            args.input = ["output/run2", "test"]
            args.prune_replaced = None
            args.replace_run = True
            args.output_run = "output/run6"
            fwk.runPipeline(copy.deepcopy(qgraph), taskFactory, args)
        butler.registry.refresh()
        collections = set(butler.registry.queryCollections(...))
        self.assertEqual(collections, {"test", "output", "output/run1", "output/run2", "output/run4"})

    def testSubgraph(self):
        """Test successfull execution of trivial quantum graph.
        """
        args = _makeArgs(butler_config=self.root, input="test", output="output")
        butler = makeSimpleButler(self.root, run=args.input, inMemory=False)
        populateButler(self.pipeline, butler)

        fwk = CmdLineFwk()
        qgraph = fwk.makeGraph(self.pipeline, args)

        # Select first two nodes for execution. This depends on node ordering
        # which I assume is the same as execution order.
        nNodes = 2
        nodeIds = [node.nodeId.number for node in qgraph]
        nodeIds = nodeIds[:nNodes]

        self.assertEqual(len(qgraph.taskGraph), self.nQuanta)
        self.assertEqual(len(qgraph), self.nQuanta)

        with makeTmpFile(suffix=".qgraph") as tmpname, makeSQLiteRegistry() as registryConfig:
            with open(tmpname, "wb") as saveFile:
                qgraph.save(saveFile)

            args = _makeArgs(qgraph=tmpname, qgraph_node_id=nodeIds, registryConfig=registryConfig,
                             execution_butler_location=None)
            fwk = CmdLineFwk()

            # load graph, should only read a subset
            qgraph = fwk.makeGraph(pipeline=None, args=args)
            self.assertEqual(len(qgraph), nNodes)

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
