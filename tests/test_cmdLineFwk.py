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
import pickle
import os
import tempfile
import unittest

from lsst.ctrl.mpexec.cmdLineFwk import CmdLineFwk
from lsst.ctrl.mpexec.cmdLineParser import _PipelineAction
import lsst.pex.config as pexConfig
from lsst.pipe.base import (Pipeline, PipelineTask, PipelineTaskConfig,
                            QuantumGraph, TaskFactory, InitInputDatasetField)
import lsst.utils.tests


@contextlib.contextmanager
def makeTmpFile():
    """Context manager for generating temporary file name.

    Temporary file is deleted on exiting context.
    """
    fd, tmpname = tempfile.mkstemp()
    os.close(fd)
    yield tmpname
    with contextlib.suppress(OSError):
        os.remove(tmpname)


class SimpleConfig(PipelineTaskConfig):
    field = pexConfig.Field(dtype=str, doc="arbitrary string")
    schema = InitInputDatasetField(doc="Schema", name="",
                                   nameTemplate="{template}schema",
                                   storageClass="SourceCatalog")

    def setDefaults(self):
        PipelineTaskConfig.setDefaults(self)
        self.formatTemplateNames({"template": ""})


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
              save_pipeline="", save_qgraph="", pipeline_dot="", qgraph_dot=""):
    """Return parsed command line arguments.

    Parameters
    ----------
    pipeline : `str`, optional
        Name of the pickle file with pipeline.
    qgraph : `str`, optional
        Name of the pickle file with QGraph.
    pipeline_actions : itrable of `cmdLinePArser._PipelineAction`, optional
    order_pipeline : `bool`
    save_pipeline : `str`
        Name of the pickle file to store pipeline.
    save_qgraph : `str`
        Name of the pickle file to store QGraph.
    pipeline_dot : `str`
        Name of the DOT file to write pipeline graph.
    qgraph_dot : `str`
        Name of the DOT file to write QGrpah representation.
    """
    args = argparse.Namespace()
    args.pipeline = pipeline
    args.qgraph = qgraph
    args.pipeline_actions = pipeline_actions
    args.order_pipeline = order_pipeline
    args.save_pipeline = save_pipeline
    args.save_qgraph = save_qgraph
    args.pipeline_dot = pipeline_dot
    args.qgraph_dot = qgraph_dot
    return args


class CmdLineFwkTestCase(unittest.TestCase):
    """A test case for CmdLineFwk
    """

    def testMakePipeline(self):
        """Tests for CmdLineFwk.makePipeline method
        """
        fwk = CmdLineFwk()
        taskFactory = TaskFactoryMock()

        # make empty pipeline
        args = _makeArgs()
        pipeline = fwk.makePipeline(taskFactory, args)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 0)

        # few tests with pickle
        with makeTmpFile() as tmpname:
            # make empty pipeline and store it in a file
            args = _makeArgs(save_pipeline=tmpname)
            pipeline = fwk.makePipeline(taskFactory, args)
            self.assertIsInstance(pipeline, Pipeline)

            # read pipeline from a file
            args = _makeArgs(pipeline=tmpname)
            pipeline = fwk.makePipeline(taskFactory, args)
            self.assertIsInstance(pipeline, Pipeline)
            self.assertEqual(len(pipeline), 0)

            # pickle with wrong object type
            with open(tmpname, "wb") as pickleFile:
                pickle.dump({}, pickleFile)
            args = _makeArgs(pipeline=tmpname)
            with self.assertRaises(TypeError):
                fwk.makePipeline(taskFactory, args)

        # single task pipeline
        actions = [
            _PipelineAction(action="new_task", label="task1", value="TaskOne")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(taskFactory, args)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 1)

        # many task pipeline
        actions = [
            _PipelineAction(action="new_task", label="task1a", value="TaskOne"),
            _PipelineAction(action="new_task", label="task2", value="TaskTwo"),
            _PipelineAction(action="new_task", label="task1b", value="TaskOne")
        ]
        args = _makeArgs(pipeline_actions=actions)
        pipeline = fwk.makePipeline(taskFactory, args)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 3)

    def testMakeGraphFromPickle(self):
        """Tests for CmdLineFwk.makeGraph method.

        Only most trivial case is tested that does not do actual graph
        building.
        """
        fwk = CmdLineFwk()
        taskFactory = TaskFactoryMock()

        with makeTmpFile() as tmpname:

            # make empty graph and store it in a file
            qgraph = QuantumGraph()
            with open(tmpname, "wb") as pickleFile:
                pickle.dump(qgraph, pickleFile)
            args = _makeArgs(qgraph=tmpname)
            qgraph = fwk.makeGraph(None, taskFactory, args)
            self.assertIsInstance(qgraph, QuantumGraph)
            self.assertEqual(len(qgraph), 0)

            # pickle with wrong object type
            with open(tmpname, "wb") as pickleFile:
                pickle.dump({}, pickleFile)
            args = _makeArgs(qgraph=tmpname)
            with self.assertRaises(TypeError):
                fwk.makeGraph(None, taskFactory, args)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
