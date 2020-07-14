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

import os
import tempfile
import unittest

from lsst.ctrl.mpexec.cli import script
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetaskCli
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner
from lsst.pipe.base import Pipeline
import lsst.utils.tests


class BuildTestCase(unittest.TestCase):
    """Test a few of the inputs to the build script function to test basic
    funcitonality."""

    def testMakeEmptyPipeline(self):
        """Test building a pipeline with default arguments.
        """
        pipeline = script.build()
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 0)

    def testSavePipeline(self):
        """Test pipeline serialization."""
        with tempfile.TemporaryDirectory() as tempdir:
            # make empty pipeline and store it in a file
            filename = os.path.join(tempdir, "pipeline")
            pipeline = script.build(save_pipeline=filename)
            self.assertIsInstance(pipeline, Pipeline)

            # read pipeline from a file
            pipeline = script.build(pipeline=filename)
            self.assertIsInstance(pipeline, Pipeline)
            self.assertIsInstance(pipeline, Pipeline)
            self.assertEqual(len(pipeline), 0)

    def testShowPipeline(self):
        """Test showing the pipeline."""
        class ShowInfo:
            def __init__(self, show, expectedOutput):
                self.show = show
                self.expectedOutput = expectedOutput

            def __repr__(self):
                return f"ShowInfo({self.show}, {self.expectedOutput}"

        testdata = [
            ShowInfo("pipeline", """description: anonymous
tasks:
  task:
    class: testUtil.AddTask
    config:
    - addend: '100'"""),
            ShowInfo("config", """### Configuration for task `task'
# Flag to enable/disable metadata saving for a task, enabled by default.
config.saveMetadata=True

# amount to add
config.addend=100

# name for connection input
config.connections.input='add_input'

# name for connection output
config.connections.output='add_output'

# name for connection initout
config.connections.initout='add_init_output'"""),

            # history will contain machine-specific paths, TBD how to verify
            ShowInfo("history=task::addend", None),
            ShowInfo("tasks", "### Subtasks for task `AddTask'")
        ]

        for showInfo in testdata:
            runner = LogCliRunner()
            result = runner.invoke(pipetaskCli, ["build",
                                                 "--task", "testUtil.AddTask:task",
                                                 "--config", "task:addend=100",
                                                 "--show", showInfo.show])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            if showInfo.expectedOutput is not None:
                self.assertIn(showInfo.expectedOutput, result.output, msg=f"for {showInfo}")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
