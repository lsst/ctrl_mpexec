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

import click
import lsst.utils.tests
from lsst.ctrl.mpexec.cli import opt, script
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetaskCli
from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.pipe.base import Pipeline


class BuildTestCase(unittest.TestCase):
    """Test a few of the inputs to the build script function to test basic
    funcitonality."""

    @staticmethod
    def buildArgs(**kwargs):
        defaultArgs = dict(
            log_level=(),
            order_pipeline=False,
            pipeline=None,
            pipeline_actions=(),
            pipeline_dot=None,
            save_pipeline=None,
            show=ShowInfo([]),
        )
        defaultArgs.update(kwargs)
        return defaultArgs

    def testMakeEmptyPipeline(self):
        """Test building a pipeline with default arguments."""
        pipeline = script.build(**self.buildArgs())
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 0)

    def testSavePipeline(self):
        """Test pipeline serialization."""
        with tempfile.TemporaryDirectory() as tempdir:
            # make empty pipeline and store it in a file
            filename = os.path.join(tempdir, "pipeline")
            pipeline = script.build(**self.buildArgs(filename=filename))
            self.assertIsInstance(pipeline, Pipeline)

            # read pipeline from a file
            pipeline = script.build(**self.buildArgs(filename=filename))
            self.assertIsInstance(pipeline, Pipeline)
            self.assertIsInstance(pipeline, Pipeline)
            self.assertEqual(len(pipeline), 0)

    def testShowPipeline(self):
        """Test showing the pipeline."""

        class ShowInfoCmp:
            def __init__(self, show, expectedOutput):
                self.show = show
                self.expectedOutput = expectedOutput

            def __repr__(self):
                return f"ShowInfoCmp({self.show}, {self.expectedOutput}"

        testdata = [
            ShowInfoCmp(
                "pipeline",
                """description: anonymous
tasks:
  task:
    class: lsst.pipe.base.tests.simpleQGraph.AddTask
    config:
    - addend: '100'""",
            ),
            ShowInfoCmp(
                "config",
                """### Configuration for task `task'
# Flag to enable/disable metadata saving for a task, enabled by default.
config.saveMetadata=True

# Flag to enable/disable saving of log output for a task, enabled by default.
config.saveLogOutput=True

# amount to add
config.addend=100

# name for connection input
config.connections.input='add_dataset{in_tmpl}'

# name for connection output
config.connections.output='add_dataset{out_tmpl}'

# name for connection output2
config.connections.output2='add2_dataset{out_tmpl}'

# name for connection initout
config.connections.initout='add_init_output{out_tmpl}'

# Template parameter used to format corresponding field template parameter
config.connections.in_tmpl='_in'

# Template parameter used to format corresponding field template parameter
config.connections.out_tmpl='_out'""",
            ),
            # history will contain machine-specific paths, TBD how to verify
            ShowInfoCmp("history=task::addend", None),
            ShowInfoCmp("tasks", "### Subtasks for task `lsst.pipe.base.tests.simpleQGraph.AddTask'"),
        ]

        for showInfo in testdata:
            runner = LogCliRunner()
            result = runner.invoke(
                pipetaskCli,
                [
                    "build",
                    "--task",
                    "lsst.pipe.base.tests.simpleQGraph.AddTask:task",
                    "--config",
                    "task:addend=100",
                    "--show",
                    showInfo.show,
                ],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            if showInfo.expectedOutput is not None:
                self.assertIn(showInfo.expectedOutput, result.output, msg=f"for {showInfo}")

    def testMissingOption(self):
        """Test that the build script fails if options are missing."""

        @click.command()
        @opt.pipeline_build_options()
        def cli(**kwargs):
            script.build(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(cli)
        # The cli call should fail, because script.build takes more options
        # than are defined by pipeline_build_options.
        self.assertNotEqual(result.exit_code, 0)


class QgraphTestCase(unittest.TestCase):
    def testMissingOption(self):
        """Test that if options for the qgraph script are missing that it
        fails."""

        @click.command()
        @opt.pipeline_build_options()
        def cli(**kwargs):
            script.qgraph(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(cli)
        # The cli call should fail, because qgraph.build takes more options
        # than are defined by pipeline_build_options.
        self.assertNotEqual(result.exit_code, 0)


class RunTestCase(unittest.TestCase):
    def testMissingOption(self):
        """Test that if options for the run script are missing that it
        fails."""

        @click.command()
        @opt.pipeline_build_options()
        def cli(**kwargs):
            script.run(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(cli)
        # The cli call should fail, because qgraph.run takes more options
        # than are defined by pipeline_build_options.
        self.assertNotEqual(result.exit_code, 0)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
