# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
import unittest.mock

import click

import lsst.utils.tests
from lsst.ctrl.mpexec.cli import opt, script
from lsst.ctrl.mpexec.cli.cmd.commands import coverage_context
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetaskCli
from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.pipe.base import Pipeline


class BuildTestCase(unittest.TestCase):
    """Test a few of the inputs to the build script function to test basic
    funcitonality.
    """

    @staticmethod
    def buildArgs(**kwargs):
        defaultArgs = dict(
            log_level=(),
            order_pipeline=False,
            pipeline=None,
            pipeline_actions=(),
            pipeline_dot=None,
            pipeline_mermaid=None,
            save_pipeline=None,
            show=ShowInfo([]),
        )
        defaultArgs.update(kwargs)
        return defaultArgs

    def testMakeEmptyPipeline(self):
        """Test building a pipeline with default arguments."""
        pipeline_graph_factory = script.build(**self.buildArgs())
        self.assertIsInstance(pipeline_graph_factory.pipeline, Pipeline)
        self.assertEqual(len(pipeline_graph_factory.pipeline), 0)

    def testSavePipeline(self):
        """Test pipeline serialization."""
        with tempfile.TemporaryDirectory() as tempdir:
            # make empty pipeline and store it in a file
            filename = os.path.join(tempdir, "pipeline_file.yaml")
            pipeline_graph_factory = script.build(**self.buildArgs(save_pipeline=filename))
            self.assertIsInstance(pipeline_graph_factory.pipeline, Pipeline)
            self.assertTrue(os.path.isfile(filename))
            # read pipeline from a file
            pipeline_graph_factory = script.build(**self.buildArgs(pipeline=filename))
            self.assertIsInstance(pipeline_graph_factory.pipeline, Pipeline)
            self.assertEqual(len(pipeline_graph_factory.pipeline), 0)

    def testShowPipeline(self):
        """Test showing the pipeline."""

        class ShowInfoCmp:
            def __init__(self, show, expectedOutput, *args):
                self.show = show
                self.expectedOutput = expectedOutput
                self.args = args

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
            ShowInfoCmp(
                "pipeline-graph",
                "\n".join(
                    [
                        "○  add_dataset_in: {detector} NumpyArray",
                        "│",
                        "■  task2: {detector}",
                        "│",
                        "◍  add_dataset_out2, add2_dataset_out2: {detector} NumpyArray",
                    ]
                ),
                "--task",
                "lsst.pipe.base.tests.simpleQGraph.AddTask:task2",
                "-c",
                "task2:connections.out_tmpl=_out2",
                "--select-tasks",
                "<=add2_dataset_out2",
            ),
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
                    *showInfo.args,
                ],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            if showInfo.expectedOutput is not None:
                self.assertIn(showInfo.expectedOutput, result.output, msg=f"for {showInfo}")
        # Trying to show the pipeline with --select-tasks should fail, because
        # --select-tasks acts on the PipelineGraph and hence wouldn't affect
        # the YAML and that'd be confusing.
        runner = LogCliRunner()
        result = runner.invoke(
            pipetaskCli,
            [
                "build",
                "--task",
                "lsst.pipe.base.tests.simpleQGraph.AddTask:task",
                "--show",
                "pipeline",
                "--select-tasks",
                ">=task",
            ],
        )
        self.assertEqual(result.exit_code, 1)

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
    """Test pipetask qgraph command-line."""

    def testMissingOption(self):
        """Test that if options for the qgraph script are missing that it
        fails.
        """

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
    """Test pipetask run command-line."""

    def testMissingOption(self):
        """Test that if options for the run script are missing that it
        fails.
        """

        @click.command()
        @opt.pipeline_build_options()
        def cli(**kwargs):
            script.run(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(cli)
        # The cli call should fail, because qgraph.run takes more options
        # than are defined by pipeline_build_options.
        self.assertNotEqual(result.exit_code, 0)


class CoverageTestCase(unittest.TestCase):
    """Test coverage context manager."""

    @unittest.mock.patch.dict("sys.modules", coverage=unittest.mock.MagicMock())
    def testWithCoverage(self):
        """Test that the coverage context manager runs when invoked."""
        with coverage_context({"coverage": True}):
            self.assertTrue(True)

    @unittest.mock.patch("lsst.ctrl.mpexec.cli.cmd.commands.import_module", side_effect=ModuleNotFoundError())
    def testWithMissingCoverage(self, mock_import):  # numpydoc ignore=PR01
        """Test that the coverage context manager complains when coverage is
        not available.
        """
        with self.assertRaises(click.exceptions.ClickException):
            with coverage_context({"coverage": True}):
                pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
