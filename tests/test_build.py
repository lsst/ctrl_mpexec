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

import contextlib
import os
import tempfile
import textwrap
import unittest
import unittest.mock

import click.testing

import lsst.utils.tests
from lsst.ctrl.mpexec.cli import opt, script
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetaskCli
from lsst.ctrl.mpexec.cli.utils import (
    PipetaskCommand,
    collect_pipeline_actions,
)
from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.pipe.base import Pipeline


# I am not positive this adds value vs. `tempfile.NamedTemporaryFile`, but
# given the trouble we've had with temporary file/directory cleanup edge cases
# in CI, I'm not inclined to remove it (it was lifted from test_cmdLineFwk.py,
# which is going away).
@contextlib.contextmanager
def make_tmp_file(contents=None, suffix=None):
    """Context manager for generating temporary file name.

    Temporary file is deleted on exiting context.

    Parameters
    ----------
    contents : `bytes` or `None`, optional
        Data to write into a file.
    suffix : `str` or `None`, optional
        Suffix to use for temporary file.

    Yields
    ------
    `str`
        Name of the temporary file.
    """
    fd, tmpname = tempfile.mkstemp(suffix=suffix)
    if contents:
        os.write(fd, contents)
    os.close(fd)
    yield tmpname
    with contextlib.suppress(OSError):
        os.remove(tmpname)


class BuildTestCase(unittest.TestCase):
    """Test a few of the inputs to the build script function to test basic
    functionality.
    """

    @staticmethod
    def _make_args(*args: str) -> dict[str, object]:
        mock = unittest.mock.Mock()

        @click.command(cls=PipetaskCommand)
        @click.pass_context
        @opt.pipeline_build_options()
        def fake_build(ctx: click.Context, **kwargs: object):
            kwargs = collect_pipeline_actions(ctx, **kwargs)
            mock(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(fake_build, args, catch_exceptions=False)
        if result.exit_code != 0:
            raise RuntimeError(f"Failure getting default args for 'build': {result}")
        mock.assert_called_once()
        result: dict[str, object] = mock.call_args[1]
        result["show"] = ShowInfo([])
        return result

    def test_make_empty_pipeline(self):
        """Test building a pipeline with default arguments, saving it, and
        reading it.
        """
        with make_tmp_file() as tmpname:
            # make empty pipeline and store it in a file
            pgf = script.build(**self._make_args("--save-pipeline", tmpname))
            self.assertIsInstance(pgf.pipeline, Pipeline)
            # read pipeline from a file
            pgf = script.build(**self._make_args("--pipeline", tmpname))
            self.assertIsInstance(pgf.pipeline, Pipeline)
            self.assertEqual(len(pgf.pipeline), 0)

    def test_single_task_pipeline(self):
        """Test building a pipeline with a single task."""
        pgf = script.build(**self._make_args("-t", "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask:a"))
        self.assertIsInstance(pgf.pipeline, Pipeline)
        self.assertEqual(len(pgf.pipeline), 1)

    def test_multi_task_pipeline(self):
        """Test building a pipeline with multiple tasks."""
        pgf = script.build(
            **self._make_args(
                "-t",
                "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask:a",
                "-t",
                "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask:b",
                "-t",
                "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask:c",
            )
        )
        self.assertIsInstance(pgf.pipeline, Pipeline)
        self.assertEqual(len(pgf.pipeline), 3)
        self.assertEqual(pgf.pipeline.task_labels, {"a", "b", "c"})

    def test_config(self):
        """Test building a pipeline with a config override."""
        pgf = script.build(
            **self._make_args(
                "-t",
                "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask:a",
                "-c",
                "a:int_value=100",
            )
        )
        pipeline_graph = pgf()
        self.assertEqual(len(pipeline_graph.tasks), 1)
        self.assertEqual(next(iter(pipeline_graph.tasks.values())).config.int_value, 100)

    def test_config_file(self):
        """Test building a pipeline with a config file override."""
        overrides = b"config.int_value = 1000\n"
        with make_tmp_file(overrides) as tmpname:
            pgf = script.build(
                **self._make_args(
                    "-t",
                    "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask:a",
                    "-C",
                    f"a:{tmpname}",
                )
            )
            pipeline_graph = pgf()
            self.assertEqual(len(pipeline_graph.tasks), 1)
            self.assertEqual(next(iter(pipeline_graph.tasks.values())).config.int_value, 1000)

    def test_build_show(self):
        """Test the --show option on the build subcommand."""
        # This pipeline YAML snippet is formatted exactly the way
        # '--show pipeline' writes it back out.
        pipeline_data = textwrap.dedent("""
          description: single-task test pipeline
          tasks:
            t:
              class: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
              config:
              - python: |-
                  from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                  config.inputs["a"] = DynamicConnectionConfig(dataset_type_name="d1")
                  config.outputs["b"] = DynamicConnectionConfig(dataset_type_name="d2")
                int_value: 100
          subsets:
            test_subset:
              subset:
              - t
        """).strip()
        runner = LogCliRunner()
        with make_tmp_file(pipeline_data.encode()) as pipeline_file:
            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "pipeline"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(pipeline_data, result.output)

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "config"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("### Configuration for task `t'", result.output)
            self.assertIn("config.int_value=100", result.output)
            self.assertIn("config.inputs['a'].dataset_type_name='d1'", result.output)

            result = runner.invoke(
                pipetaskCli, ["build", "-p", pipeline_file, "--show", "history=t::int_value"]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # history will contain machine-specific paths, TBD how to verify

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "pipeline-graph"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(
                textwrap.dedent("""
                    ○  d1: {} _mock_StructuredDataDict
                    │
                    ■  t: {}
                    │
                    ○  d2: {} _mock_StructuredDataDict
                    """).strip(),
                result.output,
            )

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "task-graph"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("■  t: {}", result.output)

            # Trying to show the pipeline with --select-tasks should fail,
            # because --select-tasks acts on the PipelineGraph and hence
            # wouldn't affect the YAML and that'd be confusing.
            result = runner.invoke(
                pipetaskCli,
                [
                    "build",
                    "-p",
                    pipeline_file,
                    "--show",
                    "pipeline",
                    "--select-tasks",
                    ">=task",
                ],
            )
            self.assertEqual(result.exit_code, 1)

            result = runner.invoke(
                pipetaskCli, ["build", "-p", pipeline_file, "--show", "config=t::int_Value:NOIGNORECASE"]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertEqual(
                "### Configuration for task `t'", result.output.strip()
            )  # No match for the field.

            result = runner.invoke(
                pipetaskCli, ["build", "-p", pipeline_file, "--show", "config=t::int_Value"]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("### Configuration for task `t'", result.output)  # Matches...
            self.assertIn("config.int_value=100", result.output)
            self.assertIn("NOIGNORECASE", result.output)  # ...but warns and recommends NOIGNORECASE

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "dump-config=b"])
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Pipeline has no tasks named b", result.output)

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "history"])
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Please provide a value", result.output)

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "history=b::param"])
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Pipeline has no tasks named b", result.output)

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "subsets"])
            self.assertEqual(result.exit_code, 0)
            self.assertIn("test_subset", result.output)

            result = runner.invoke(pipetaskCli, ["build", "-p", pipeline_file, "--show", "tasks"])
            self.assertEqual(result.exit_code, 0)
            self.assertIn(
                "### Subtasks for task `lsst.pipe.base.tests.mocks.DynamicTestPipelineTask'", result.output
            )

    def test_show_unrecognized(self):
        """Test that ShowInfo raises when given unrecognized commands."""
        with self.assertRaises(ValueError):
            ShowInfo(["unrecognized", "config"])

    def test_missing_option(self):
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


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
