# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for ctrl_mpexec CLI qgraph subcommand."""

import os
import textwrap
import unittest

import click.testing

from lsst.ctrl.mpexec.cli import opt, script
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.pipe.base.quantum_reports import Report
from lsst.pipe.base.tests.mocks import DirectButlerRepo


class QgraphTest(unittest.TestCase):
    """Tests for the "pipetask qgraph" command.

    Note that significant coverage for the qgraph command is also provided by
    test_run.py, since those tests generally build a quantum graph and then run
    it.
    """

    def test_qgraph_summary(self):
        """Test reading a saved graph and writing a summary."""
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.add_task()
            qgc = helper.make_quantum_graph_builder().finish(attach_datastore_records=False)
            graph_uri = os.path.join(root, "graph.qg")
            qgc.write(graph_uri)
            test_filename = os.path.join(root, "summary.json")
            runner = LogCliRunner()
            result = runner.invoke(
                pipetask_cli,
                ["qgraph", "--butler-config", root, "--qgraph", graph_uri, "--summary", test_filename],
                input="no",
            )
            # Check that we can read from the command line
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # Check that we can open and read the file produced by make_reports
            with open(test_filename) as f:
                summary = Report.model_validate_json(f.read())
            self.assertEqual(summary.qgraphSummary.outputRun, "output_run")
            self.assertEqual(len(summary.qgraphSummary.qgraphTaskSummaries), 2)

    def test_partial_read_counts(self):
        """Test reading only one node from a saved graph and check that the
        printed task/quantum counts reflect only that node.
        """
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.add_task()
            qgc = helper.make_quantum_graph_builder().finish(attach_datastore_records=False)
            quantum_id = next(iter(qgc.quantum_datasets.keys()))
            graph_uri = os.path.join(root, "graph.qg")
            qgc.write(graph_uri)
            log_filename = os.path.join(root, "qgraph.log")
            runner = LogCliRunner()
            result = runner.invoke(
                pipetask_cli,
                [
                    "--log-level",
                    "lsst.ctrl.mpexec.cli.utils=INFO",
                    "--log-file",
                    log_filename,
                    "qgraph",
                    "--butler-config",
                    root,
                    "--qgraph",
                    graph_uri,
                    "--qgraph-node-id",
                    str(quantum_id),
                ],
                input="no",
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            with open(log_filename) as log_file:
                log = log_file.read()
            self.assertIn("1 quantum for 1 task", log)

    def test_qgraph_show(self):
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.add_task()
            qgc = helper.make_quantum_graph_builder().finish(attach_datastore_records=False)
            graph_uri = os.path.join(root, "graph.qg")
            qgc.write(graph_uri)
            runner = LogCliRunner()
            result = runner.invoke(
                pipetask_cli,
                ["qgraph", "--butler-config", root, "--qgraph", graph_uri, "--show", "graph"],
                input="no",
            )
            self.assertIn(
                "task_auto1 (lsst.pipe.base.tests.mocks.DynamicTestPipelineTask)",
                result.output,
            )
            self.assertIn(
                "task_auto2 (lsst.pipe.base.tests.mocks.DynamicTestPipelineTask)",
                result.output,
            )
            self.assertIn("DatasetType('dataset_auto0', {}, _mock_StructuredDataDict)", result.output)
            self.assertIn("DatasetType('dataset_auto1', {}, _mock_StructuredDataDict)", result.output)
            self.assertIn("DatasetType('dataset_auto2', {}, _mock_StructuredDataDict)", result.output)
            result = runner.invoke(
                pipetask_cli,
                ["qgraph", "--butler-config", root, "--qgraph", graph_uri, "--show", "workflow"],
                input="no",
            )
            id1, id2 = qgc.quantum_datasets.keys()
            self.assertEqual(
                result.output.strip(),
                textwrap.dedent(f"""
                    Quantum {id1}: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
                    Quantum {id2}: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
                    Parent Quantum {id1} - Child Quantum {id2}
                """).strip(),
            )
            result = runner.invoke(
                pipetask_cli,
                ["qgraph", "--butler-config", root, "--qgraph", graph_uri, "--show", "uri"],
                input="no",
            )
            self.assertIn(
                f"Quantum {id1}: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask",
                result.output,
            )
            self.assertIn(
                f"Quantum {id2}: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask",
                result.output,
            )

    def test_missing_option(self):
        """Test that if options for the qgraph script are missing that it
        fails.
        """

        @click.command()
        @opt.qgraph_options()
        def cli(**kwargs):
            script.qgraph(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(cli)
        # The cli call should fail, because qgraph takes more options than are
        # defined by qgraph_options.
        self.assertNotEqual(result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
