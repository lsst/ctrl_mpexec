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

"""Unit tests for ctrl_mpexec CLI update-graph-run subcommand."""

import os
import unittest

import yaml
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.ctrl.mpexec.cli.script.report import print_summary
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base.quantum_provenance_graph import Summary
from lsst.pipe.base.tests.simpleQGraph import makeSimpleQGraph
from lsst.pipe.base.tests.util import check_output_run
from yaml.loader import SafeLoader

TESTDIR = os.path.abspath(os.path.dirname(__file__))

expected_mock_datasets = [
    "add_dataset1",
    "add2_dataset1",
    "task0_metadata",
    "task0_log",
    "add_dataset2",
    "add2_dataset2",
    "task1_metadata",
    "task1_log",
    "add_dataset3",
    "add2_dataset3",
    "task2_metadata",
    "task2_log",
    "add_dataset4",
    "add2_dataset4",
    "task3_metadata",
    "task3_log",
    "add_dataset5",
    "add2_dataset5",
    "task4_metadata",
    "task4_log",
]


class ReportTest(unittest.TestCase):
    """Test executing "pipetask report" command."""

    def setUp(self) -> None:
        self.runner = LogCliRunner()
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_report(self):
        """Test for making a report on the produced, missing and expected
        datasets in a quantum graph.
        """
        metadata = {"output_run": "run"}
        butler, qgraph = makeSimpleQGraph(
            run="run",
            root=self.root,
            metadata=metadata,
        )
        # Check that we can get the proper run collection from the qgraph
        self.assertEqual(check_output_run(qgraph, "run"), [])

        graph_uri = os.path.join(self.root, "graph.qgraph")
        qgraph.saveUri(graph_uri)

        test_filename = os.path.join(self.root, "report_test.yaml")

        result = self.runner.invoke(
            pipetask_cli,
            ["report", self.root, graph_uri, "--full-output-filename", test_filename, "--no-logs"],
            input="no",
        )

        # Check that we can read from the command line
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Check that we can open and read the file produced by make_reports
        with open(test_filename) as f:
            report_output_dict = yaml.load(f, Loader=SafeLoader)

        self.assertIsNotNone(report_output_dict["task0"])
        self.assertIsNotNone(report_output_dict["task0"]["failed_quanta"])
        self.assertIsInstance(report_output_dict["task0"]["n_expected"], int)

        result_hr = self.runner.invoke(
            pipetask_cli,
            ["report", self.root, graph_uri, "--no-logs"],
            input="no",
        )

        # Check that we can read from the command line
        self.assertEqual(result_hr.exit_code, 0, clickResultMsg(result_hr))

        # Check that we get string output
        self.assertIsInstance(result_hr.stdout, str)

        # Check that task0 and the failed quanta for task0 exist in the string
        self.assertIn("task0", result_hr.stdout)
        self.assertIn("Failed", result_hr.stdout)
        self.assertIn("Expected", result_hr.stdout)
        self.assertIn("Succeeded", result_hr.stdout)

        # Check brief option for pipetask report
        result_brief = self.runner.invoke(
            pipetask_cli,
            ["report", self.root, graph_uri, "--no-logs", "--brief"],
            input="no",
        )
        self.assertIsInstance(result_brief.stdout, str)

        # Check that task0 and the failed quanta for task0 exist in the string
        self.assertIn("task0", result_brief.stdout)
        self.assertIn("Failed", result_brief.stdout)
        self.assertIn("Expected", result_brief.stdout)
        self.assertIn("Succeeded", result_brief.stdout)

        # Test cli for the QPG
        result_v2_terminal_out = self.runner.invoke(
            pipetask_cli,
            ["report", self.root, graph_uri, "--no-logs", "--force-v2"],
            input="no",
        )

        # Check that we can read from the command line
        self.assertEqual(result_v2_terminal_out.exit_code, 0, clickResultMsg(result_v2_terminal_out))

        # Check that we get string output
        self.assertIsInstance(result_v2_terminal_out.stdout, str)

        # Check that task0 and the quanta for task0 exist in the string
        self.assertIn("task0", result_v2_terminal_out.stdout)
        self.assertIn("Unknown", result_v2_terminal_out.stdout)
        self.assertIn("Successful", result_v2_terminal_out.stdout)
        self.assertIn("Blocked", result_v2_terminal_out.stdout)
        self.assertIn("Failed", result_v2_terminal_out.stdout)
        self.assertIn("Wonky", result_v2_terminal_out.stdout)
        self.assertIn("TOTAL", result_v2_terminal_out.stdout)
        self.assertIn("EXPECTED", result_v2_terminal_out.stdout)

        # Check that title from the error summary appears
        self.assertIn("Unsuccessful Datasets", result_v2_terminal_out.stdout)

        # Test cli for the QPG brief option
        result_v2_brief = self.runner.invoke(
            pipetask_cli,
            ["report", self.root, graph_uri, "--no-logs", "--force-v2", "--brief"],
            input="no",
        )

        # Check that we can read from the command line
        self.assertEqual(result_v2_brief.exit_code, 0, clickResultMsg(result_v2_brief))

        # Check that we get string output
        self.assertIsInstance(result_v2_brief.stdout, str)

        # Check that task0 and the quanta for task0 exist in the string
        self.assertIn("task0", result_v2_brief.stdout)
        self.assertIn("Unknown", result_v2_brief.stdout)
        self.assertIn("Successful", result_v2_brief.stdout)
        self.assertIn("Blocked", result_v2_brief.stdout)
        self.assertIn("Failed", result_v2_brief.stdout)
        self.assertIn("Wonky", result_v2_brief.stdout)
        self.assertIn("TOTAL", result_v2_brief.stdout)
        self.assertIn("EXPECTED", result_v2_brief.stdout)

        # Check that the full output option works
        test_filename_v2 = os.path.join(self.root, "report_test.json")
        result_v2_full = self.runner.invoke(
            pipetask_cli,
            [
                "report",
                self.root,
                graph_uri,
                "--no-logs",
                "--full-output-filename",
                test_filename_v2,
                "--force-v2",
            ],
            input="no",
        )

        self.assertEqual(result_v2_full.exit_code, 0, clickResultMsg(result_v2_full))
        # Check the "brief" output that prints to the terminal first:
        # Check that we get string output
        self.assertIsInstance(result_v2_full.stdout, str)

        # Check that task0 and the quanta for task0 exist in the string
        self.assertIn("task0", result_v2_full.stdout)
        self.assertIn("Unknown", result_v2_full.stdout)
        self.assertIn("Successful", result_v2_full.stdout)
        self.assertIn("Blocked", result_v2_full.stdout)
        self.assertIn("Failed", result_v2_full.stdout)
        self.assertIn("Wonky", result_v2_full.stdout)
        self.assertIn("TOTAL", result_v2_full.stdout)
        self.assertIn("EXPECTED", result_v2_full.stdout)

        # Then validate the full output json file:
        with open(test_filename_v2) as f:
            output = f.read()
            model = Summary.model_validate_json(output)
            # Below is the same set of tests as in `pipe_base`:
            for task_summary in model.tasks.values():
                self.assertEqual(task_summary.n_successful, 0)
                self.assertEqual(task_summary.n_blocked, 0)
                self.assertEqual(task_summary.n_unknown, 1)
                self.assertEqual(task_summary.n_expected, 1)
                self.assertListEqual(task_summary.failed_quanta, [])
                self.assertListEqual(task_summary.recovered_quanta, [])
                self.assertListEqual(task_summary.wonky_quanta, [])
                self.assertEqual(task_summary.n_wonky, 0)
                self.assertEqual(task_summary.n_failed, 0)
            for dataset_type_name, dataset_type_summary in model.datasets.items():
                self.assertListEqual(
                    dataset_type_summary.unsuccessful_datasets,
                    [{"instrument": "INSTR", "detector": 0}],
                )
                self.assertEqual(dataset_type_summary.n_visible, 0)
                self.assertEqual(dataset_type_summary.n_shadowed, 0)
                self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                self.assertEqual(dataset_type_summary.n_expected, 1)
                self.assertEqual(dataset_type_summary.n_cursed, 0)
                self.assertEqual(dataset_type_summary.n_unsuccessful, 1)
                self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                self.assertIn(dataset_type_name, expected_mock_datasets)
            match dataset_type_name:
                case name if name in ["add_dataset1", "add2_dataset1", "task0_metadata", "task0_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task0")
                case name if name in ["add_dataset2", "add2_dataset2", "task1_metadata", "task1_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task1")
                case name if name in ["add_dataset3", "add2_dataset3", "task2_metadata", "task2_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task2")
                case name if name in ["add_dataset4", "add2_dataset4", "task3_metadata", "task3_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task3")
                case name if name in ["add_dataset5", "add2_dataset5", "task4_metadata", "task4_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task4")

    def test_aggregate_reports(self):
        """Test `pipetask aggregate-reports` command. We make one
        `SimpleQgraph` and then fake a copy in a couple of different ways,
        making sure we can aggregate the similar graphs.
        """
        metadata = {"output_run": "run1"}
        butler, qgraph1 = makeSimpleQGraph(
            run="run",
            root=self.root,
            metadata=metadata,
        )

        # Check that we can get the proper run collection from the qgraph
        self.assertEqual(check_output_run(qgraph1, "run"), [])

        # Save the graph
        graph_uri_1 = os.path.join(self.root, "graph1.qgraph")
        qgraph1.saveUri(graph_uri_1)

        file1 = os.path.join(self.root, "report_test_1.json")
        file2 = os.path.join(self.root, "report_test_2.json")
        aggregate_file = os.path.join(self.root, "aggregate_report.json")

        report1 = self.runner.invoke(
            pipetask_cli,
            [
                "report",
                self.root,
                graph_uri_1,
                "--no-logs",
                "--full-output-filename",
                file1,
                "--force-v2",
            ],
            input="no",
        )

        self.assertEqual(report1.exit_code, 0, clickResultMsg(report1))
        # Now, copy the json output into a duplicate file and aggregate
        with open(file1, "r") as f:
            sum1 = Summary.model_validate_json(f.read())
            sum2 = sum1.model_copy(deep=True)
            print_summary(sum2, file2, brief=False)

            # Then use these file outputs as the inputs to aggregate reports:
            aggregate_report = self.runner.invoke(
                pipetask_cli,
                [
                    "aggregate-reports",
                    file1,
                    file2,
                    "--full-output-filename",
                    aggregate_file,
                ],
            )
            # Check that aggregate command had a zero exit code:
            self.assertEqual(aggregate_report.exit_code, 0, clickResultMsg(aggregate_report))

        # Check that it aggregates as expected:
        with open(aggregate_file) as f:
            agg_sum = Summary.model_validate_json(f.read())
            for task_label, task_summary in agg_sum.tasks.items():
                self.assertEqual(task_summary.n_successful, 0)
                self.assertEqual(task_summary.n_blocked, 0)
                self.assertEqual(task_summary.n_unknown, 2)
                self.assertEqual(task_summary.n_expected, 2)
                self.assertListEqual(task_summary.failed_quanta, [])
                self.assertListEqual(task_summary.recovered_quanta, [])
                self.assertListEqual(task_summary.wonky_quanta, [])
                self.assertEqual(task_summary.n_wonky, 0)
                self.assertEqual(task_summary.n_failed, 0)
            for dataset_type_name, dataset_type_summary in agg_sum.datasets.items():
                self.assertListEqual(
                    dataset_type_summary.unsuccessful_datasets,
                    [{"instrument": "INSTR", "detector": 0}, {"instrument": "INSTR", "detector": 0}],
                )
                self.assertEqual(dataset_type_summary.n_visible, 0)
                self.assertEqual(dataset_type_summary.n_shadowed, 0)
                self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                self.assertEqual(dataset_type_summary.n_expected, 2)
                self.assertEqual(dataset_type_summary.n_cursed, 0)
                self.assertEqual(dataset_type_summary.n_unsuccessful, 2)
                self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                self.assertIn(dataset_type_name, expected_mock_datasets)
                match dataset_type_name:
                    case name if name in ["add_dataset1", "add2_dataset1", "task0_metadata", "task0_log"]:
                        self.assertEqual(dataset_type_summary.producer, "task0")
                    case name if name in ["add_dataset2", "add2_dataset2", "task1_metadata", "task1_log"]:
                        self.assertEqual(dataset_type_summary.producer, "task1")
                    case name if name in ["add_dataset3", "add2_dataset3", "task2_metadata", "task2_log"]:
                        self.assertEqual(dataset_type_summary.producer, "task2")
                    case name if name in ["add_dataset4", "add2_dataset4", "task3_metadata", "task3_log"]:
                        self.assertEqual(dataset_type_summary.producer, "task3")
                    case name if name in ["add_dataset5", "add2_dataset5", "task4_metadata", "task4_log"]:
                        self.assertEqual(dataset_type_summary.producer, "task4")


if __name__ == "__main__":
    unittest.main()
