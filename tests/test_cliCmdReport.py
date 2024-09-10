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
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base.quantum_provenance_graph import DatasetTypeSummary, Summary, TaskSummary
from lsst.pipe.base.tests.simpleQGraph import makeSimpleQGraph
from lsst.pipe.base.tests.util import check_output_run
from yaml.loader import SafeLoader

TESTDIR = os.path.abspath(os.path.dirname(__file__))


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

        with open("delete_me.yaml", "w") as f:
            yaml.safe_dump(report_output_dict, f)

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
            self.assertDictEqual(
                model.tasks,
                {
                    "task0": TaskSummary(
                        n_successful=0,
                        n_blocked=0,
                        n_unknown=1,
                        n_expected=1,
                        failed_quanta=[],
                        recovered_quanta=[],
                        wonky_quanta=[],
                        n_wonky=0,
                        n_failed=0,
                    ),
                    "task1": TaskSummary(
                        n_successful=0,
                        n_blocked=0,
                        n_unknown=1,
                        n_expected=1,
                        failed_quanta=[],
                        recovered_quanta=[],
                        wonky_quanta=[],
                        n_wonky=0,
                        n_failed=0,
                    ),
                    "task2": TaskSummary(
                        n_successful=0,
                        n_blocked=0,
                        n_unknown=1,
                        n_expected=1,
                        failed_quanta=[],
                        recovered_quanta=[],
                        wonky_quanta=[],
                        n_wonky=0,
                        n_failed=0,
                    ),
                    "task3": TaskSummary(
                        n_successful=0,
                        n_blocked=0,
                        n_unknown=1,
                        n_expected=1,
                        failed_quanta=[],
                        recovered_quanta=[],
                        wonky_quanta=[],
                        n_wonky=0,
                        n_failed=0,
                    ),
                    "task4": TaskSummary(
                        n_successful=0,
                        n_blocked=0,
                        n_unknown=1,
                        n_expected=1,
                        failed_quanta=[],
                        recovered_quanta=[],
                        wonky_quanta=[],
                        n_wonky=0,
                        n_failed=0,
                    ),
                },
            )
            self.assertDictEqual(
                model.datasets,
                {
                    "add_dataset1": DatasetTypeSummary(
                        producer="task0",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add2_dataset1": DatasetTypeSummary(
                        producer="task0",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task0_metadata": DatasetTypeSummary(
                        producer="task0",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task0_log": DatasetTypeSummary(
                        producer="task0",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add_dataset2": DatasetTypeSummary(
                        producer="task1",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add2_dataset2": DatasetTypeSummary(
                        producer="task1",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task1_metadata": DatasetTypeSummary(
                        producer="task1",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task1_log": DatasetTypeSummary(
                        producer="task1",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add_dataset3": DatasetTypeSummary(
                        producer="task2",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add2_dataset3": DatasetTypeSummary(
                        producer="task2",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task2_metadata": DatasetTypeSummary(
                        producer="task2",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task2_log": DatasetTypeSummary(
                        producer="task2",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add_dataset4": DatasetTypeSummary(
                        producer="task3",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add2_dataset4": DatasetTypeSummary(
                        producer="task3",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task3_metadata": DatasetTypeSummary(
                        producer="task3",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task3_log": DatasetTypeSummary(
                        producer="task3",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add_dataset5": DatasetTypeSummary(
                        producer="task4",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "add2_dataset5": DatasetTypeSummary(
                        producer="task4",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task4_metadata": DatasetTypeSummary(
                        producer="task4",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                    "task4_log": DatasetTypeSummary(
                        producer="task4",
                        n_visible=0,
                        n_shadowed=0,
                        n_predicted_only=0,
                        n_expected=1,
                        cursed_datasets=[],
                        unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
                        n_cursed=0,
                        n_unsuccessful=1,
                    ),
                },
            )


if __name__ == "__main__":
    unittest.main()
