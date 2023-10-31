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
        """Test for making a report on the produced and missing expected
        datasets in a quantum graph. in a graph.
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
            ["report", self.root, graph_uri, test_filename, "--no-logs"],
            input="no",
        )

        # Check that we can read from the command line
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Check that we can open and read the file produced by make_reports
        with open(test_filename) as f:
            report_output_dict = yaml.load(f, Loader=SafeLoader)
        self.assertIsNotNone(report_output_dict["task0"])
        self.assertIsNotNone(report_output_dict["task0"]["failed_quanta"])


if __name__ == "__main__":
    unittest.main()
