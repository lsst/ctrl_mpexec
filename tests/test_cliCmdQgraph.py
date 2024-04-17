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
import unittest

from lsst.ctrl.mpexec import Report
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base.tests.simpleQGraph import makeSimpleQGraph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QgraphTest(unittest.TestCase):
    """Test executing "pipetask qgraph" command."""

    def setUp(self) -> None:
        self.runner = LogCliRunner()
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_qgraph_summary(self):
        """Test for making a summary of a QuantumGraph."""
        metadata = {"output_run": "run"}
        butler, qgraph = makeSimpleQGraph(
            run="run",
            root=self.root,
            metadata=metadata,
        )

        graph_uri = os.path.join(self.root, "graph.qgraph")
        qgraph.saveUri(graph_uri)

        test_filename = os.path.join(self.root, "summary.json")

        result = self.runner.invoke(
            pipetask_cli,
            ["qgraph", "--butler-config", self.root, "--qgraph", graph_uri, "--summary", test_filename],
            input="no",
        )
        # Check that we can read from the command line
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Check that we can open and read the file produced by make_reports
        with open(test_filename) as f:
            summary = Report.model_validate_json(f.read())
        self.assertEqual(summary.qgraphSummary.outputRun, "run")
        self.assertEqual(len(summary.qgraphSummary.qgraphTaskSummaries), 5)


if __name__ == "__main__":
    unittest.main()
