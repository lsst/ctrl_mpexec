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

from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.tests.simpleQGraph import makeSimpleQGraph
from lsst.pipe.base.tests.util import check_output_run

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class UpdateGraphRunTest(unittest.TestCase):
    """Test executing "pipetask update-graph-run" commands."""

    instrument = "lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"

    def setUp(self) -> None:
        self.runner = LogCliRunner()
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_update(self):
        """Test for updating output run in a graph."""
        nQuanta = 3
        metadata = {"output_run": "run"}
        _, qgraph = makeSimpleQGraph(
            nQuanta,
            run="run",
            root=self.root,
            instrument=self.instrument,
            metadata=metadata,
        )
        self.assertEqual(check_output_run(qgraph, "run"), [])

        old_path = os.path.join(self.root, "graph.qgraph")
        qgraph.saveUri(old_path)

        new_path = os.path.join(self.root, "graph-updated.qgraph")
        result = self.runner.invoke(
            pipetask_cli,
            ["update-graph-run", old_path, "new-run", new_path],
            input="no",
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        updated_graph = QuantumGraph.loadUri(new_path)
        self.assertEqual(check_output_run(updated_graph, "new-run"), [])
        assert updated_graph.metadata is not None
        self.assertEqual(updated_graph.metadata["output_run"], "new-run")
        self.assertEqual(updated_graph.graphID, qgraph.graphID)

        # Check that we can turn off metadata updates.
        result = self.runner.invoke(
            pipetask_cli,
            ["update-graph-run", "--metadata-run-key=''", old_path, "new-run2", new_path],
            input="no",
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        updated_graph = QuantumGraph.loadUri(new_path)
        self.assertEqual(check_output_run(updated_graph, "new-run2"), [])
        assert updated_graph.metadata is not None
        self.assertEqual(updated_graph.metadata["output_run"], "run")

        # Now check that we can make new graph ID.
        result = self.runner.invoke(
            pipetask_cli,
            ["update-graph-run", "--update-graph-id", old_path, "new-run3", new_path],
            input="no",
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        updated_graph = QuantumGraph.loadUri(new_path)
        self.assertEqual(check_output_run(updated_graph, "new-run3"), [])
        self.assertNotEqual(updated_graph.graphID, qgraph.graphID)


if __name__ == "__main__":
    unittest.main()
