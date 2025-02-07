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

"""Unit tests for ctrl_mpexec CLI cleanup subcommand."""

import os
import unittest

from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.daf.butler import Butler
from lsst.daf.butler.cli.butler import cli as butler_cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import MetricTestRepo, makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class CleanupCollectionTest(unittest.TestCase):
    """Test executing "pipetask cleanup" commands."""

    def setUp(self):
        self.runner = LogCliRunner()

        # this creates a repo with collections:
        #    Name     Type
        # ---------- ------
        # ingest     TAGGED
        # ingest/run RUN
        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(
            self.root,
            configFile=os.path.join(TESTDIR, "config/metricTestRepoButler.yaml"),
        )

    def tearDown(self):
        removeTestTempDir(self.root)

    def test_cleanup_yesNo(self):
        """Test cleaning up a non-chained collection and that other collections
        in the chain remian. Verify the yes/no dialog works.
        """
        # add the collection ingest/run to a CHAINED collection called "in"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "in", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # clean up the CHAINED collection "in", verify that the TAGGED
        # collection "ingest" is removed, but the child RUN collection
        # "ingest/run" is not removed.
        result = self.runner.invoke(pipetask_cli, ["cleanup", "-b", self.root, "in"], input="n")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn("Will remove:\n  runs: \n  others: ingest\n", result.output)
        self.assertIn("Aborted.", result.output)

        # run cleanup again but say "y" to continue, and check for expected
        # outputs.
        result = self.runner.invoke(pipetask_cli, ["cleanup", "-b", self.root, "in"], input="y")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn("Will remove:\n  runs: \n  others: ingest\n", result.output)
        self.assertIn("Done.", result.output)

        butler = Butler.from_config(self.root)
        self.assertEqual(set(butler.registry.queryCollections()), {"in", "ingest/run"})

    def test_nonExistantCollection(self):
        """Test running cleanup on a collection that has never existed."""
        result = self.runner.invoke(pipetask_cli, ["cleanup", "-b", self.root, "foo"])
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))
        self.assertIn('Did not find a collection named "foo"', result.output)

    def test_removedCollection(self):
        """Test cleaning up a collection that used to exist."""
        # Remove the TAGGED collection called "ingest"
        result = self.runner.invoke(butler_cli, ["remove-collections", self.root, "ingest"], input="y")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Add the collection ingest/run to a CHAINED collection called "in"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "in", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        # Purge the collection
        result = self.runner.invoke(pipetask_cli, ["purge", "-b", self.root, "in"], input="no")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Attempt to clean up the collection, expect a message that there was
        # nothing to do.
        result = self.runner.invoke(pipetask_cli, ["cleanup", "-b", self.root, "in"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn("Did not find any collections to remove.", result.output)

    def test_cleanupNonChained(self):
        result = self.runner.invoke(pipetask_cli, ["cleanup", "-b", self.root, "ingest"])
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))
        self.assertIn(
            'Error: COLLECTION must be a CHAINED collection, "ingest" is a "TAGGED" collection.',
            result.output,
        )


if __name__ == "__main__":
    unittest.main()
