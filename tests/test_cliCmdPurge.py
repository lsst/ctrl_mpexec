# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for ctrl_mpexec CLI purge subcommand.
"""


import os
import unittest

from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.daf.butler.cli.butler import cli as butler_cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import MetricTestRepo, makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PurgeTest(unittest.TestCase):
    """Test executing "pipetask purge" commands."""

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

    def test_singleChain_yesNo(self):
        """Test removing a chain with one child, and the yes/no
        confirmation."""
        # add the collection ingest/run to a CHAINED collection called "in"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "in", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # purge the CHAINED collection called "in", but say "no", check for
        # expected outputs.
        result = self.runner.invoke(pipetask_cli, ["purge", "-b", self.root, "in"], input="no")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn("Will remove:\n  runs: ingest/run\n  chains: in\n  others: \n", result.output)
        self.assertIn("Aborted.", result.output)

        # purge the CHAINED collection called "in", and say "yes", check for
        # expected outputs.
        result = self.runner.invoke(pipetask_cli, ["purge", "-b", self.root, "in"], input="yes")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn("Will remove:\n  runs: ingest/run\n  chains: in\n  others: \n", result.output)
        self.assertIn("Done.", result.output)

    def test_granparentChain_noConfirm(self):
        """Test removing a chain with children and grandchildren, and the
        --no-confirm option.
        """

        # add the collection ingest/run to a CHAINED collection called "ing"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "ing", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # add the CHAINED collectin "ing" a CHAINED collection called "in"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "in", "ing"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # purge the CHAINED collection called "in" with --no-confirm and check
        # for expected outputs.
        result = self.runner.invoke(
            pipetask_cli,
            ["purge", "-b", self.root, "in", "--recursive", "--no-confirm"],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn("Removed:\n  runs: ingest/run\n  chains: in, ing\n  others: \n", result.output)

    def test_topParentWithParent(self):
        """Test that purging a chain with a parent fails."""

        # add the collection ingest/run to a CHAINED collection called "ing"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "ing", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # add the CHAINED collectin "ing" a CHAINED collection called "in"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "in", "ing"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # purge the CHAINED collection called "ing" and check for expected
        # outputs.
        result = self.runner.invoke(
            pipetask_cli,
            ["purge", "-b", self.root, "ing"],
        )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))
        self.assertIn(
            'The passed-in collection "ing" must not be contained in other collections but '
            'is contained in collection(s) "in".',
            result.output,
        )

    def test_childWithMultipleParents(self):
        """Test that a child chain with multiple parents fails."""
        # add the collection ingest/run to a CHAINED collection called "ing"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "ing", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # add the collectin ingest/run to a CHAINED collection called "foo"
        result = self.runner.invoke(butler_cli, ["collection-chain", self.root, "foo", "ingest/run"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # purge the CHAINED collection called "ing" and check for expected
        # outputs.
        result = self.runner.invoke(
            pipetask_cli,
            ["purge", "-b", self.root, "ing"],
        )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))
        self.assertIn(
            'Collection "ingest/run" is in multiple chained collections:',
            result.output,
        )
        self.assertIn('"foo"', result.output)
        self.assertIn('"ing"', result.output)

    def test_notFound_notChained(self):
        """Test for failure when the top level collection is not found,
        and when a top level connection is not a CHAINED collection.
        """
        # Test purging a collection that does not exist.
        result = self.runner.invoke(
            pipetask_cli,
            ["purge", "-b", self.root, "notACollection"],
        )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))
        self.assertIn(
            'The passed-in colleciton "notACollection" was not found.',
            result.output,
        )

        # Test purging a colleciton that is not a CHAINED collection.
        result = self.runner.invoke(
            pipetask_cli,
            ["purge", "-b", self.root, "ingest/run"],
        )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))
        self.assertIn(
            'The passed-in collection must be a CHAINED collection; "ingest/run" is a RUN collection.',
            result.output,
        )


if __name__ == "__main__":
    unittest.main()
