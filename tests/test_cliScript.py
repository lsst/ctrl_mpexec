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

import unittest
import unittest.mock

import click

import lsst.utils.tests
from lsst.ctrl.mpexec.cli import opt, script
from lsst.ctrl.mpexec.cli.cmd.commands import coverage_context


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
