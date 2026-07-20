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

"""Unit tests for the ctrl_mpexec CLI run subcommand."""

import unittest
import unittest.mock

from lsst.ctrl.mpexec.cli.cmd import commands
from lsst.ctrl.mpexec.cli.pipetask import cli as pipetask_cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg


class RunCommandThreadingTest(unittest.TestCase):
    """Test implicit threading handling of "pipetask run"."""

    def setUp(self):
        self.runner = LogCliRunner()

    def _invoke_with_mocks(self, args: list[str]) -> tuple:
        """Invoke the CLI with the script functions replaced by mocks that
        share a common parent so that call order can be checked.
        """
        manager = unittest.mock.Mock()
        with (
            unittest.mock.patch.object(commands, "disable_implicit_threading", manager.disable),
            unittest.mock.patch.object(commands.script, "build", manager.build),
            unittest.mock.patch.object(commands.script, "qgraph", manager.qgraph),
            unittest.mock.patch.object(commands.script, "run", manager.run),
        ):
            result = self.runner.invoke(pipetask_cli, args)
        return result, manager

    def test_disable_before_graph_building(self):
        """Implicit threading must be disabled before the pipeline is built
        so that thread pools created during task imports and graph generation
        are already limited.
        """
        result, manager = self._invoke_with_mocks(["run", "-b", "repo"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        called = [name for name, _, _ in manager.mock_calls]
        self.assertIn("disable", called)
        self.assertLess(called.index("disable"), called.index("build"))

    def test_enable_implicit_threading(self):
        """No early disabling when implicit threading is requested."""
        result, manager = self._invoke_with_mocks(["run", "-b", "repo", "--enable-implicit-threading"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        called = [name for name, _, _ in manager.mock_calls]
        self.assertNotIn("disable", called)


if __name__ == "__main__":
    unittest.main()
