# This file is part of daf_butler.
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

import unittest
import unittest.mock

from lsst.daf.butler.tests import CliCmdTestBase
from lsst.ctrl.mpexec.cli.pipetask import cli
from lsst.ctrl.mpexec.cli.cmd import build
from lsst.ctrl.mpexec.cmdLineParser import _PipelineAction


class BuildTestCase(CliCmdTestBase, unittest.TestCase):

    defaultExpected = dict(order_pipeline=False,
                           pipeline=None,
                           pipeline_actions=list(),
                           pipeline_dot=None,
                           save_pipeline=None,
                           show=(),
                           log_level=dict())
    command = build
    cli = cli

    def test_actionConversion(self):
        """Verify that a pipeline action gets captured and processed correctly
        and passed to the script function in the `pipeline_actions` list. """

        self.run_test(["build", "-t", "foo"],
                      self.makeExpected(pipeline_actions=[_PipelineAction(action="new_task",
                                                                          label=None,
                                                                          value="foo")]))


if __name__ == "__main__":
    unittest.main()
