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
from lsst.daf.butler.cli.utils import Mocker
from lsst.ctrl.mpexec.cli.pipetask import cli
from lsst.ctrl.mpexec.cli.cmd import build, qgraph, run
from lsst.ctrl.mpexec.cmdLineParser import _PipelineAction


class BuildTestCase(CliCmdTestBase, unittest.TestCase):

    def setUp(self):
        Mocker.reset()
        super().setUp()

    @staticmethod
    def defaultExpected():
        return dict(order_pipeline=False,
                    pipeline=None,
                    pipeline_actions=list(),
                    pipeline_dot=None,
                    save_pipeline=None,
                    show=(),
                    log_level=dict())

    @staticmethod
    def command():
        return build

    cli = cli

    def test_actionConversion(self):
        """Verify that a pipeline action gets captured and processed correctly
        and passed to the script function in the `pipeline_actions` list. """

        self.run_test(["build", "-t", "foo"],
                      self.makeExpected(pipeline_actions=[_PipelineAction(action="new_task",
                                                                          label=None,
                                                                          value="foo")]))


class QgraphTestCase(CliCmdTestBase, unittest.TestCase):

    def setUp(self):
        Mocker.reset()
        super().setUp()

    @staticmethod
    def defaultExpected():
        return dict(butler_config=None,
                    data_query=None,
                    extend_run=False,
                    input=(),
                    log_level={},
                    output=None,
                    output_run=None,
                    pipeline=None,
                    prune_replaced=None,
                    qgraph=None,
                    qgraph_dot=None,
                    replace_run=False,
                    save_qgraph=None,
                    save_single_quanta=None,
                    show=(),
                    skip_existing=False)

    @staticmethod
    def command():
        return qgraph

    cli = cli

    def test_defaultValues(self):
        """Test the default values match the defaultExpected values."""
        self.run_test(["qgraph"], self.makeExpected())


class RunTestCase(CliCmdTestBase, unittest.TestCase):

    @staticmethod
    def defaultExpected():
        return dict(butler_config=None,
                    data_query=None,
                    debug=None,
                    do_raise=False,
                    extend_run=False,
                    graph_fixup=None,
                    init_only=False,
                    input=(),
                    log_level={},
                    no_versions=False,
                    output=None,
                    output_run=None,
                    processes=None,
                    profile=None,
                    prune_replaced=None,
                    qgraph=None,
                    register_dataset_types=False,
                    replace_run=False,
                    skip_existing=False,
                    skip_init_writes=False,
                    timeout=None,
                    fail_fast=False)

    @staticmethod
    def command():
        return run

    cli = cli

    def setUp(self):
        Mocker.reset()
        super().setUp()

    def test_defaultValues(self):
        """Test the default values match the defaultExpected values."""
        self.run_test(["run"], self.makeExpected())

    def test_subcommandPassButlerParameters(self):
        """Test that a butler parameter passed to `qgraph` are forwarded to
        `run`."""
        configFileName = "butler_config.yaml"
        self.run_test(["qgraph", "--butler-config", configFileName, "run"],
                      (QgraphTestCase.makeExpected(butler_config=configFileName),  # qgraph call
                       self.makeExpected(butler_config=configFileName)),  # run call
                      withTempFile=configFileName)


if __name__ == "__main__":
    unittest.main()
