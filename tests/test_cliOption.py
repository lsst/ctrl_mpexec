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


import click
import unittest

from lsst.ctrl.mpexec.cli.opt import (delete_option,
                                      order_pipeline_option,
                                      pipeline_dot_option,
                                      pipeline_option,
                                      save_pipeline_option,
                                      show_option,
                                      task_option)
from lsst.daf.butler.tests import (OptFlagTest,
                                   OptHelpTest,
                                   OptMultipleTest,
                                   OptPathTypeTest,
                                   OptRequiredTest)


class DeleteTestCase(OptHelpTest,
                     OptMultipleTest,
                     OptRequiredTest,
                     unittest.TestCase):
    metavar = "LABEL"
    optionName = "delete"
    optionClass = delete_option


class OrderPipelineTestCase(OptFlagTest,
                            OptHelpTest,
                            unittest.TestCase):
    optionName = "order-pipeline"
    optionClass = order_pipeline_option


class PipelineTestCase(OptHelpTest,
                       OptRequiredTest,
                       unittest.TestCase):
    shortOptionName = "p"
    optionName = "pipeline"
    optionClass = pipeline_option
    valueType = click.Path(exists=True, writable=False, dir_okay=False, file_okay=True)


class PipelineDotTestCase(OptHelpTest,
                          OptPathTypeTest,
                          OptRequiredTest,
                          unittest.TestCase):
    optionName = "pipeline-dot"
    optionClass = pipeline_dot_option
    valueType = click.Path(exists=True, writable=True, file_okay=True, dir_okay=False)


class SavePipelineTestCase(OptHelpTest,
                           OptRequiredTest,
                           unittest.TestCase):
    metavar = "FILE"
    shortOptionName = "s"
    optionName = "save-pipeline"
    optionClass = save_pipeline_option
    valueType = click.Path(writable=True, dir_okay=True, file_okay=False)


class ShowTestCase(OptHelpTest,
                   OptMultipleTest,
                   OptRequiredTest,
                   unittest.TestCase):
    metavar = show_option.defaultMetavar
    optionName = "show"
    optionClass = show_option


class TaskTestCase(OptHelpTest,
                   OptMultipleTest,
                   OptRequiredTest,
                   unittest.TestCase):
    metavar = task_option.defaultMetavar
    shortOptionName = "t"
    optionName = "task"
    optionClass = task_option


if __name__ == "__main__":
    unittest.main()
