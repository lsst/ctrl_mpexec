# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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

from lsst.daf.butler.cli.utils import MWOptionDecorator, MWPath, split_commas, unwrap


delete_option = MWOptionDecorator("--delete",
                                  callback=split_commas,
                                  help="Delete task with given label from pipeline.",
                                  multiple=True)


order_pipeline_option = MWOptionDecorator("--order-pipeline",
                                          help=unwrap("""Order tasks in pipeline based on their data
                                          dependencies, ordering is performed as last step before saving or
                                          executing pipeline."""),
                                          is_flag=True)


pipeline_option = MWOptionDecorator("-p", "--pipeline",
                                    help="Location of a pipeline definition file in YAML format.",
                                    type=MWPath(exists=True, file_okay=True, dir_okay=False, readable=True))


pipeline_dot_option = MWOptionDecorator("--pipeline-dot",
                                        help=unwrap(""""Location for storing GraphViz DOT representation of a
                                                    pipeline."""),
                                        type=MWPath(writable=True, file_okay=True, dir_okay=False))


save_pipeline_option = MWOptionDecorator("-s", "--save-pipeline",
                                         help=unwrap("""Location for storing resulting pipeline definition in
                                                     YAML format."""),
                                         type=MWPath(dir_okay=False, file_okay=True, writable=True))

show_option = MWOptionDecorator("--show",
                                callback=split_commas,
                                help=unwrap("""Dump various info to standard output. Possible items are:
                                            `config`, `config=[Task::]<PATTERN>` or
                                            `config=[Task::]<PATTERN>:NOIGNORECASE` to dump configuration
                                            fields possibly matching given pattern and/or task label;
                                            `history=<FIELD>` to dump configuration history for a field, field
                                            name is specified as [Task::][SubTask.]Field; `dump-config`,
                                            `dump-config=Task` to dump complete configuration for a task given
                                            its label or all tasks; `pipeline` to show pipeline composition;
                                            `graph` to show information about quanta; `workflow` to show
                                            information about quanta and their dependency; `tasks` to show
                                            task composition."""),
                                metavar="ITEM|ITEM=VALUE",
                                multiple=True)


task_option = MWOptionDecorator("-t", "--task",
                                callback=split_commas,
                                help=unwrap("""Task name to add to pipeline, must be a fully qualified task
                                            name. Task name can be followed by colon and label name, if label
                                            is not given then task base name (class name) is used as
                                            label."""),
                                metavar="TASK[:LABEL]",
                                multiple=True)
