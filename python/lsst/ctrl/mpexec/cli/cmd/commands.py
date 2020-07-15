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
import copy

from lsst.daf.butler.cli.opt import (config_file_option,
                                     config_option,
                                     log_level_option)
from lsst.daf.butler.cli.utils import cli_handle_exception
from lsst.obs.base.cli.opt import instrument_parameter
from ..opt import (delete_option,
                   order_pipeline_option,
                   pipeline_dot_option,
                   pipeline_option,
                   save_pipeline_option,
                   show_option,
                   task_option)
from .. import script
from ..utils import makePipelineActions


instrumentOptionHelp = ("Add an instrument which will be used to load config overrides when defining a "
                        "pipeline. This must be the fully qualified class name.")


class PipetaskCommand(click.Command):
    def parse_args(self, ctx, args):
        ctx.obj = copy.copy(args)
        super().parse_args(ctx, args)


@click.command(cls=PipetaskCommand, short_help="Build pipeline definition.")
@click.pass_context
@pipeline_option()
@task_option(multiple=True)
@delete_option(metavar="LABEL", multiple=True)
@config_option(metavar="LABEL:NAME=VALUE", multiple=True)
@config_file_option(help="Configuration override file(s), applies to a task with a given label.",
                    metavar="LABEL:FILE",
                    multiple=True)
@order_pipeline_option()
@save_pipeline_option()
@pipeline_dot_option()
@instrument_parameter(help=instrumentOptionHelp, metavar="instrument", multiple=True)
@show_option(multiple=True)
@log_level_option(defaultValue=None)
def build(ctx, *args, **kwargs):
    """Build and optionally save pipeline definition.

    This does not require input data to be specified.
    """
    # The pipeline actions (task, delete, config, config_file, and instrument)
    # must be handled in the order they appear on the command line, but the CLI
    # specification gives them all different option names. So, instead of using
    # the individual action options as they appear in kwargs (because
    # invocation order can't be known), we capture the CLI arguments by
    # overriding `click.Command.parse_args` and save them in the Context's
    # `obj` parameter. We use `makePipelineActions` to create a list of
    # pipeline actions from the CLI arguments and pass that list to the script
    # function using the `pipeline_actions` kwarg name, and remove the action
    # options from kwargs.
    for pipelineAction in (task_option.optionKey, delete_option.optionKey, config_option.optionKey,
                           config_file_option.optionKey, instrument_parameter.optionKey):
        kwargs.pop(pipelineAction)
    kwargs['pipeline_actions'] = makePipelineActions(ctx.obj)
    cli_handle_exception(script.build, *args, **kwargs)


@click.command(cls=PipetaskCommand)
def qgraph(*args, **kwargs):
    """Not implemented.

    Build and optionally save pipeline and quantum graph.
    """
    print("Not implemented.")


@click.command(cls=PipetaskCommand)
def run(*args, **kwargs):
    """Not implemented.

    Build and execute pipeline and quantum graph.
    """
    print("Not implemented.")
