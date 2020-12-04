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

from lsst.daf.butler.cli.opt import (config_file_option,
                                     config_option,
                                     options_file_option)
from lsst.daf.butler.cli.utils import (cli_handle_exception,
                                       MWCtxObj,
                                       option_section,
                                       unwrap)
import lsst.obs.base.cli.opt as obsBaseOpts
from .. import opt as ctrlMpExecOpts
from .. import script
from ..utils import makePipelineActions, PipetaskCommand


epilog = unwrap("""Notes:

--task, --delete, --config, --config-file, and --instrument action options can
appear multiple times; all values are used, in order left to right.

FILE reads command-line options from the specified file. Data may be
distributed among multiple lines (e.g. one option per line). Data after # is
treated as a comment and ignored. Blank lines and lines starting with # are
ignored.)
""")


def _doBuild(ctx, **kwargs):
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
    for pipelineAction in (ctrlMpExecOpts.task_option.name(), ctrlMpExecOpts.delete_option.name(),
                           config_option.name(), config_file_option.name(),
                           obsBaseOpts.instrument_option.name()):
        kwargs.pop(pipelineAction)
    kwargs['pipeline_actions'] = makePipelineActions(MWCtxObj.getFrom(ctx).args)
    return cli_handle_exception(script.build, **kwargs)


@click.command(cls=PipetaskCommand, epilog=epilog, short_help="Build pipeline definition.")
@click.pass_context
@ctrlMpExecOpts.show_option()
@ctrlMpExecOpts.pipeline_build_options()
@option_section(sectionText="")
@options_file_option()
def build(ctx, **kwargs):
    """Build and optionally save pipeline definition.

    This does not require input data to be specified.
    """
    _doBuild(ctx, **kwargs)


@click.command(cls=PipetaskCommand, epilog=epilog)
@click.pass_context
@ctrlMpExecOpts.show_option()
@ctrlMpExecOpts.pipeline_build_options()
@ctrlMpExecOpts.qgraph_options()
@ctrlMpExecOpts.butler_options()
@option_section(sectionText="")
@options_file_option()
def qgraph(ctx, **kwargs):
    """Build and optionally save quantum graph.
    """
    pipeline = _doBuild(ctx, **kwargs)
    cli_handle_exception(script.qgraph, pipelineObj=pipeline, **kwargs)


@click.command(cls=PipetaskCommand, epilog=epilog)
@ctrlMpExecOpts.run_options()
def run(ctx, **kwargs):
    """Build and execute pipeline and quantum graph.
    """
    pipeline = _doBuild(ctx, **kwargs)
    qgraph = cli_handle_exception(script.qgraph, pipelineObj=pipeline, **kwargs)
    cli_handle_exception(script.run, qgraphObj=qgraph, **kwargs)
