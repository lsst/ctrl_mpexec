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

from functools import partial
from typing import Any

import click
import lsst.pipe.base.cli.opt as pipeBaseOpts
from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler.cli.opt import config_file_option, config_option, confirm_option, options_file_option
from lsst.daf.butler.cli.utils import MWCtxObj, catch_and_exit, option_section, unwrap

from .. import opt as ctrlMpExecOpts
from .. import script
from ..script import confirmable
from ..utils import _ACTION_CONFIG, _ACTION_CONFIG_FILE, PipetaskCommand, makePipelineActions

epilog = unwrap(
    """Notes:

--task, --delete, --config, --config-file, and --instrument action options can
appear multiple times; all values are used, in order left to right.

FILE reads command-line options from the specified file. Data may be
distributed among multiple lines (e.g. one option per line). Data after # is
treated as a comment and ignored. Blank lines and lines starting with # are
ignored.)
"""
)


def _collectActions(ctx: click.Context, **kwargs: Any) -> dict[str, Any]:
    """Extract pipeline building options, replace them with PipelineActions,
    return updated `kwargs`.

    Notes
    -----
    The pipeline actions (task, delete, config, config_file, and instrument)
    must be handled in the order they appear on the command line, but the CLI
    specification gives them all different option names. So, instead of using
    the individual action options as they appear in kwargs (because
    invocation order can't be known), we capture the CLI arguments by
    overriding `click.Command.parse_args` and save them in the Context's
    `obj` parameter. We use `makePipelineActions` to create a list of
    pipeline actions from the CLI arguments and pass that list to the script
    function using the `pipeline_actions` kwarg name, and remove the action
    options from kwargs.
    """
    for pipelineAction in (
        ctrlMpExecOpts.task_option.name(),
        ctrlMpExecOpts.delete_option.name(),
        config_option.name(),
        config_file_option.name(),
        pipeBaseOpts.instrument_option.name(),
    ):
        kwargs.pop(pipelineAction)

    actions = makePipelineActions(MWCtxObj.getFrom(ctx).args)
    mock_configs = []
    pipeline_actions = []
    for action in actions:
        if action.label and action.label.endswith("-mock"):
            if action.action not in (_ACTION_CONFIG.action, _ACTION_CONFIG_FILE.action):
                raise ValueError(f"Unexpected option for mock task config overrides: {action}")
            mock_configs.append(action)
        else:
            pipeline_actions.append(action)

    kwargs["mock_configs"] = mock_configs
    kwargs["pipeline_actions"] = pipeline_actions
    return kwargs


@click.command(cls=PipetaskCommand, epilog=epilog, short_help="Build pipeline definition.")
@click.pass_context
@ctrlMpExecOpts.show_option()
@ctrlMpExecOpts.pipeline_build_options()
@option_section(sectionText="")
@options_file_option()
@catch_and_exit
def build(ctx: click.Context, **kwargs: Any) -> None:
    """Build and optionally save pipeline definition.

    This does not require input data to be specified.
    """
    kwargs = _collectActions(ctx, **kwargs)
    show = ShowInfo(kwargs.pop("show", []))
    script.build(**kwargs, show=show)
    if show.unhandled:
        print(
            "The following '--show' options were not known to the build command: "
            f"{', '.join(show.unhandled)}"
        )


@click.command(cls=PipetaskCommand, epilog=epilog)
@click.pass_context
@ctrlMpExecOpts.show_option()
@ctrlMpExecOpts.pipeline_build_options()
@ctrlMpExecOpts.qgraph_options()
@ctrlMpExecOpts.butler_options()
@option_section(sectionText="")
@options_file_option()
@catch_and_exit
def qgraph(ctx: click.Context, **kwargs: Any) -> None:
    """Build and optionally save quantum graph."""
    kwargs = _collectActions(ctx, **kwargs)
    show = ShowInfo(kwargs.pop("show", []))
    pipeline = script.build(**kwargs, show=show)
    if show.handled and not show.unhandled:
        # The show option was given and all options were processed.
        # No need to also build the quantum graph.
        return
    script.qgraph(pipelineObj=pipeline, **kwargs, show=show)


@click.command(cls=PipetaskCommand, epilog=epilog)
@ctrlMpExecOpts.run_options()
@catch_and_exit
def run(ctx: click.Context, **kwargs: Any) -> None:
    """Build and execute pipeline and quantum graph."""
    kwargs = _collectActions(ctx, **kwargs)
    show = ShowInfo(kwargs.pop("show", []))
    pipeline = script.build(**kwargs, show=show)
    if show.handled and not show.unhandled:
        # The show option was given and all options were processed.
        # No need to also build the quantum graph.
        return
    qgraph = script.qgraph(pipelineObj=pipeline, **kwargs, show=show)
    if show.handled:
        # The show option was given and all graph options were processed.
        # No need to also run the pipeline.
        return
    script.run(qgraphObj=qgraph, **kwargs)


@click.command(cls=PipetaskCommand)
@ctrlMpExecOpts.butler_config_option()
@ctrlMpExecOpts.collection_argument()
@confirm_option()
@ctrlMpExecOpts.recursive_option(
    help="""If the parent CHAINED collection has child CHAINED collections,
    search the children until nested chains that start with the parent's name
    are removed."""
)
def purge(confirm: bool, **kwargs: Any) -> None:
    """Remove a CHAINED collection and its contained collections.

    COLLECTION is the name of the chained collection to purge. it must not be a
    child of any other CHAINED collections

    Child collections must be members of exactly one collection.

    The collections that will be removed will be printed, there will be an
    option to continue or abort (unless using --no-confirm).
    """
    confirmable.confirm(partial(script.purge, **kwargs), confirm)


@click.command(cls=PipetaskCommand)
@ctrlMpExecOpts.butler_config_option()
@ctrlMpExecOpts.collection_argument()
@confirm_option()
def cleanup(confirm: bool, **kwargs: Any) -> None:
    """Remove non-members of CHAINED collections.

    Removes collections that start with the same name as a CHAINED
    collection but are not members of that collection.
    """
    confirmable.confirm(partial(script.cleanup, **kwargs), confirm)
