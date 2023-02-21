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

import sys
import tempfile
from functools import partial
from typing import Any

import click
import coverage
import lsst.pipe.base.cli.opt as pipeBaseOpts
from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler.cli.opt import (
    config_file_option,
    config_option,
    confirm_option,
    options_file_option,
    processes_option,
    repo_argument,
)
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


def _unhandledShow(show: ShowInfo, cmd: str) -> None:
    if show.unhandled:
        print(
            f"The following '--show' options were not known to the {cmd} command: "
            f"{', '.join(show.unhandled)}",
            file=sys.stderr,
        )


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
    _unhandledShow(show, "build")


def _start_coverage(coverage_packages: tuple) -> coverage.Coverage:
    coveragerc = """
[html]
directory = covhtml

[run]
branch = True
concurrency = multiprocessing
"""

    if coverage_packages:
        pkgs = ",".join(coverage_packages)
        click.echo(f"Coverage enabled of packages: {pkgs}")
        coveragerc += f"source_pkgs={pkgs}"
    else:
        click.echo("Coverage enabled")

    with tempfile.NamedTemporaryFile(mode="w") as cov_file:
        cov_file.write(coveragerc)
        cov_file.flush()
        cov = coverage.Coverage(config_file=cov_file.name)

    cov.start()
    return cov


def _stop_coverage(cov: coverage.Coverage) -> None:
    cov.stop()
    outdir = "./covhtml"
    cov.html_report(directory=outdir)
    cov.report()
    click.echo(f"Coverage data written to {outdir}")


@click.command(cls=PipetaskCommand, epilog=epilog)
@click.pass_context
@ctrlMpExecOpts.coverage_option()
@ctrlMpExecOpts.coverage_packages_option()
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
    coverage = kwargs.pop("coverage", False)
    coverage_packages = kwargs.pop("cov_packages", ())
    if coverage:
        cov = _start_coverage(coverage_packages)

    try:
        show = ShowInfo(kwargs.pop("show", []))
        pipeline = script.build(**kwargs, show=show)
        if show.handled and not show.unhandled:
            print(
                "No quantum graph generated. The --show option was given and all options were processed.",
                file=sys.stderr,
            )
            return
        if script.qgraph(pipelineObj=pipeline, **kwargs, show=show) is None:
            raise click.ClickException("QuantumGraph was empty; CRITICAL logs above should provide details.")
        _unhandledShow(show, "qgraph")
    finally:
        if coverage:
            _stop_coverage(cov)


@click.command(cls=PipetaskCommand, epilog=epilog)
@ctrlMpExecOpts.run_options()
@catch_and_exit
def run(ctx: click.Context, **kwargs: Any) -> None:
    """Build and execute pipeline and quantum graph."""
    kwargs = _collectActions(ctx, **kwargs)
    coverage = kwargs.pop("coverage", False)
    if coverage:
        coverage_packages = kwargs.pop("cov_packages", ())
        cov = _start_coverage(coverage_packages)

    try:
        show = ShowInfo(kwargs.pop("show", []))
        pipeline = script.build(**kwargs, show=show)
        if show.handled and not show.unhandled:
            print(
                "No quantum graph generated or pipeline executed. "
                "The --show option was given and all options were processed.",
                file=sys.stderr,
            )
            return
        if (qgraph := script.qgraph(pipelineObj=pipeline, **kwargs, show=show)) is None:
            raise click.ClickException("QuantumGraph was empty; CRITICAL logs above should provide details.")
        _unhandledShow(show, "run")
        if show.handled:
            print(
                "No pipeline executed. The --show option was given and all options were processed.",
                file=sys.stderr,
            )
            return
        script.run(qgraphObj=qgraph, **kwargs)
    finally:
        if coverage:
            _stop_coverage(cov)


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


@click.command(cls=PipetaskCommand)
@repo_argument()
@ctrlMpExecOpts.qgraph_argument()
@ctrlMpExecOpts.config_search_path_option()
@ctrlMpExecOpts.qgraph_id_option()
def pre_exec_init_qbb(repo: str, qgraph: str, **kwargs: Any) -> None:
    """Execute pre-exec-init on Quantum-Backed Butler.

    REPO is the location of the butler/registry config file.

    QGRAPH is the path to a serialized Quantum Graph file.
    """
    script.pre_exec_init_qbb(repo, qgraph, **kwargs)


@click.command(cls=PipetaskCommand)
@repo_argument()
@ctrlMpExecOpts.qgraph_argument()
@ctrlMpExecOpts.config_search_path_option()
@ctrlMpExecOpts.qgraph_id_option()
@ctrlMpExecOpts.qgraph_node_id_option()
@processes_option()
@ctrlMpExecOpts.pdb_option()
@ctrlMpExecOpts.profile_option()
@ctrlMpExecOpts.coverage_option()
@ctrlMpExecOpts.coverage_packages_option()
@ctrlMpExecOpts.debug_option()
@ctrlMpExecOpts.start_method_option()
@ctrlMpExecOpts.timeout_option()
@ctrlMpExecOpts.fail_fast_option()
@ctrlMpExecOpts.summary_option()
@ctrlMpExecOpts.enable_implicit_threading_option()
def run_qbb(repo: str, qgraph: str, **kwargs: Any) -> None:
    """Execute pipeline using Quantum-Backed Butler.

    REPO is the location of the butler/registry config file.

    QGRAPH is the path to a serialized Quantum Graph file.
    """
    coverage = kwargs.pop("coverage", False)
    coverage_packages = kwargs.pop("cov_packages", ())
    if coverage:
        cov = _start_coverage(coverage_packages)

    try:
        script.run_qbb(repo, qgraph, **kwargs)
    finally:
        if coverage:
            _stop_coverage(cov)
