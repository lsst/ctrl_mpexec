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

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from tempfile import NamedTemporaryFile
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
from ..utils import PipetaskCommand, makePipelineActions

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
    pipeline_actions = []
    for action in actions:
        pipeline_actions.append(action)

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
    if kwargs.get("butler_config") is not None and {"pipeline-graph", "task-graph"}.isdisjoint(show.commands):
        raise click.ClickException(
            "--butler-config was provided but nothing uses it "
            "(only --show pipeline-graph and --show task-graph do)."
        )
    script.build(**kwargs, show=show)
    _unhandledShow(show, "build")


@contextmanager
def coverage_context(kwargs: dict[str, Any]) -> Iterator[None]:
    """Enable coverage recording."""
    packages = kwargs.pop("cov_packages", ())
    report = kwargs.pop("cov_report", True)
    if not kwargs.pop("coverage", False):
        yield
        return
    with NamedTemporaryFile("w") as rcfile:
        rcfile.write(
            """
[run]
branch = True
concurrency = multiprocessing
"""
        )
        if packages:
            packages_str = ",".join(packages)
            rcfile.write(f"source_pkgs = {packages_str}\n")
        rcfile.flush()
        cov = coverage.Coverage(config_file=rcfile.name)
        cov.start()
        try:
            yield
        finally:
            cov.stop()
            cov.save()
            if report:
                outdir = "./covhtml"
                cov.html_report(directory=outdir)
                click.echo(f"Coverage report written to {outdir}.")


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
    with coverage_context(kwargs):
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


@click.command(cls=PipetaskCommand, epilog=epilog)
@ctrlMpExecOpts.run_options()
@catch_and_exit
def run(ctx: click.Context, **kwargs: Any) -> None:
    """Build and execute pipeline and quantum graph."""
    kwargs = _collectActions(ctx, **kwargs)
    with coverage_context(kwargs):
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
@ctrlMpExecOpts.coverage_options()
def pre_exec_init_qbb(repo: str, qgraph: str, **kwargs: Any) -> None:
    """Execute pre-exec-init on Quantum-Backed Butler.

    REPO is the location of the butler/registry config file.

    QGRAPH is the path to a serialized Quantum Graph file.
    """
    with coverage_context(kwargs):
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
@ctrlMpExecOpts.coverage_options()
@ctrlMpExecOpts.debug_option()
@ctrlMpExecOpts.start_method_option()
@ctrlMpExecOpts.timeout_option()
@ctrlMpExecOpts.fail_fast_option()
@ctrlMpExecOpts.summary_option()
@ctrlMpExecOpts.enable_implicit_threading_option()
@ctrlMpExecOpts.cores_per_quantum_option()
@ctrlMpExecOpts.memory_per_quantum_option()
def run_qbb(repo: str, qgraph: str, **kwargs: Any) -> None:
    """Execute pipeline using Quantum-Backed Butler.

    REPO is the location of the butler/registry config file.

    QGRAPH is the path to a serialized Quantum Graph file.
    """
    with coverage_context(kwargs):
        script.run_qbb(repo, qgraph, **kwargs)


@click.command(cls=PipetaskCommand)
@ctrlMpExecOpts.qgraph_argument()
@ctrlMpExecOpts.run_argument()
@ctrlMpExecOpts.output_qgraph_argument()
@ctrlMpExecOpts.metadata_run_key_option()
@ctrlMpExecOpts.update_graph_id_option()
def update_graph_run(
    qgraph: str,
    run: str,
    output_qgraph: str,
    metadata_run_key: str,
    update_graph_id: bool,
) -> None:
    """Update existing quantum graph with new output run name and re-generate
    output dataset IDs.

    QGRAPH is the URL to a serialized Quantum Graph file.

    RUN is the new RUN collection name for output graph.

    OUTPUT_QGRAPH is the URL to store the updated Quantum Graph.
    """
    script.update_graph_run(qgraph, run, output_qgraph, metadata_run_key, update_graph_id)


@click.command(cls=PipetaskCommand)
@repo_argument()
@ctrlMpExecOpts.qgraph_argument()
@click.argument("output_yaml", type=click.Path(exists=False))
@click.option("--logs/--no-logs", default=True, help="Get butler log datasets for extra information.")
def report(repo: str, qgraph: str, output_yaml: str, logs: bool = True) -> None:
    """Write a yaml file summarizing the produced and missing expected datasets
    in a quantum graph.

    REPO is the location of the butler/registry config file.

    QGRAPH is the URL to a serialized Quantum Graph file.

    OUTPUT_YAML is the URL to store the summary report.
    """
    script.report(repo, qgraph, output_yaml, logs)
