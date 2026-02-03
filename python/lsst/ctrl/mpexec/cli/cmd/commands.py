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
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from functools import partial
from importlib import import_module
from tempfile import NamedTemporaryFile
from typing import Any

import click

from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler.cli.opt import (
    collections_option,
    confirm_option,
    options_file_option,
    processes_option,
    repo_argument,
    where_option,
)
from lsst.daf.butler.cli.utils import catch_and_exit, option_section, unwrap
from lsst.pipe.base.quantum_reports import Report

from .. import opt as ctrlMpExecOpts
from .. import script
from ..script import confirmable
from ..utils import PipetaskCommand, collect_pipeline_actions

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
    kwargs = collect_pipeline_actions(ctx, **kwargs)
    show = ShowInfo(kwargs.pop("show", []))
    if kwargs.get("butler_config") is not None and (
        {"pipeline-graph", "task-graph"}.isdisjoint(show.commands) and not kwargs.get("pipeline_dot")
    ):
        raise click.ClickException(
            "--butler-config was provided but nothing uses it "
            "(only --show pipeline-graph, --show task-graph and --pipeline-dot do)."
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
    # Lazily import coverage only when we might need it
    try:
        coverage = import_module("coverage")
    except ModuleNotFoundError:
        raise click.ClickException("coverage was requested but the coverage package is not installed.")
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
@ctrlMpExecOpts.pipeline_build_options(skip_butler_config=True)
@ctrlMpExecOpts.qgraph_options()
@ctrlMpExecOpts.butler_options()
@option_section(sectionText="")
@options_file_option()
@catch_and_exit
def qgraph(ctx: click.Context, **kwargs: Any) -> None:
    """Build and optionally save quantum graph."""
    kwargs = collect_pipeline_actions(ctx, **kwargs)
    summary = kwargs.pop("summary", None)
    with coverage_context(kwargs):
        show = ShowInfo(kwargs.pop("show", []))
        # The only reason 'build' might want a butler is to resolve the
        # pipeline graph for its own 'show' options, which wouldn't run in this
        # context.  Take it out of the kwargs so it doesn't instantiate a
        # butler unnecessarily.
        butler_config = kwargs.pop("butler_config", None)
        pipeline_graph_factory = script.build(**kwargs, show=show)
        kwargs["butler_config"] = butler_config
        if show.handled and not show.unhandled:
            print(
                "No quantum graph generated. The --show option was given and all options were processed.",
                file=sys.stderr,
            )
            return
        if (
            qgraph := script.qgraph(
                pipeline_graph_factory,
                **kwargs,
                show=show,
                # Making a summary report requires that we load the same graph
                # components as execution.
                for_execution=(summary is not None),
            )
        ) is None:
            raise click.ClickException("QuantumGraph was empty; ERROR logs above should provide details.")
        # QuantumGraph-only summary call here since script.qgraph also called
        # by run methods.
        if summary:
            report = Report(qgraphSummary=qgraph._make_summary())
            with open(summary, "w") as out:
                # Do not save fields that are not set.
                out.write(report.model_dump_json(exclude_none=True, indent=2))

        _unhandledShow(show, "qgraph")


@click.command(cls=PipetaskCommand, epilog=epilog)
@ctrlMpExecOpts.run_options()
@catch_and_exit
def run(ctx: click.Context, **kwargs: Any) -> None:
    """Build and execute pipeline and quantum graph."""
    kwargs = collect_pipeline_actions(ctx, **kwargs)
    with coverage_context(kwargs):
        show = ShowInfo(kwargs.pop("show", []))
        pipeline_graph_factory = script.build(**kwargs, show=show)
        if show.handled and not show.unhandled:
            print(
                "No quantum graph generated or pipeline executed. "
                "The --show option was given and all options were processed.",
                file=sys.stderr,
            )
            return
        if (qgraph := script.qgraph(pipeline_graph_factory, for_execution=True, **kwargs, show=show)) is None:
            raise click.ClickException("QuantumGraph was empty; ERROR logs above should provide details.")
        _unhandledShow(show, "run")
        if show.handled:
            print(
                "No pipeline executed. The --show option was given and all options were processed.",
                file=sys.stderr,
            )
            return
        script.run(qgraph, **kwargs)


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
@ctrlMpExecOpts.raise_on_partial_outputs_option()
@ctrlMpExecOpts.summary_option()
@ctrlMpExecOpts.enable_implicit_threading_option()
@ctrlMpExecOpts.cores_per_quantum_option()
@ctrlMpExecOpts.memory_per_quantum_option()
@ctrlMpExecOpts.no_existing_outputs_option()
def run_qbb(repo: str, qgraph: str, **kwargs: Any) -> None:
    """Execute pipeline using Quantum-Backed Butler.

    REPO is the location of the butler/registry config file.

    QGRAPH is the path to a serialized Quantum Graph file.
    """
    with coverage_context(kwargs):
        script.run_qbb(butler_config=repo, qgraph=qgraph, **kwargs)


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
@click.argument("qgraphs", nargs=-1)
@collections_option()
@where_option()
@click.option(
    "--full-output-filename",
    default="",
    help="Output report as a file with this name. "
    "For pipetask report on one graph, this should be a yaml file. For multiple graphs "
    "or when using the --force-v2 option, this should be a json file. We will be "
    "deprecating the single-graph-only (QuantumGraphExecutionReport) option soon.",
)
@click.option("--logs/--no-logs", default=True, help="Get butler log datasets for extra information.")
@click.option(
    "--brief",
    default=False,
    is_flag=True,
    help="Only show counts in report (a brief summary). Note that counts are"
    " also printed to the screen when using the --full-output-filename option.",
)
@click.option(
    "--curse-failed-logs",
    is_flag=True,
    default=False,
    help="If log datasets are missing in v2 (QuantumProvenanceGraph), mark them as cursed",
)
@click.option(
    "--force-v2",
    is_flag=True,
    default=False,
    help="Use the QuantumProvenanceGraph instead of the QuantumGraphExecutionReport, "
    "even when there is only one qgraph. Otherwise, the QuantumGraphExecutionReport "
    "will run on one graph by default.",
)
@click.option(
    "--read-caveats",
    type=click.Choice(["exhaustive", "lazy", "none"], case_sensitive=False),
    default="lazy",
)
@click.option(
    "--use-qbb/--no-use-qbb",
    is_flag=True,
    default=True,
    help="Whether to use a quantum-backed butler for metadata and log reads.",
)
@click.option(
    "--view-graph",
    is_flag=True,
    default=False,
    help="Display pipeline processing status as a graph on stdout instead of a plain-text summary.",
)
@processes_option()
def report(
    repo: str,
    qgraphs: Sequence[str],
    collections: Sequence[str] | None,
    where: str,
    full_output_filename: str = "",
    logs: bool = True,
    brief: bool = False,
    curse_failed_logs: bool = False,
    force_v2: bool = False,
    read_caveats: str = "lazy",
    use_qbb: bool = True,
    processes: int = 1,
    view_graph: bool = False,
) -> None:
    """Summarize the state of executed quantum graph(s), with counts of failed,
    successful and expected quanta, as well as counts of output datasets and
    their query (visible/shadowed) states. Analyze one or more attempts at the
    same processing on the same dataquery-identified "group" and resolve
    recoveries and persistent failures. Identify mismatch errors between
    attempts.

    Save the report as a file (``--full-output-filename``) or print it to
    stdout (default). If the terminal is overwhelmed with data_ids from
    failures try the ``--brief`` option.

    Butler ``collections`` and ``where`` options are for use in
    `lsst.daf.butler.Registry.queryDatasets` if paring down the collections
    would be useful. Pass collections in order of most to least recent. By
    default the collections and query will be taken from the graphs.

    REPO is the location of the butler/registry config file.

    QGRAPHS is a sequence of links to serialized Quantum Graphs which have
    been executed and are to be analyzed. Pass the graphs in order of first to
    last executed.
    """
    if any([force_v2, len(qgraphs) > 1, collections, where, curse_failed_logs]):
        script.report_v2(
            repo,
            qgraphs,
            collections,
            where,
            full_output_filename,
            logs,
            brief,
            curse_failed_logs,
            read_caveats=(read_caveats if read_caveats != "none" else None),  # type: ignore[arg-type]
            use_qbb=use_qbb,
            n_cores=processes,
            view_graph=view_graph,
        )
    else:
        assert len(qgraphs) == 1, "Cannot make a report without a quantum graph."
        script.report(repo, qgraphs[0], full_output_filename, logs, brief)


@click.command(cls=PipetaskCommand)
@click.argument("filenames", nargs=-1)
@click.option(
    "--full-output-filename",
    default="",
    help="Output report as a file with this name (json).",
)
@click.option(
    "--brief",
    default=False,
    is_flag=True,
    help="Only show counts in report (a brief summary). Note that counts are"
    " also printed to the screen when using the --full-output-filename option.",
)
def aggregate_reports(
    filenames: Sequence[str], full_output_filename: str | None, brief: bool = False
) -> None:
    """Aggregate pipetask report output on disjoint data-id groups into one
    Summary over common tasks and datasets. Intended for use when the same
    pipeline has been run over all groups (i.e., to aggregate all reports
    for a given step). This functionality is only compatible with reports
    from the `~lsst.pipe.base.quantum_provenance_graph.QuantumProvenanceGraph`,
    so the reports must be run over multiple groups or with the ``--force-v2``
    option.

    Save the report as a file (``--full-output-filename``) or print it to
    stdout (default). If the terminal is overwhelmed with data_ids from
    failures try the ``--brief`` option.

    FILENAMES are the space-separated paths to json file output created by
    pipetask report.
    """
    script.aggregate_reports(filenames, full_output_filename, brief)
