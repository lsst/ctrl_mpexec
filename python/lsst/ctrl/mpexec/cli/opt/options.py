# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

import click

from lsst.daf.butler.cli.utils import MWOptionDecorator, MWPath, split_commas, unwrap
from lsst.utils.doImport import doImportType

if TYPE_CHECKING:
    # Avoid regular module-scope import of test-only code that tinkers with the
    # storage class singleton.
    from lsst.pipe.base.tests.mocks import ForcedFailure


butler_config_option = MWOptionDecorator(
    "-b", "--butler-config", help="Location of the gen3 butler/registry config file."
)


data_query_option = MWOptionDecorator(
    "-d", "--data-query", help="User data selection expression.", metavar="QUERY"
)


debug_option = MWOptionDecorator(
    "--debug", help="Enable debugging output using lsstDebug facility (imports debug.py).", is_flag=True
)

coverage_option = MWOptionDecorator(
    "--coverage", help="Enable coverage output (requires coverage package).", is_flag=True
)

coverage_report_option = MWOptionDecorator(
    "--cov-report/--no-cov-report",
    help="If coverage is enabled, controls whether to produce an HTML coverage report.",
    default=True,
)

coverage_packages_option = MWOptionDecorator(
    "--cov-packages",
    help=unwrap(
        """Python packages to restrict coverage to.  If none are provided, runs coverage on all packages."""
    ),
    multiple=True,
    callback=split_commas,
)

delete_option = MWOptionDecorator(
    "--delete", callback=split_commas, help="Delete task with given label from pipeline.", multiple=True
)


pdb_option = MWOptionDecorator(
    "--pdb",
    help="Post-mortem debugger to launch for exceptions (defaults to pdb if unspecified; requires a tty).",
    is_flag=False,
    flag_value="pdb",
    default=None,
)


extend_run_option = MWOptionDecorator(
    "--extend-run",
    help=(
        "Instead of creating a new RUN collection, insert datasets into either the one given by "
        "--output-run (if provided) or the first child collection of --output (which must be of type RUN). "
        "This also enables --skip-existing option when building a graph. "
        "When executing a graph this option skips quanta with all existing outputs."
    ),
    is_flag=True,
)


graph_fixup_option = MWOptionDecorator(
    "--graph-fixup",
    help="Name of the class or factory method which makes an instance used for execution graph fixup.",
)


init_only_option = MWOptionDecorator(
    "--init-only",
    help="Do not actually run; just register dataset types and/or save init outputs.",
    is_flag=True,
)


input_option = MWOptionDecorator(
    "-i",
    "--input",
    callback=split_commas,
    default=[],
    help="Comma-separated names of the input collection(s).",
    metavar="COLLECTION",
    multiple=True,
)


rebase_option = MWOptionDecorator(
    "--rebase",
    help=unwrap("""Reset output collection chain if it is inconsistent with --inputs"""),
    is_flag=True,
)


no_versions_option = MWOptionDecorator(
    "--no-versions", help="Do not save or check package versions.", is_flag=True
)


order_pipeline_option = MWOptionDecorator(
    "--order-pipeline",
    help=unwrap(
        """Order tasks in pipeline based on their data
        dependencies, ordering is performed as last step before saving or
        executing pipeline."""
    ),
    is_flag=True,
)


output_option = MWOptionDecorator(
    "-o",
    "--output",
    help=unwrap(
        """Name of the output CHAINED collection. This may either be an
        existing CHAINED collection to use as both input and output
        (incompatible with --input), or a new CHAINED collection created
        to include all inputs (requires --input). In both cases, the
        collection's children will start with an output RUN collection
        that directly holds all new datasets (see --output-run)."""
    ),
    metavar="COLL",
)


output_run_option = MWOptionDecorator(
    "--output-run",
    help=unwrap(
        """Name of the new output RUN collection. If not provided
        then --output must be provided and a new RUN collection will
        be created by appending a timestamp to the value passed with
        --output. If this collection already exists then
        --extend-run must be passed."""
    ),
    metavar="COLL",
)


pipeline_option = MWOptionDecorator(
    "-p",
    "--pipeline",
    help="Location of a pipeline definition file in YAML format.",
    type=MWPath(file_okay=True, dir_okay=False, readable=True),
)


pipeline_dot_option = MWOptionDecorator(
    "--pipeline-dot",
    help="Location for storing GraphViz DOT representation of a pipeline.",
    type=MWPath(writable=True, file_okay=True, dir_okay=False),
)


pipeline_mermaid_option = MWOptionDecorator(
    "--pipeline-mermaid",
    help="Location for storing Mermaid representation of a pipeline.",
    type=MWPath(writable=True, file_okay=True, dir_okay=False),
)


profile_option = MWOptionDecorator(
    "--profile", help="Dump cProfile statistics to file name.", type=MWPath(file_okay=True, dir_okay=False)
)


prune_replaced_option = MWOptionDecorator(
    "--prune-replaced",
    help=unwrap(
        """Delete the datasets in the collection replaced by
        --replace-run, either just from the datastore
        ('unstore') or by removing them and the RUN completely
        ('purge'). Requires --replace-run."""
    ),
    type=click.Choice(choices=("unstore", "purge"), case_sensitive=False),
)


qgraph_option = MWOptionDecorator(
    "-g",
    "--qgraph",
    help=unwrap(
        """Location for a serialized quantum graph definition (pickle
        file). If this option is given then all input data options and
        pipeline-building options cannot be used.  Can be a URI."""
    ),
)


qgraph_id_option = MWOptionDecorator(
    "--qgraph-id",
    help=unwrap(
        """Quantum graph identifier, if specified must match the
        identifier of the graph loaded from a file. Ignored if graph
        is not loaded from a file."""
    ),
)


qgraph_datastore_records_option = MWOptionDecorator(
    "--qgraph-datastore-records",
    help=unwrap(
        """Include datastore records into generated quantum graph, these records are used by a
        quantum-backed butler.
        """
    ),
    is_flag=True,
)


# I wanted to use default=None here to match Python API but click silently
# replaces None with an empty tuple when multiple=True.
qgraph_node_id_option = MWOptionDecorator(
    "--qgraph-node-id",
    callback=split_commas,
    multiple=True,
    help=unwrap(
        """Only load a specified set of nodes when graph is
        loaded from a file, nodes are identified by UUID
        values. One or more comma-separated integers are
        accepted. By default all nodes are loaded. Ignored if
        graph is not loaded from a file."""
    ),
)

qgraph_header_data_option = MWOptionDecorator(
    "--show-qgraph-header",
    is_flag=True,
    default=False,
    help="Print the headerData for Quantum Graph to the console",
)

qgraph_dot_option = MWOptionDecorator(
    "--qgraph-dot",
    help="Location for storing GraphViz DOT representation of a quantum graph.",
    type=MWPath(writable=True, file_okay=True, dir_okay=False),
)

qgraph_mermaid_option = MWOptionDecorator(
    "--qgraph-mermaid",
    help="Location for storing Mermaid representation of a quantum graph.",
    type=MWPath(writable=True, file_okay=True, dir_okay=False),
)


replace_run_option = MWOptionDecorator(
    "--replace-run",
    help=unwrap(
        """Before creating a new RUN collection in an existing
        CHAINED collection, remove the first child collection
        (which must be of type RUN). This can be used to repeatedly
        write to the same (parent) collection during development,
        but it does not delete the datasets associated with the
        replaced run unless --prune-replaced is also passed.
        Requires --output, and incompatible with --extend-run."""
    ),
    is_flag=True,
)


save_pipeline_option = MWOptionDecorator(
    "-s",
    "--save-pipeline",
    help="Location for storing resulting pipeline definition in YAML format.",
    type=MWPath(dir_okay=False, file_okay=True, writable=True),
)

save_qgraph_option = MWOptionDecorator(
    "-q",
    "--save-qgraph",
    help="URI location for storing a serialized quantum graph definition (pickle file).",
)


save_single_quanta_option = MWOptionDecorator(
    "--save-single-quanta",
    help=unwrap(
        """Format string of locations for storing individual
        quantum graph definition (pickle files). The curly
        brace {} in the input string will be replaced by a
        quantum number. Can be a URI."""
    ),
)


show_option = MWOptionDecorator(
    "--show",
    callback=split_commas,
    help=unwrap(
        """Dump various info to standard output. Possible items are:
        ``config``, ``config=[Task::]<PATTERN>`` or
        ``config=[Task::]<PATTERN>:NOIGNORECASE`` to dump configuration
        fields possibly matching given pattern and/or task label;
        ``history=<FIELD>`` to dump configuration history for a field,
        field name is specified as ``[Task::]<PATTERN>``; ``dump-config``,
        ``dump-config=Task`` to dump complete configuration for a task
        given its label or all tasks; ``pipeline`` to show pipeline
        composition; ``graph`` to show information about quanta;
        ``workflow`` to show information about quanta and their
        dependency; ``tasks`` to show task composition; ``uri`` to show
        predicted dataset URIs of quanta; ``pipeline-graph`` for a
        text-based visualization of the pipeline (tasks and dataset types);
        ``task-graph`` for a text-based visualization of just the tasks.
        With -b, pipeline-graph and task-graph include additional information.
        """
    ),
    metavar="ITEM|ITEM=VALUE",
    multiple=True,
)


skip_existing_in_option = MWOptionDecorator(
    "--skip-existing-in",
    callback=split_commas,
    default=None,
    metavar="COLLECTION",
    multiple=True,
    help=unwrap(
        """If all Quantum outputs already exist in the specified list of
        collections then that Quantum will be excluded from the QuantumGraph.
        """
    ),
)


skip_existing_option = MWOptionDecorator(
    "--skip-existing",
    is_flag=True,
    help=unwrap(
        """This option is equivalent to --skip-existing-in with the name of
        the output RUN collection. If both --skip-existing-in and
        --skip-existing are given then output RUN collection is appended to
        the list of collections."""
    ),
)


clobber_outputs_option = MWOptionDecorator(
    "--clobber-outputs",
    help=(
        "Remove outputs of failed quanta from the output run when they would block the execution of new "
        "quanta with the same data ID (or assume that this will be done, if just building a QuantumGraph).  "
        "Does nothing if --extend-run is not passed."
    ),
    is_flag=True,
)


skip_init_writes_option = MWOptionDecorator(
    "--skip-init-writes",
    help=unwrap(
        """Do not write collection-wide 'init output' datasets
                                                        (e.g.schemas)."""
    ),
    is_flag=True,
)


enable_implicit_threading_option = MWOptionDecorator(
    "--enable-implicit-threading",
    help=unwrap(
        """Do not disable implicit threading use by third-party libraries (e.g. OpenBLAS).
        Implicit threading is always disabled during execution with multiprocessing."""
    ),
    is_flag=True,
)

cores_per_quantum_option = MWOptionDecorator(
    "-n",
    "--cores-per-quantum",
    default=1,
    help=unwrap(
        """Number of cores available to each quantum when executing.
        If '-j' is used each subprocess will be allowed to use this number of cores."""
    ),
    type=click.IntRange(min=1),
)

memory_per_quantum_option = MWOptionDecorator(
    "--memory-per-quantum",
    default="",
    help=unwrap(
        """Memory allocated for each quantum to use when executing.
        This memory allocation is not enforced by the execution system and is purely advisory.
        If '-j' used each subprocess will be allowed to use this amount of memory.
        Units are allowed and the default units for a plain integer are MB.
        For example: '3GB', '3000MB' and '3000' would all result in the same
        memory limit. Default is for no limit."""
    ),
    type=str,
)

task_option = MWOptionDecorator(
    "-t",
    "--task",
    callback=split_commas,
    help=unwrap(
        """Task name to add to pipeline, must be a fully qualified task
        name. Task name can be followed by colon and label name, if label
        is not given then task base name (class name) is used as
        label."""
    ),
    metavar="TASK[:LABEL]",
    multiple=True,
)


timeout_option = MWOptionDecorator(
    "--timeout", type=click.IntRange(min=0), help="Timeout for multiprocessing; maximum wall time (sec)."
)


start_method_option = MWOptionDecorator(
    "--start-method",
    default=None,
    type=click.Choice(choices=["spawn", "fork", "forkserver"]),
    help=(
        "Multiprocessing start method, default is platform-specific. "
        "Fork method is no longer supported, spawn is used instead if fork is selected."
    ),
)


fail_fast_option = MWOptionDecorator(
    "--fail-fast",
    help="Stop processing at first error, default is to process as many tasks as possible.",
    is_flag=True,
)

raise_on_partial_outputs_option = MWOptionDecorator(
    "--raise-on-partial-outputs/--no-raise-on-partial-outputs",
    help="Consider partial outputs from a task an error instead of a qualified success.",
    is_flag=True,
    default=True,
)

save_execution_butler_option = MWOptionDecorator(
    "--save-execution-butler",
    help="Export location for an execution-specific butler after making QuantumGraph",
)

mock_option = MWOptionDecorator(
    "--mock",
    help="Mock pipeline execution.",
    is_flag=True,
)

unmocked_dataset_types_option = MWOptionDecorator(
    "--unmocked-dataset-types",
    callback=split_commas,
    default=None,
    metavar="COLLECTION",
    multiple=True,
    help="Names of input dataset types that should not be mocked.",
)


def parse_mock_failure(
    ctx: click.Context, param: click.Option, value: Iterable[str] | None
) -> Mapping[str, ForcedFailure]:
    """Parse the --mock-failure option values into the mapping accepted by
    `~lsst.pipe.base.tests.mocks.mock_task_defs`.

    Parameters
    ----------
    ctx : `click.Context`
        Context provided by Click.
    param : `click.Option`
        Click option.
    value : `~collections.abc.Iterable` [`str`] or `None`
        Value from option.
    """
    # Avoid regular module-scope import of test-only code that tinkers with the
    # storage class singleton.
    from lsst.pipe.base.tests.mocks import ForcedFailure

    result: dict[str, ForcedFailure] = {}
    if value is None:
        return result
    for entry in value:
        try:
            task_label, error_type_name, where, *rest = entry.split(":")
            if rest:
                (memory_required,) = rest
            else:
                memory_required = None
        except ValueError:
            raise click.UsageError(
                f"Invalid value for --mock-failure option: {entry!r}; "
                "expected a string of the form 'task:error:where[:mem]'."
            ) from None
        error_type = doImportType(error_type_name) if error_type_name else None
        result[task_label] = ForcedFailure(where, error_type, memory_required)
    return result


mock_failure_option = MWOptionDecorator(
    "--mock-failure",
    callback=parse_mock_failure,
    metavar="LABEL:EXCEPTION:WHERE",
    default=None,
    multiple=True,
    help=unwrap(
        """Specifications for tasks that should be configured to fail
        when mocking execution.  This is a colon-separated 3-tuple or 4-tuple,
        where the first entry the task label, the second the fully-qualified
        exception type (empty for ValueError, and the third a string (which
        typically needs to be quoted to be passed as one argument value by the
        shell) of the form passed to --where, indicating which data IDs should
        fail.  The final optional term is the memory "required" by the task
        (with units recognized by astropy), which will cause the error to only
        occur if the "available" memory (according to
        ExecutionResources.max_mem) is less than this value.  Note that actual
        memory usage is irrelevant here; this is all mock behavior."""
    ),
)


clobber_execution_butler_option = MWOptionDecorator(
    "--clobber-execution-butler",
    help=unwrap(
        """When creating execution butler overwrite
                                                                   any existing products"""
    ),
    is_flag=True,
)

target_datastore_root_option = MWOptionDecorator(
    "--target-datastore-root",
    help=unwrap(
        """Root directory for datastore of execution butler.
        Default is to use the original datastore.
        """
    ),
)

dataset_query_constraint = MWOptionDecorator(
    "--dataset-query-constraint",
    help=unwrap(
        """When constructing a quantum graph constrain by
        pre-existence of specified dataset types. Valid
        values are `all` for all inputs dataset types in
        pipeline, ``off`` to not consider dataset type
        existence as a constraint, single or comma
        separated list of dataset type names."""
    ),
    default="all",
)

summary_option = MWOptionDecorator(
    "--summary",
    help=(
        "Location for storing job summary (JSON file). Note that the"
        " structure of this file may not be stable."
    ),
    type=MWPath(dir_okay=False, file_okay=True, writable=True),
)


recursive_option = MWOptionDecorator(
    "--recursive",
    is_flag=True,
)

config_search_path_option = MWOptionDecorator(
    "--config-search-path",
    callback=split_commas,
    default=[],
    help="Additional search paths for butler configuration.",
    metavar="PATH",
    multiple=True,
)

update_graph_id_option = MWOptionDecorator(
    "--update-graph-id",
    help=unwrap("Update graph ID with new unique value."),
    is_flag=True,
)

metadata_run_key_option = MWOptionDecorator(
    "--metadata-run-key",
    help=(
        "Quantum graph metadata key for the name of the output run. "
        "Empty string disables update of the metadata. "
        "Default value: output_run."
    ),
    default="output_run",
)
