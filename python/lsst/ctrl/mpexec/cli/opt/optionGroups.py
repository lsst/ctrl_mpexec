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


__all__ = (
    "butler_options",
    "execution_options",
    "meta_info_options",
    "pipeline_build_options",
    "qgraph_options",
    "run_options",
)


import click
import lsst.daf.butler.cli.opt as dafButlerOpts
import lsst.pipe.base.cli.opt as pipeBaseOpts
from lsst.daf.butler.cli.opt import transfer_option
from lsst.daf.butler.cli.utils import OptionGroup, option_section, unwrap

from . import options as ctrlMpExecOpts

instrumentOptionHelp = (
    "Add an instrument which will be used to load config overrides when "
    "defining a pipeline. This must be the fully qualified class name."
)


class pipeline_build_options(OptionGroup):  # noqa: N801
    """Decorator to add options to the command function for building a
    pipeline."""

    def __init__(self) -> None:
        self.decorators = [
            option_section(sectionText="Pipeline build options:"),
            ctrlMpExecOpts.pipeline_option(),
            ctrlMpExecOpts.task_option(),
            ctrlMpExecOpts.delete_option(metavar="LABEL"),
            dafButlerOpts.config_option(metavar="LABEL:NAME=VALUE", multiple=True),
            dafButlerOpts.config_file_option(
                help=unwrap(
                    """Configuration override file(s), applies to a task
                                                         with a given label."""
                ),
                metavar="LABEL:FILE",
                multiple=True,
            ),
            ctrlMpExecOpts.order_pipeline_option(),
            ctrlMpExecOpts.save_pipeline_option(),
            ctrlMpExecOpts.pipeline_dot_option(),
            pipeBaseOpts.instrument_option(help=instrumentOptionHelp, metavar="instrument", multiple=True),
        ]


class qgraph_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for creating a quantum
    graph."""

    def __init__(self) -> None:
        self.decorators = [
            option_section(sectionText="Quantum graph building options:"),
            ctrlMpExecOpts.qgraph_option(),
            ctrlMpExecOpts.qgraph_id_option(),
            ctrlMpExecOpts.qgraph_node_id_option(),
            ctrlMpExecOpts.qgraph_datastore_records_option(),
            ctrlMpExecOpts.skip_existing_in_option(),
            ctrlMpExecOpts.skip_existing_option(),
            ctrlMpExecOpts.clobber_outputs_option(),
            ctrlMpExecOpts.save_qgraph_option(),
            ctrlMpExecOpts.save_single_quanta_option(),
            ctrlMpExecOpts.qgraph_dot_option(),
            ctrlMpExecOpts.save_execution_butler_option(),
            ctrlMpExecOpts.clobber_execution_butler_option(),
            ctrlMpExecOpts.target_datastore_root_option(),
            transfer_option(
                help=unwrap(
                    """Data transfer mode for the execution butler datastore.
                    Defaults to "copy" if --target-datastore-root is provided.
                    """
                ),
            ),
            ctrlMpExecOpts.dataset_query_constraint(),
            ctrlMpExecOpts.qgraph_header_data_option(),
        ]


class butler_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for configuring a
    butler."""

    def __init__(self) -> None:
        self.decorators = [
            option_section(sectionText="Data repository and selection options:"),
            ctrlMpExecOpts.butler_config_option(required=True),
            ctrlMpExecOpts.input_option(),
            ctrlMpExecOpts.output_option(),
            ctrlMpExecOpts.output_run_option(),
            ctrlMpExecOpts.extend_run_option(),
            ctrlMpExecOpts.replace_run_option(),
            ctrlMpExecOpts.prune_replaced_option(),
            ctrlMpExecOpts.data_query_option(),
        ]


class execution_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for executing a
    pipeline."""

    def __init__(self) -> None:
        self.decorators = [
            option_section(sectionText="Execution options:"),
            ctrlMpExecOpts.clobber_outputs_option(),
            ctrlMpExecOpts.pdb_option(),
            ctrlMpExecOpts.profile_option(),
            dafButlerOpts.processes_option(),
            ctrlMpExecOpts.start_method_option(),
            ctrlMpExecOpts.timeout_option(),
            ctrlMpExecOpts.fail_fast_option(),
            ctrlMpExecOpts.graph_fixup_option(),
            ctrlMpExecOpts.mock_option(),
            ctrlMpExecOpts.summary_option(),
            ctrlMpExecOpts.enable_implicit_threading_option(),
        ]


class meta_info_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for managing pipeline
    meta information."""

    def __init__(self) -> None:
        self.decorators = [
            option_section(sectionText="Meta-information output options:"),
            ctrlMpExecOpts.skip_init_writes_option(),
            ctrlMpExecOpts.init_only_option(),
            dafButlerOpts.register_dataset_types_option(),
            ctrlMpExecOpts.no_versions_option(),
        ]


class run_options(OptionGroup):  # noqa: N801
    """Decorator to add the run options to the run command."""

    def __init__(self) -> None:
        self.decorators = [
            click.pass_context,
            ctrlMpExecOpts.debug_option(),
            ctrlMpExecOpts.coverage_option(),
            ctrlMpExecOpts.coverage_packages_option(),
            ctrlMpExecOpts.show_option(),
            pipeline_build_options(),
            qgraph_options(),
            butler_options(),
            execution_options(),
            meta_info_options(),
            option_section(sectionText=""),
            dafButlerOpts.options_file_option(),
        ]
