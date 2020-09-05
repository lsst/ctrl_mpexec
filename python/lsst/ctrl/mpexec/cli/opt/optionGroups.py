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


__all__ = ("butler_options", "execution_options", "meta_info_options", "pipeline_build_options",
           "qgraph_options")


from functools import partial

from lsst.daf.butler.cli.utils import option_section, split_kv, unwrap
import lsst.obs.base.cli.opt as obsBaseOpts
import lsst.daf.butler.cli.opt as dafButlerOpts
from . import options as ctrlMpExecOpts

instrumentOptionHelp = ("Add an instrument which will be used to load config overrides when defining a "
                        "pipeline. This must be the fully qualified class name.")


class OptionGroup:
    """Base class for an option group decorator. Requires the option group
    subclass to have a property called `decorator`."""

    def __call__(self, f):
        for decorator in reversed(self.decorators):
            f = decorator(f)
        return f


class pipeline_build_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for building a pipeline.
    """

    def __init__(self):
        self.decorators = [
            option_section(sectionText="Pipeline build options:"),
            ctrlMpExecOpts.pipeline_option(),
            ctrlMpExecOpts.task_option(),
            ctrlMpExecOpts.delete_option(metavar="LABEL"),
            dafButlerOpts.config_option(metavar="LABEL:NAME=VALUE", multiple=True),
            dafButlerOpts.config_file_option(help=unwrap("""Configuration override file(s), applies to a task
                                                         with a given label."""),
                                             metavar="LABEL:FILE",
                                             multiple=True),
            ctrlMpExecOpts.order_pipeline_option(),
            ctrlMpExecOpts.save_pipeline_option(),
            ctrlMpExecOpts.pipeline_dot_option(),
            obsBaseOpts.instrument_option(help=instrumentOptionHelp, metavar="instrument", multiple=True)]


class qgraph_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for creating a quantum
    graph."""

    def __init__(self):
        self.decorators = [
            option_section(sectionText="Quantum graph building options:"),
            ctrlMpExecOpts.qgraph_option(),
            ctrlMpExecOpts.skip_existing_option(),
            ctrlMpExecOpts.save_qgraph_option(),
            ctrlMpExecOpts.save_single_quanta_option(),
            ctrlMpExecOpts.qgraph_dot_option()]


class butler_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for configuring a butler.
    """

    def __init__(self):
        self.decorators = [
            option_section(sectionText="Data repository and selection options:"),
            ctrlMpExecOpts.butler_config_option(),
            # CLI API says `--input` values should be given like
            # "datasetType:collectionName" or just "datasetType", but CmdLineFwk api
            # wants input values to be a tuple of tuples, where each tuple is
            # ("collectionName", "datasetType"), or (..., "datasetType") with elipsis if no
            # collectionName is provided. Setting `return_type=tuple`, `reverse_kv=True`,
            # and `default_key=...` make `split_kv` callback structure its return value
            # that way.
            ctrlMpExecOpts.input_option(callback=partial(split_kv, return_type=tuple, default_key=...,
                                                         reverse_kv=True, unseparated_okay=True),
                                        multiple=True),
            ctrlMpExecOpts.output_option(),
            ctrlMpExecOpts.output_run_option(),
            ctrlMpExecOpts.extend_run_option(),
            ctrlMpExecOpts.replace_run_option(),
            ctrlMpExecOpts.prune_replaced_option(),
            ctrlMpExecOpts.data_query_option()]


class execution_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for executing a pipeline.
    """

    def __init__(self):
        self.decorators = [
            option_section(sectionText="Execution options:"),
            ctrlMpExecOpts.clobber_partial_outputs_option(),
            ctrlMpExecOpts.do_raise_option(),
            ctrlMpExecOpts.profile_option(),
            ctrlMpExecOpts.processes_option(),
            ctrlMpExecOpts.timeout_option(),
            ctrlMpExecOpts.fail_fast_option(),
            ctrlMpExecOpts.graph_fixup_option()]


class meta_info_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for managing pipeline
    meta information."""

    def __init__(self):
        self.decorators = [
            option_section(sectionText="Meta-information output options:"),
            ctrlMpExecOpts.skip_init_writes_option(),
            ctrlMpExecOpts.init_only_option(),
            ctrlMpExecOpts.register_dataset_types_option(),
            ctrlMpExecOpts.no_versions_option()]
