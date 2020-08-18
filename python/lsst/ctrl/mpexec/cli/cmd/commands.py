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
from functools import partial

from lsst.daf.butler.cli.opt import (config_file_option,
                                     config_option,
                                     log_level_option)
from lsst.daf.butler.cli.utils import (cli_handle_exception,
                                       MWCommand,
                                       MWCtxObj,
                                       option_section,
                                       split_kv,
                                       unwrap)
from lsst.obs.base.cli.opt import instrument_option
from .. import opt
from .. import script
from ..utils import makePipelineActions


instrumentOptionHelp = ("Add an instrument which will be used to load config overrides when defining a "
                        "pipeline. This must be the fully qualified class name.")


forwardEpilog = unwrap("""Options marked with (f) are forwarded to the next subcommand if multiple subcommands
                are chained in the same command execution. Previous values may be overridden by passing new
                option values into the next subcommand.""")

buildEpilog = unwrap(f"""Notes:

--task, --delete, --config, --config-file, and --instrument action options can
appear multiple times; all values are used, in order left to right.

FILE reads command-line options from the specified file. Data may be
distributed among multiple lines (e.g. one option per line). Data after # is
treated as a comment and ignored. Blank lines and lines starting with # are
ignored.)
""")

qgraphEpilog = forwardEpilog

runEpilog = forwardEpilog


@click.command(cls=MWCommand, epilog=buildEpilog, short_help="Build pipeline definition.")
@click.pass_context
@log_level_option()
@opt.pipeline_option()
@opt.task_option()
@opt.delete_option(metavar="LABEL")
@config_option(metavar="LABEL:NAME=VALUE", multiple=True)
@config_file_option(help="Configuration override file(s), applies to a task with a given label.",
                    metavar="LABEL:FILE",
                    multiple=True)
@instrument_option(help=instrumentOptionHelp, metavar="instrument", multiple=True)
@opt.order_pipeline_option()
@opt.save_pipeline_option()
@opt.pipeline_dot_option()
@opt.show_option()
def build(ctx, *args, **kwargs):
    """Build and optionally save pipeline definition.

    This does not require input data to be specified.
    """
    def processor(objs):
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
        for pipelineAction in (opt.task_option.name(), opt.delete_option.name(),
                               config_option.name(), config_file_option.name(),
                               instrument_option.name()):
            kwargs.pop(pipelineAction)
        kwargs['pipeline_actions'] = makePipelineActions(MWCtxObj.getFrom(ctx).args)
        objs.pipeline = cli_handle_exception(script.build, *args, **kwargs)
        return objs
    return processor


@click.command(cls=MWCommand, epilog=qgraphEpilog)
@click.pass_context
@log_level_option()
@opt.qgraph_option()
@opt.skip_existing_option()
@opt.save_qgraph_option()
@opt.save_single_quanta_option()
@opt.qgraph_dot_option()
@option_section(sectionText="Data repository and selection options:")
@opt.butler_config_option(forward=True)
# CLI API says `--input` values should be given like
# "datasetType:collectionName" or just "datasetType", but CmdLineFwk api
# wants input values to be a tuple of tuples, where each tuple is
# ("collectionName", "datasetType"), or (..., "datasetType") with elipsis if no
# collectionName is provided. Setting `return_type=tuple`, `reverse_kv=True`,
# and `default_key=...` make `split_kv` callback structure its return value
# that way.
@opt.input_option(callback=partial(split_kv, return_type=tuple, default_key=..., reverse_kv=True,
                                   unseparated_okay=True),
                  multiple=True,
                  forward=True)
@opt.output_option(forward=True)
@opt.output_run_option(forward=True)
@opt.extend_run_option(forward=True)
@opt.replace_run_option(forward=True)
@opt.prune_replaced_option(forward=True)
@opt.data_query_option(forward=True)
@option_section(sectionText="Other options:")
@opt.show_option()
def qgraph(ctx, *args, **kwargs):
    """Build and optionally save quantum graph.
    """
    def processor(objs):
        newKwargs = objs.butlerArgs.update(ctx.command.params, MWCtxObj.getFrom(ctx).args, **kwargs)
        objs.qgraph = cli_handle_exception(script.qgraph, pipeline=objs.pipeline, **newKwargs)
        return objs
    return processor


@click.command(cls=MWCommand, epilog=runEpilog)
@click.pass_context
@log_level_option()
@opt.debug_option()
@option_section(sectionText="Data repository and selection options:")
@opt.butler_config_option(forward=True)
# CLI API says `--input` values should be given like
# "datasetType:collectionName" or just "datasetType", but CmdLineFwk api
# wants input values to be a tuple of tuples, where each tuple is
# ("collectionName", "datasetType"), or (..., "datasetType") - elipsis if no
# collectionName is provided.
@opt.input_option(callback=partial(split_kv, return_type=tuple, default_key=..., reverse_kv=True,
                                   unseparated_okay=True),
                  multiple=True,
                  forward=True)
@opt.output_option(forward=True)
@opt.output_run_option(forward=True)
@opt.extend_run_option(forward=True)
@opt.replace_run_option(forward=True)
@opt.prune_replaced_option(forward=True)
@opt.data_query_option(forward=True)
@option_section(sectionText="Execution options:")
@opt.do_raise_option()
@opt.profile_option()
@opt.processes_option()
@opt.timeout_option()
@opt.graph_fixup_option()
@option_section(sectionText="Meta-information output options:")
@opt.skip_init_writes_option()
@opt.init_only_option()
@opt.register_dataset_types_option()
@opt.no_versions_option()
@opt.skip_existing_option(help=unwrap("""Do not try to overwrite any datasets that might exist in the butler.
                                      If not provided then any existing conflicting dataset will cause butler
                                      exception."""))
def run(ctx, *args, **kwargs):
    """Execute pipeline and quantum graph.
    """
    def processor(objs):
        newKwargs = objs.butlerArgs.update(ctx.command.params, MWCtxObj.getFrom(ctx).args, **kwargs)
        cli_handle_exception(script.run, qgraph=objs.qgraph, **newKwargs)
        return objs
    return processor
